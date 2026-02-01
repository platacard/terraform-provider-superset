package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Global cache for GetAllDatabases to avoid multiple API calls across different client instances.
var (
	globalDatabasesCache      []map[string]interface{}
	globalDatabasesCacheTime  time.Time
	globalDatabasesCacheTTL   = 5 * time.Minute // Cache for 5 minutes
	globalDatabasesCacheMutex sync.RWMutex
)

// Global cache for permissions-resources to avoid rate limiting during role_permissions creation.
// Key: "permissionName|viewMenuName" -> Value: permission ID
var (
	globalPermissionsCache      map[string]int64
	globalPermissionsCacheTime  time.Time
	globalPermissionsCacheTTL   = 2 * time.Minute // Cache for 2 minutes
	globalPermissionsCacheMutex sync.RWMutex
)

// Client represents a client for Superset API.
type Client struct {
	Host     string
	Username string
	Password string
	Token    string
	Cookies  []*http.Cookie
	// OIDC client credentials fields
	OIDCTokenURL string
	ClientID     string
	ClientSecret string
	TokenExpiry  time.Time
}

// NewClient creates a new Superset client with the specified host, username, and password.
// It uses database authentication (AUTH_DB) to obtain an access token.
// It returns a pointer to the created Client and an error if authentication fails.
func NewClient(host, username, password string) (*Client, error) {
	client := &Client{
		Host:     host,
		Username: username,
		Password: password,
	}

	err := client.authenticate()
	if err != nil {
		return nil, err
	}

	return client, nil
}

// NewClientWithOIDC creates a new Superset client using OIDC client credentials authentication.
// This is the recommended method for Superset instances configured with OAuth/OIDC (e.g., Keycloak).
// The client credentials grant is used to obtain an access token from the identity provider,
// which is then used as a Bearer token for Superset API requests.
func NewClientWithOIDC(host, oidcTokenURL, clientID, clientSecret string) (*Client, error) {
	client := &Client{
		Host:         host,
		OIDCTokenURL: oidcTokenURL,
		ClientID:     clientID,
		ClientSecret: clientSecret,
	}

	err := client.authenticateOIDC()
	if err != nil {
		return nil, err
	}

	return client, nil
}

// authenticateOIDC authenticates using OIDC client credentials grant.
// It requests an access token from the OIDC provider's token endpoint.
func (c *Client) authenticateOIDC() error {
	data := url.Values{}
	data.Set("grant_type", "client_credentials")
	data.Set("client_id", c.ClientID)
	data.Set("client_secret", c.ClientSecret)

	req, err := http.NewRequest("POST", c.OIDCTokenURL, strings.NewReader(data.Encode()))
	if err != nil {
		return fmt.Errorf("failed to create OIDC token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to request OIDC token: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read OIDC token response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("OIDC token request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var tokenResponse struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
		TokenType   string `json:"token_type"`
	}

	if err := json.Unmarshal(body, &tokenResponse); err != nil {
		return fmt.Errorf("failed to parse OIDC token response: %w", err)
	}

	if tokenResponse.AccessToken == "" {
		return fmt.Errorf("OIDC token response did not contain access_token")
	}

	c.Token = tokenResponse.AccessToken
	// Set token expiry with a small buffer (30 seconds before actual expiry)
	if tokenResponse.ExpiresIn > 0 {
		c.TokenExpiry = time.Now().Add(time.Duration(tokenResponse.ExpiresIn-30) * time.Second)
	}

	return nil
}

// authenticate sends an authentication request to the Superset API using the provided username and password.
// It returns an error if the authentication fails or if there is an error during the request.
func (c *Client) authenticate() error {
	url := fmt.Sprintf("%s/api/v1/security/login", c.Host)
	payload := map[string]string{
		"username": c.Username,
		"password": c.Password,
		"provider": "db",
	}
	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonPayload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to authenticate with Superset, status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	var result map[string]interface{}
	err = json.Unmarshal(body, &result)
	if err != nil {
		return err
	}

	token, ok := result["access_token"].(string)
	if !ok {
		return fmt.Errorf("failed to retrieve access token from response")
	}

	c.Token = token
	c.Cookies = resp.Cookies()
	return nil
}

// refreshOIDCTokenIfNeeded checks if the OIDC token is about to expire and refreshes it.
// This is a no-op for clients using database authentication.
func (c *Client) refreshOIDCTokenIfNeeded() error {
	// Only refresh if using OIDC authentication
	if c.OIDCTokenURL == "" {
		return nil
	}

	// Check if token is expired or will expire soon
	if time.Now().After(c.TokenExpiry) {
		return c.authenticateOIDC()
	}

	return nil
}

// DoRequest sends an HTTP request to the specified endpoint using the specified method.
// It takes the HTTP method, endpoint URL, and payload as input parameters.
// If a payload is provided, it will be serialized to JSON before sending the request.
// The function returns the HTTP response and an error, if any.
func (c *Client) DoRequest(method, endpoint string, payload interface{}) (*http.Response, error) {
	// Refresh OIDC token if needed
	if err := c.refreshOIDCTokenIfNeeded(); err != nil {
		return nil, fmt.Errorf("failed to refresh OIDC token: %w", err)
	}

	url := fmt.Sprintf("%s%s", c.Host, endpoint)
	var jsonPayload []byte
	var err error

	if payload != nil {
		jsonPayload, err = json.Marshal(payload)
		if err != nil {
			return nil, err
		}
	}

	req, err := http.NewRequest(method, url, bytes.NewBuffer(jsonPayload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.Token))

	client := &http.Client{}
	return client.Do(req)
}

// DoRequestWithHeadersAndCookies performs an HTTP request with additional headers and cookies.
func (c *Client) DoRequestWithHeadersAndCookies(method, endpoint string, payload interface{}, headers map[string]string, cookies []*http.Cookie) (*http.Response, error) {
	// Refresh OIDC token if needed
	if err := c.refreshOIDCTokenIfNeeded(); err != nil {
		return nil, fmt.Errorf("failed to refresh OIDC token: %w", err)
	}

	url := fmt.Sprintf("%s%s", c.Host, endpoint)
	var jsonPayload []byte
	var err error

	if payload != nil {
		jsonPayload, err = json.Marshal(payload)
		if err != nil {
			return nil, err
		}
	}

	req, err := http.NewRequest(method, url, bytes.NewBuffer(jsonPayload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.Token))
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	for _, cookie := range cookies {
		req.AddCookie(cookie)
	}

	client := &http.Client{}
	return client.Do(req)
}

// GetCSRFToken retrieves the CSRF token.
func (c *Client) GetCSRFToken() (string, []*http.Cookie, error) {
	headers := map[string]string{
		"Referer": c.Host,
	}
	resp, err := c.DoRequestWithHeadersAndCookies("GET", "/api/v1/security/csrf_token/", nil, headers, nil)
	if err != nil {
		return "", nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", nil, fmt.Errorf("failed to get CSRF token, status code: %d", resp.StatusCode)
	}

	var result map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return "", nil, err
	}

	csrfToken, ok := result["result"].(string)
	if !ok {
		return "", nil, fmt.Errorf("failed to retrieve CSRF token from response")
	}

	return csrfToken, resp.Cookies(), nil
}

// GetRoleIDByName retrieves the ID of a role by its name from the Superset API.
// It sends a GET request to the Superset API to fetch all roles, and then searches for the role with the specified name.
// If the role is found, its ID is returned. Otherwise, an error is returned.
// The function expects a valid Superset client to be passed as the receiver (c).
// The roleName parameter specifies the name of the role to search for.
// The function returns the ID of the role and an error, if any.
func (c *Client) GetRoleIDByName(roleName string) (int64, error) {
	endpoint := "/api/v1/security/roles?q=(page_size:5000)"
	resp, err := c.DoRequest("GET", endpoint, nil)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("failed to fetch roles from Superset, status code: %d", resp.StatusCode)
	}

	var result struct {
		Roles []struct {
			ID   int64  `json:"id"`
			Name string `json:"name"`
		} `json:"result"`
	}

	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return 0, err
	}

	for _, role := range result.Roles {
		if role.Name == roleName {
			return role.ID, nil
		}
	}

	return 0, fmt.Errorf("role %s not found", roleName)
}

// GetRolePermissions retrieves the permissions associated with a given role ID from Superset.
// It makes a GET request to the Superset API and returns a slice of Permission objects and an error, if any.
func (c *Client) GetRolePermissions(roleID int64) ([]Permission, error) {
	endpoint := fmt.Sprintf("/api/v1/security/roles/%d/permissions/", roleID)
	resp, err := c.DoRequest("GET", endpoint, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch permissions from Superset, status code: %d", resp.StatusCode)
	}

	var result struct {
		Permissions []Permission `json:"result"`
	}

	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return nil, err
	}

	return result.Permissions, nil
}

// GetPermissionViewMenuIDs retrieves the IDs of permissions and view menus
// based on the provided permissions. It sends a GET request to the Superset
// API to fetch the permissions resources and filters the results based on
// the provided permissions. It returns a slice of int64 IDs that match the
// provided permissions, or an error if the request fails or the decoding of
// the response fails.
//
// Parameters:
//   - permissions: A slice of maps containing the permission and view menu names
//     to filter the results.
//
// Returns:
// - A slice of int64 IDs that match the provided permissions.
// - An error if the request fails or the decoding of the response fails.
func (c *Client) GetPermissionViewMenuIDs(permissions []map[string]string) ([]int64, error) {
	url := fmt.Sprintf("%s/api/v1/security/permissions-resources/?q=(page_size:5000)", c.Host)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch permissions resources from Superset, status code: %d", resp.StatusCode)
	}

	var result struct {
		Resources []struct {
			ID         int64 `json:"id"`
			Permission struct {
				Name string `json:"name"`
			} `json:"permission"`
			ViewMenu struct {
				Name string `json:"name"`
			} `json:"view_menu"`
		} `json:"result"`
	}
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return nil, err
	}

	var ids []int64
	for _, perm := range permissions {
		for _, res := range result.Resources {
			if res.Permission.Name == perm["permission"] && res.ViewMenu.Name == perm["view_menu"] {
				ids = append(ids, res.ID)
				break
			}
		}
	}
	return ids, nil
}

// CreateRole creates a role with the specified name in the Superset application.
// If the role already exists, it returns the existing role ID.
// It returns the ID of the created role and any error encountered.
func (c *Client) CreateRole(name string) (int64, error) {
	// Check if role already exists
	existingID, err := c.GetRoleIDByName(name)
	if err == nil {
		return existingID, nil
	}

	endpoint := "/api/v1/security/roles/"
	payload := map[string]string{"name": name}
	resp, err := c.DoRequest("POST", endpoint, payload)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body) // Read the response body
		return 0, fmt.Errorf("failed to create role, status code: %d, response: %s", resp.StatusCode, string(body))
	}

	var result map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return 0, err
	}

	id, ok := result["id"].(int64)
	if !ok {
		idFloat, okFloat := result["id"].(float64)
		if !okFloat {
			return 0, fmt.Errorf("failed to retrieve role ID from response")
		}
		id = int64(idFloat)
	}

	return id, nil
}

// GetRole retrieves a role by its ID from the Superset API.
// It sends a GET request to the "/api/v1/security/roles/{id}" endpoint
// and returns the role as a *Role object if successful.
// If there is an error during the request or response handling,
// it returns nil and an error describing the issue.
func (c *Client) GetRole(id int64) (*Role, error) {
	endpoint := fmt.Sprintf("/api/v1/security/roles/%d", id)
	resp, err := c.DoRequest("GET", endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("error making GET request to %s: %v", endpoint, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body) // Read the response body for detailed error logging
		return nil, fmt.Errorf("failed to fetch role, status code: %d, response: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response body: %v", err)
	}

	// Define a struct to match the JSON structure
	var result struct {
		ID     int64 `json:"id"`
		Result struct {
			ID   int64  `json:"id"`
			Name string `json:"name"`
		} `json:"result"`
	}

	err = json.Unmarshal(body, &result)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling response to struct: %v", err)
	}

	// Create a Role instance to return
	role := &Role{
		ID:   result.Result.ID,
		Name: result.Result.Name,
	}

	return role, nil
}

// UpdateRole updates the name of a role with the specified ID.
// If the role with the given ID does not exist, an error is returned.
// If the existing role already has the specified name, no update is performed.
// The updated role name is sent to the Superset API using a PUT request.
// If the update is successful, the function returns nil.
// If the update fails, an error is returned with the corresponding status code and response body.
func (c *Client) UpdateRole(id int64, name string) error {
	existingRole, err := c.GetRole(id)
	if err != nil {
		return err
	}

	if existingRole.Name == name {
		return nil
	}

	endpoint := fmt.Sprintf("/api/v1/security/roles/%d", id)
	payload := map[string]string{"name": name}
	resp, err := c.DoRequest("PUT", endpoint, payload)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body) // Read the response body
		return fmt.Errorf("failed to update role, status code: %d, response: %s", resp.StatusCode, string(body))
	}

	return nil
}

// DeleteRole deletes a role with the specified ID from the Superset server.
// It sends a DELETE request to the Superset API endpoint for deleting roles.
// If the request is successful and the role is deleted, it returns nil.
// If there is an error or the response status code is not 204 (No Content) or 200 (OK),
// it returns an error with the corresponding status code and response body.
func (c *Client) DeleteRole(id int64) error {
	endpoint := fmt.Sprintf("/api/v1/security/roles/%d", id)
	resp, err := c.DoRequest("DELETE", endpoint, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body) // Read the response body
		return fmt.Errorf("failed to delete role, status code: %d, response: %s", resp.StatusCode, string(body))
	}

	return nil
}

// GetPermissionIDByNameAndView retrieves the ID of a permission by its name and view menu name.
// It sends a GET request to the Superset API to fetch the permissions resources and searches for the resource
// that matches the given permission name and view menu name. If a match is found, it returns the ID of the resource.
// If no match is found, it returns an error indicating that the permission with the given name and view menu name was not found.
//
// Parameters:
// - permissionName: The name of the permission to search for.
// - viewMenuName: The name of the view menu to search for.
//
// Returns:
// - int64: The ID of the permission resource if found.
// - error: An error if the request fails or if the permission resource is not found.
func (c *Client) GetPermissionIDByNameAndView(permissionName, viewMenuName string) (int64, error) {
	cacheKey := permissionName + "|" + viewMenuName

	// Check cache first (read lock)
	globalPermissionsCacheMutex.RLock()
	if globalPermissionsCache != nil && time.Since(globalPermissionsCacheTime) < globalPermissionsCacheTTL {
		if id, ok := globalPermissionsCache[cacheKey]; ok {
			globalPermissionsCacheMutex.RUnlock()
			return id, nil
		}
		// Cache exists but permission not found - could still be not found
		globalPermissionsCacheMutex.RUnlock()
		return 0, fmt.Errorf("permission %s with view menu %s not found", permissionName, viewMenuName)
	}
	globalPermissionsCacheMutex.RUnlock()

	// Cache miss or expired - rebuild cache (write lock)
	globalPermissionsCacheMutex.Lock()
	defer globalPermissionsCacheMutex.Unlock()

	// Double-check after acquiring write lock
	if globalPermissionsCache != nil && time.Since(globalPermissionsCacheTime) < globalPermissionsCacheTTL {
		if id, ok := globalPermissionsCache[cacheKey]; ok {
			return id, nil
		}
		return 0, fmt.Errorf("permission %s with view menu %s not found", permissionName, viewMenuName)
	}

	// Rebuild cache by fetching all permissions with pagination
	globalPermissionsCache = make(map[string]int64)
	const pageSize = 100
	page := 0

	for {
		endpoint := fmt.Sprintf("/api/v1/security/permissions-resources?q=(page:%d,page_size:%d)", page, pageSize)
		resp, err := c.DoRequest("GET", endpoint, nil)
		if err != nil {
			return 0, err
		}

		// Handle rate limiting with retry
		if resp.StatusCode == 429 {
			resp.Body.Close()
			time.Sleep(2 * time.Second)
			resp, err = c.DoRequest("GET", endpoint, nil)
			if err != nil {
				return 0, err
			}
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return 0, fmt.Errorf("failed to fetch permissions resources from Superset, status code: %d", resp.StatusCode)
		}

		var result struct {
			Count     int `json:"count"`
			Resources []struct {
				ID         int64 `json:"id"`
				Permission struct {
					Name string `json:"name"`
				} `json:"permission"`
				ViewMenu struct {
					Name string `json:"name"`
				} `json:"view_menu"`
			} `json:"result"`
		}

		err = json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()
		if err != nil {
			return 0, err
		}

		// Add all permissions to cache
		for _, resource := range result.Resources {
			key := resource.Permission.Name + "|" + resource.ViewMenu.Name
			globalPermissionsCache[key] = resource.ID
		}

		// Check if we've fetched all pages
		fetched := (page + 1) * pageSize
		if fetched >= result.Count || len(result.Resources) == 0 {
			break
		}
		page++

		// Small delay to avoid rate limiting
		time.Sleep(50 * time.Millisecond)
	}

	globalPermissionsCacheTime = time.Now()

	// Look up the requested permission from the cache
	if id, ok := globalPermissionsCache[cacheKey]; ok {
		return id, nil
	}

	return 0, fmt.Errorf("permission %s with view menu %s not found", permissionName, viewMenuName)
}

// UpdateRolePermissions updates the permissions of a role in the Superset application.
// It takes the role ID and a slice of permission IDs as parameters.
// The function sends a POST request to the Superset API to update the role permissions.
// It returns an error if the request fails or if the response status code is not 200 OK.
func (c *Client) UpdateRolePermissions(roleID int64, permissionIDs []int64) error {
	url := fmt.Sprintf("%s/api/v1/security/roles/%d/permissions", c.Host, roleID)
	data := map[string][]int64{"permission_view_menu_ids": permissionIDs}
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to update role permissions, status code: %d, response: %s", resp.StatusCode, string(body))
	}

	return nil
}

// ClearRolePermissions clears the permissions for a given role ID in Superset.
// It sends a POST request to the Superset API to update the role's permissions.
// The function returns an error if the request fails or if the response status code is not 200 OK.
func (c *Client) ClearRolePermissions(roleID int64) error {
	endpoint := fmt.Sprintf("/api/v1/security/roles/%d/permissions", roleID)
	payload := map[string]interface{}{
		"permission_view_menu_ids": []int64{},
	}
	resp, err := c.DoRequest("POST", endpoint, payload)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body) // Read the response body
		return fmt.Errorf("failed to clear role permissions, status code: %d, response: %s", resp.StatusCode, string(body))
	}

	return nil
}

// FetchRoles fetches the roles from the Superset API.
// It sends a GET request to the "/api/v1/security/roles?q=(page_size:5000)" endpoint
// and returns a slice of rawRoleModel and an error.
func (c *Client) FetchRoles() ([]rawRoleModel, error) {
	endpoint := "/api/v1/security/roles?q=(page_size:5000)"
	resp, err := c.DoRequest("GET", endpoint, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch roles from Superset, status code: %d", resp.StatusCode)
	}

	var result struct {
		Roles []rawRoleModel `json:"result"`
	}

	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return nil, err
	}

	return result.Roles, nil
}

// GetDatabaseSchemasByID retrieves the database schemas by the given database ID.
// It makes a GET request to the Superset API and returns a list of schema names.
// If the request fails or the response status code is not 200 OK, an error is returned.
func (c *Client) GetDatabaseSchemasByID(databaseID int64) ([]string, error) {
	endpoint := fmt.Sprintf("/api/v1/database/%d/schemas/", databaseID)
	resp, err := c.DoRequest("GET", endpoint, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch schemas from Superset, status code: %d", resp.StatusCode)
	}

	var result struct {
		Result []string `json:"result"`
	}
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return nil, err
	}

	return result.Result, nil
}

// GetDatabaseConnectionByID retrieves the database connection information by its ID from Superset.
// It makes a GET request to the Superset API and returns the response as a map[string]interface{}.
// If the request fails or the response status code is not 200 OK, an error is returned.
func (c *Client) GetDatabaseConnectionByID(databaseID int64) (map[string]interface{}, error) {
	endpoint := fmt.Sprintf("/api/v1/database/%d/connection", databaseID)
	resp, err := c.DoRequest("GET", endpoint, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch database connection from Superset, status code: %d", resp.StatusCode)
	}

	var result map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// GetAllDatabases retrieves all databases from Superset with global caching.
func (c *Client) GetAllDatabases() ([]map[string]interface{}, error) {
	// Check global cache first (read lock)
	globalDatabasesCacheMutex.RLock()
	if len(globalDatabasesCache) > 0 && time.Since(globalDatabasesCacheTime) < globalDatabasesCacheTTL {
		result := globalDatabasesCache
		globalDatabasesCacheMutex.RUnlock()
		return result, nil
	}
	globalDatabasesCacheMutex.RUnlock()

	// Need to fetch data - acquire write lock
	globalDatabasesCacheMutex.Lock()
	defer globalDatabasesCacheMutex.Unlock()

	// Double-check in case another goroutine already fetched while we were waiting
	if len(globalDatabasesCache) > 0 && time.Since(globalDatabasesCacheTime) < globalDatabasesCacheTTL {
		return globalDatabasesCache, nil
	}

	endpoint := "/api/v1/database/?q=(page_size:5000)"
	resp, err := c.DoRequest("GET", endpoint, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch databases from Superset, status code: %d", resp.StatusCode)
	}

	var result struct {
		Result []map[string]interface{} `json:"result"`
	}
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return nil, err
	}

	// Cache the result globally
	globalDatabasesCache = result.Result
	globalDatabasesCacheTime = time.Now()

	return result.Result, nil
}

// GetDatabasesInfos retrieves information about all databases.
// It returns a map containing the details of each database, including the database ID, name, schemas, and SQLAlchemy URI.
// If an error occurs during the retrieval process, it returns nil and the error.
func (c *Client) GetDatabasesInfos() (map[string]interface{}, error) {
	databasesInfo, err := c.GetAllDatabases()
	if err != nil {
		return nil, err
	}
	databasesList := []map[string]interface{}{}

	// Process only first 100 databases to avoid performance issues
	limit := 100
	if len(databasesInfo) < limit {
		limit = len(databasesInfo)
	}

	for _, db := range databasesInfo[:limit] {
		dbID, ok := db["id"].(float64)
		if !ok {
			continue
		}
		databaseDetails, err := c.GetDatabaseConnectionByID(int64(dbID))
		if err != nil {
			return nil, err
		}

		var sqlalchemyURI, databaseName string
		if result, ok := databaseDetails["result"].(map[string]interface{}); ok {
			sqlalchemyURI, _ = result["sqlalchemy_uri"].(string)
			databaseName, _ = result["database_name"].(string)
		}

		if sqlalchemyURI == "" {
			sqlalchemyURI = "URI not provided"
		}

		if databaseName == "" {
			databaseName = "Name not provided"
		}

		schemas, err := c.GetDatabaseSchemasByID(int64(dbID))
		if err != nil {
			return nil, err
		}

		databasesList = append(databasesList, map[string]interface{}{
			"id":             int64(dbID),
			"database_name":  databaseName,
			"schemas":        schemas,
			"sqlalchemy_uri": sqlalchemyURI,
		})
	}

	return map[string]interface{}{"databases": databasesList}, nil
}

// CreateDatabase creates a new database in the Superset application.
// It takes a payload map[string]interface{} as input, which contains the necessary data for creating the database.
// If a database with the same name already exists, it returns the existing database info.
// The function returns a map[string]interface{} containing the response from the API and an error, if any.
func (c *Client) CreateDatabase(payload map[string]interface{}) (map[string]interface{}, error) {
	// Check if database already exists by name (idempotency)
	databaseName, _ := payload["database_name"].(string)
	if databaseName != "" {
		existingID, err := c.GetDatabaseIDByName(databaseName)
		if err == nil && existingID > 0 {
			// Database exists - fetch and return its details
			dbInfo, err := c.GetDatabaseConnectionByID(existingID)
			if err == nil {
				// Build result format matching create response
				result := map[string]interface{}{
					"id":     float64(existingID),
					"result": dbInfo["result"],
				}
				return result, nil
			}
		}
	}

	csrfToken, cookies, err := c.GetCSRFToken()
	if err != nil {
		return nil, err
	}

	headers := map[string]string{
		"X-CSRFToken": csrfToken,
		"Referer":     c.Host,
	}

	resp, err := c.DoRequestWithHeadersAndCookies("POST", "/api/v1/database/", payload, headers, cookies)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to create database, status code: %d, response: %s", resp.StatusCode, string(body))
	}

	var result map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// UpdateDatabase updates a database with the given ID using the provided payload.
// It returns the updated database as a map[string]interface{} and an error if any.
func (c *Client) UpdateDatabase(databaseID int64, payload map[string]interface{}) (map[string]interface{}, error) {
	csrfToken, cookies, err := c.GetCSRFToken()
	if err != nil {
		return nil, err
	}

	headers := map[string]string{
		"X-CSRFToken": csrfToken,
		"Referer":     c.Host,
	}

	resp, err := c.DoRequestWithHeadersAndCookies("PUT", fmt.Sprintf("/api/v1/database/%d", databaseID), payload, headers, cookies)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to update database, status code: %d, response: %s", resp.StatusCode, string(body))
	}

	var result map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// DeleteDatabase deletes a database with the given databaseID.
// It sends a DELETE request to the Superset API to delete the database.
// If the request is successful, it returns nil. Otherwise, it returns an error.
func (c *Client) DeleteDatabase(databaseID int64) error {
	csrfToken, cookies, err := c.GetCSRFToken()
	if err != nil {
		return err
	}

	headers := map[string]string{
		"X-CSRFToken": csrfToken,
		"Referer":     c.Host,
	}

	resp, err := c.DoRequestWithHeadersAndCookies("DELETE", fmt.Sprintf("/api/v1/database/%d", databaseID), nil, headers, cookies)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to delete database, status code: %d, response: %s", resp.StatusCode, string(body))
	}

	return nil
}

// GetAllDatasets fetches all datasets from Superset.
func (c *Client) GetAllDatasets() ([]map[string]interface{}, error) {
	endpoint := "/api/v1/dataset/?q=(page_size:5000)"
	resp, err := c.DoRequest("GET", endpoint, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch datasets from Superset, status code: %d", resp.StatusCode)
	}

	var result struct {
		Result []map[string]interface{} `json:"result"`
	}
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return nil, err
	}

	return result.Result, nil
}

// DatasetRequest represents the request structure for creating/updating a dataset.
type DatasetRequest struct {
	TableName string `json:"table_name"`
	Database  int64  `json:"database"`
	Schema    string `json:"schema,omitempty"`
	SQL       string `json:"sql,omitempty"`
}

// CreateDataset creates a new dataset in Superset.
// If a dataset with the same table_name and database already exists, it returns the existing dataset.
func (c *Client) CreateDataset(dataset DatasetRequest) (*map[string]interface{}, error) {
	// Check if dataset already exists (idempotency)
	existingDatasets, err := c.GetAllDatasets()
	if err == nil {
		for _, ds := range existingDatasets {
			tableName, _ := ds["table_name"].(string)
			// Check database match via the database object
			if dbInfo, ok := ds["database"].(map[string]interface{}); ok {
				dbID, _ := dbInfo["id"].(float64)
				if tableName == dataset.TableName && int64(dbID) == dataset.Database {
					// Dataset exists - return it
					if dsID, ok := ds["id"].(float64); ok {
						existingDS, getErr := c.GetDataset(int64(dsID))
						if getErr == nil {
							result := map[string]interface{}{
								"id":     dsID,
								"result": *existingDS,
							}
							return &result, nil
						}
					}
				}
			}
		}
	}

	csrfToken, cookies, err := c.GetCSRFToken()
	if err != nil {
		return nil, err
	}

	headers := map[string]string{
		"X-CSRFToken": csrfToken,
		"Referer":     c.Host,
	}

	endpoint := "/api/v1/dataset/"

	resp, err := c.DoRequestWithHeadersAndCookies("POST", endpoint, dataset, headers, cookies)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to create dataset, status code: %d, response: %s", resp.StatusCode, string(body))
	}

	var result map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return nil, err
	}

	return &result, nil
}

// ClearGlobalDatabaseCache clears the global database cache (useful for tests).
func ClearGlobalDatabaseCache() {
	globalDatabasesCacheMutex.Lock()
	defer globalDatabasesCacheMutex.Unlock()
	globalDatabasesCache = nil
	globalDatabasesCacheTime = time.Time{}
}

// GetDataset fetches a specific dataset by ID.
func (c *Client) GetDataset(id int64) (*map[string]interface{}, error) {
	endpoint := fmt.Sprintf("/api/v1/dataset/%d", id)
	resp, err := c.DoRequest("GET", endpoint, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("dataset with ID %d not found", id)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to fetch dataset, status code: %d, response: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Result map[string]interface{} `json:"result"`
	}
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return nil, err
	}

	return &result.Result, nil
}

// DatasetUpdateRequest represents the request structure for updating a dataset (excludes database field).
type DatasetUpdateRequest struct {
	TableName string `json:"table_name"`
	Schema    string `json:"schema,omitempty"`
	SQL       string `json:"sql,omitempty"`
}

// UpdateDataset updates an existing dataset (database field cannot be changed).
func (c *Client) UpdateDataset(id int64, tableName, schema, sql string) error {
	csrfToken, cookies, err := c.GetCSRFToken()
	if err != nil {
		return err
	}

	headers := map[string]string{
		"X-CSRFToken": csrfToken,
		"Referer":     c.Host,
	}

	endpoint := fmt.Sprintf("/api/v1/dataset/%d", id)

	updateReq := DatasetUpdateRequest{
		TableName: tableName,
		Schema:    schema,
		SQL:       sql,
	}

	resp, err := c.DoRequestWithHeadersAndCookies("PUT", endpoint, updateReq, headers, cookies)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to update dataset, status code: %d, response: %s", resp.StatusCode, string(body))
	}

	return nil
}

// DeleteDataset deletes a dataset by ID.
func (c *Client) DeleteDataset(id int64) error {
	csrfToken, cookies, err := c.GetCSRFToken()
	if err != nil {
		return err
	}

	headers := map[string]string{
		"X-CSRFToken": csrfToken,
		"Referer":     c.Host,
	}

	endpoint := fmt.Sprintf("/api/v1/dataset/%d", id)
	resp, err := c.DoRequestWithHeadersAndCookies("DELETE", endpoint, nil, headers, cookies)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to delete dataset, status code: %d, response: %s", resp.StatusCode, string(body))
	}

	return nil
}

// GetDatabaseIDByName finds database ID by name using cached database list.
func (c *Client) GetDatabaseIDByName(databaseName string) (int64, error) {
	databases, err := c.GetAllDatabases()
	if err != nil {
		return 0, fmt.Errorf("failed to fetch databases: %w", err)
	}

	for _, db := range databases {
		if name, ok := db["database_name"].(string); ok && name == databaseName {
			if id, ok := db["id"].(float64); ok {
				return int64(id), nil
			}
		}
	}

	return 0, fmt.Errorf("database with name '%s' not found", databaseName)
}

// GetDatabaseNameByID finds database name by ID using cached database list.
func (c *Client) GetDatabaseNameByID(databaseID int64) (string, error) {
	databases, err := c.GetAllDatabases()
	if err != nil {
		return "", fmt.Errorf("failed to fetch databases: %w", err)
	}

	for _, db := range databases {
		if id, ok := db["id"].(float64); ok && int64(id) == databaseID {
			if name, ok := db["database_name"].(string); ok {
				return name, nil
			}
		}
	}

	return "", fmt.Errorf("database with ID %d not found", databaseID)
}

// rawRoleModel represents a raw role model in the Superset client.
type rawRoleModel struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

// Permission represents a permission in the Superset application.
type Permission struct {
	ID             int64  `json:"id"`
	PermissionName string `json:"permission_name"`
	ViewMenuName   string `json:"view_menu_name"`
}

// Role represents a role in the Superset application.
type Role struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

// MetaDatabase represents a meta database connection in Superset.
type MetaDatabase struct {
	ID                  int64    `json:"id"`
	DatabaseName        string   `json:"database_name"`
	Engine              string   `json:"engine"`
	ConfigurationMethod string   `json:"configuration_method"`
	SqlalchemyURI       string   `json:"sqlalchemy_uri"`
	ExposeInSqllab      bool     `json:"expose_in_sqllab"`
	AllowCtas           bool     `json:"allow_ctas"`
	AllowCvas           bool     `json:"allow_cvas"`
	AllowDml            bool     `json:"allow_dml"`
	AllowRunAsync       bool     `json:"allow_run_async"`
	Extra               string   `json:"extra"`
	ServerCert          *string  `json:"server_cert"`
	IsManagedExternally bool     `json:"is_managed_externally"`
	ExternalURL         *string  `json:"external_url"`
	AllowedDBs          []string `json:"-"` // Helper field for allowed databases
}

// CreateMetaDatabase creates a meta database connection in Superset.
// It takes a MetaDatabase struct and returns the created database ID and an error.
func (c *Client) CreateMetaDatabase(metaDB *MetaDatabase) (int64, error) {
	csrfToken, cookies, err := c.GetCSRFToken()
	if err != nil {
		return 0, err
	}

	// Build extra JSON with allowed_dbs
	extraData := map[string]interface{}{
		"metadata_params": map[string]interface{}{},
		"engine_params": map[string]interface{}{
			"allowed_dbs": metaDB.AllowedDBs,
		},
		"metadata_cache_timeout":         map[string]interface{}{},
		"schemas_allowed_for_csv_upload": []string{},
	}
	extraJSON, err := json.Marshal(extraData)
	if err != nil {
		return 0, err
	}

	payload := map[string]interface{}{
		"database_name":         metaDB.DatabaseName,
		"engine":                metaDB.Engine,
		"configuration_method":  metaDB.ConfigurationMethod,
		"sqlalchemy_uri":        metaDB.SqlalchemyURI,
		"expose_in_sqllab":      metaDB.ExposeInSqllab,
		"allow_ctas":            metaDB.AllowCtas,
		"allow_cvas":            metaDB.AllowCvas,
		"allow_dml":             metaDB.AllowDml,
		"allow_run_async":       metaDB.AllowRunAsync,
		"extra":                 string(extraJSON),
		"server_cert":           metaDB.ServerCert,
		"is_managed_externally": metaDB.IsManagedExternally,
		"external_url":          metaDB.ExternalURL,
	}

	headers := map[string]string{
		"X-CSRFToken": csrfToken,
		"Referer":     c.Host,
	}

	resp, err := c.DoRequestWithHeadersAndCookies("POST", "/api/v1/database/", payload, headers, cookies)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("failed to create meta database, status code: %d, response: %s", resp.StatusCode, string(body))
	}

	var result map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return 0, err
	}

	id, ok := result["id"].(float64)
	if !ok {
		return 0, fmt.Errorf("failed to retrieve meta database ID from response")
	}

	return int64(id), nil
}

// GetMetaDatabase retrieves a meta database by its ID from the Superset API.
func (c *Client) GetMetaDatabase(id int64) (*MetaDatabase, error) {
	endpoint := fmt.Sprintf("/api/v1/database/%d", id)
	resp, err := c.DoRequest("GET", endpoint, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to fetch meta database, status code: %d, response: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Result MetaDatabase `json:"result"`
	}
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return nil, err
	}

	metaDB := &result.Result

	// Try to get full database info from list endpoint (extra field may not be in single-item response)
	allDBs, listErr := c.GetAllDatabases()
	if listErr == nil {
		for _, db := range allDBs {
			if dbIDFloat, ok := db["id"].(float64); ok && int64(dbIDFloat) == id {
				if extraStr, ok := db["extra"].(string); ok && extraStr != "" {
					metaDB.Extra = extraStr
				}
				break
			}
		}
	}

	// Parse allowed_dbs from extra field
	if metaDB.Extra != "" {
		var extraData map[string]interface{}
		if err := json.Unmarshal([]byte(metaDB.Extra), &extraData); err == nil {
			if engineParams, ok := extraData["engine_params"].(map[string]interface{}); ok {
				if allowedDBs, ok := engineParams["allowed_dbs"].([]interface{}); ok {
					metaDB.AllowedDBs = make([]string, len(allowedDBs))
					for i, db := range allowedDBs {
						if dbStr, ok := db.(string); ok {
							metaDB.AllowedDBs[i] = dbStr
						}
					}
				}
			}
		}
	}

	return metaDB, nil
}

// UpdateMetaDatabase updates a meta database with the given ID.
func (c *Client) UpdateMetaDatabase(id int64, metaDB *MetaDatabase) error {
	csrfToken, cookies, err := c.GetCSRFToken()
	if err != nil {
		return err
	}

	// Build extra JSON with allowed_dbs
	extraData := map[string]interface{}{
		"metadata_params": map[string]interface{}{},
		"engine_params": map[string]interface{}{
			"allowed_dbs": metaDB.AllowedDBs,
		},
		"metadata_cache_timeout":         map[string]interface{}{},
		"schemas_allowed_for_csv_upload": []string{},
	}
	extraJSON, err := json.Marshal(extraData)
	if err != nil {
		return err
	}

	payload := map[string]interface{}{
		"database_name":         metaDB.DatabaseName,
		"engine":                metaDB.Engine,
		"configuration_method":  metaDB.ConfigurationMethod,
		"sqlalchemy_uri":        metaDB.SqlalchemyURI,
		"expose_in_sqllab":      metaDB.ExposeInSqllab,
		"allow_ctas":            metaDB.AllowCtas,
		"allow_cvas":            metaDB.AllowCvas,
		"allow_dml":             metaDB.AllowDml,
		"allow_run_async":       metaDB.AllowRunAsync,
		"extra":                 string(extraJSON),
		"server_cert":           metaDB.ServerCert,
		"is_managed_externally": metaDB.IsManagedExternally,
		"external_url":          metaDB.ExternalURL,
	}

	headers := map[string]string{
		"X-CSRFToken": csrfToken,
		"Referer":     c.Host,
	}

	resp, err := c.DoRequestWithHeadersAndCookies("PUT", fmt.Sprintf("/api/v1/database/%d", id), payload, headers, cookies)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to update meta database, status code: %d, response: %s", resp.StatusCode, string(body))
	}

	return nil
}

// DeleteMetaDatabase deletes a meta database with the given ID.
func (c *Client) DeleteMetaDatabase(id int64) error {
	return c.DeleteDatabase(id) // Reuse existing delete method
}

// FindMetaDatabaseByName finds a meta database by name and sqlalchemy_uri = "superset://".
// Returns the meta database if found, nil if not found, error if search failed.
func (c *Client) FindMetaDatabaseByName(databaseName string) (*MetaDatabase, error) {
	allDBs, err := c.GetAllDatabases()
	if err != nil {
		return nil, err
	}

	for _, db := range allDBs {
		// Check if it's a meta database with matching name
		if dbName, ok := db["database_name"].(string); ok && dbName == databaseName {
			if sqlalchemyURI, ok := db["sqlalchemy_uri"].(string); ok && sqlalchemyURI == "superset://" {
				if dbID, ok := db["id"].(float64); ok {
					return c.GetMetaDatabase(int64(dbID))
				}
			}
		}
	}

	return nil, nil // Not found
}

// User represents a user in the Superset application.
type User struct {
	ID        int64   `json:"id"`
	Username  string  `json:"username"`
	FirstName string  `json:"first_name"`
	LastName  string  `json:"last_name"`
	Email     string  `json:"email"`
	Active    bool    `json:"active"`
	Roles     []int64 `json:"roles,omitempty"`
}

// rawUserModel represents a raw user model in the Superset client.
type rawUserModel struct {
	ID        int64  `json:"id"`
	Username  string `json:"username"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	Email     string `json:"email"`
	Active    bool   `json:"active"`
	Roles     []struct {
		ID   int64  `json:"id"`
		Name string `json:"name"`
	} `json:"roles"`
}

// FetchUsers fetches the users from the Superset API.
// It sends a GET request to the "/api/v1/security/users/?q=(page_size:5000)" endpoint
// and returns a slice of rawUserModel and an error.
func (c *Client) FetchUsers() ([]rawUserModel, error) {
	endpoint := "/api/v1/security/users/?q=(page_size:5000)"
	resp, err := c.DoRequest("GET", endpoint, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch users from Superset, status code: %d", resp.StatusCode)
	}

	var result struct {
		Users []rawUserModel `json:"result"`
	}

	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return nil, err
	}

	return result.Users, nil
}

// GetUser retrieves a user by its ID from the Superset API.
// It sends a GET request to the "/api/v1/security/users/{id}" endpoint
// and returns the user as a *User object if successful.
// If there is an error during the request or response handling,
// it returns nil and an error describing the issue.
func (c *Client) GetUser(id int64) (*User, error) {
	endpoint := fmt.Sprintf("/api/v1/security/users/%d", id)
	resp, err := c.DoRequest("GET", endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("error making GET request to %s: %v", endpoint, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to fetch user, status code: %d, response: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response body: %v", err)
	}

	var result struct {
		ID     int64        `json:"id"`
		Result rawUserModel `json:"result"`
	}

	err = json.Unmarshal(body, &result)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling response to struct: %v", err)
	}

	// Convert rawUserModel to User
	user := &User{
		ID:        result.Result.ID,
		Username:  result.Result.Username,
		FirstName: result.Result.FirstName,
		LastName:  result.Result.LastName,
		Email:     result.Result.Email,
		Active:    result.Result.Active,
		Roles:     make([]int64, len(result.Result.Roles)),
	}

	for i, role := range result.Result.Roles {
		user.Roles[i] = role.ID
	}

	return user, nil
}

// CreateUser creates a user with the specified parameters in the Superset application.
// It returns the ID of the created user and any error encountered.
func (c *Client) CreateUser(username, firstName, lastName, email, password string, active bool, roles []int64) (int64, error) {
	endpoint := "/api/v1/security/users/"
	payload := map[string]interface{}{
		"username":   username,
		"first_name": firstName,
		"last_name":  lastName,
		"email":      email,
		"password":   password,
		"active":     active,
		"roles":      roles,
	}

	resp, err := c.DoRequest("POST", endpoint, payload)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("failed to create user, status code: %d, response: %s", resp.StatusCode, string(body))
	}

	var result map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return 0, err
	}

	id, ok := result["id"].(float64)
	if !ok {
		return 0, fmt.Errorf("failed to retrieve user ID from response")
	}

	return int64(id), nil
}

// UpdateUser updates the user with the specified ID.
// If the user with the given ID does not exist, an error is returned.
// The updated user data is sent to the Superset API using a PUT request.
// If the update is successful, the function returns nil.
// If the update fails, an error is returned with the corresponding status code and response body.
func (c *Client) UpdateUser(id int64, username, firstName, lastName, email, password string, active bool, roles []int64) error {
	endpoint := fmt.Sprintf("/api/v1/security/users/%d", id)
	payload := map[string]interface{}{
		"username":   username,
		"first_name": firstName,
		"last_name":  lastName,
		"email":      email,
		"active":     active,
		"roles":      roles,
	}

	// Only include password if it's not empty
	if password != "" {
		payload["password"] = password
	}

	resp, err := c.DoRequest("PUT", endpoint, payload)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to update user, status code: %d, response: %s", resp.StatusCode, string(body))
	}

	return nil
}

// DeleteUser deletes a user with the specified ID from the Superset server.
// It sends a DELETE request to the Superset API endpoint for deleting users.
// If the request is successful and the user is deleted, it returns nil.
// If there is an error or the response status code is not 204 (No Content) or 200 (OK),
// it returns an error with the corresponding status code and response body.
func (c *Client) DeleteUser(id int64) error {
	endpoint := fmt.Sprintf("/api/v1/security/users/%d", id)
	resp, err := c.DoRequest("DELETE", endpoint, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to delete user, status code: %d, response: %s", resp.StatusCode, string(body))
	}

	return nil
}

// ============================================================================
// Chart API Methods
// ============================================================================

// Chart represents a chart in the Superset application.
type Chart struct {
	ID             int64   `json:"id"`
	SliceName      string  `json:"slice_name"`
	DatasourceID   int64   `json:"datasource_id"`
	DatasourceType string  `json:"datasource_type"`
	VizType        string  `json:"viz_type"`
	Description    string  `json:"description"`
	Params         string  `json:"params"`
	CacheTimeout   *int64  `json:"cache_timeout"`
	CertifiedBy    string  `json:"certified_by"`
	CertDetails    string  `json:"certification_details"`
}

// ChartCreateRequest represents the request structure for creating a chart.
type ChartCreateRequest struct {
	SliceName      string  `json:"slice_name"`
	DatasourceID   int64   `json:"datasource_id"`
	DatasourceType string  `json:"datasource_type"`
	VizType        string  `json:"viz_type,omitempty"`
	Description    string  `json:"description,omitempty"`
	Params         string  `json:"params,omitempty"`
	CacheTimeout   *int64  `json:"cache_timeout,omitempty"`
	CertifiedBy    string  `json:"certified_by,omitempty"`
	CertDetails    string  `json:"certification_details,omitempty"`
	Owners         []int64 `json:"owners,omitempty"`
	Dashboards     []int64 `json:"dashboards,omitempty"`
}

// ChartUpdateRequest represents the request structure for updating a chart.
type ChartUpdateRequest struct {
	SliceName      string  `json:"slice_name,omitempty"`
	DatasourceID   int64   `json:"datasource_id,omitempty"`
	DatasourceType string  `json:"datasource_type,omitempty"`
	VizType        string  `json:"viz_type,omitempty"`
	Description    string  `json:"description,omitempty"`
	Params         string  `json:"params,omitempty"`
	CacheTimeout   *int64  `json:"cache_timeout,omitempty"`
	CertifiedBy    string  `json:"certified_by,omitempty"`
	CertDetails    string  `json:"certification_details,omitempty"`
	Owners         []int64 `json:"owners,omitempty"`
	Dashboards     []int64 `json:"dashboards,omitempty"`
}

// GetAllCharts fetches all charts from Superset.
func (c *Client) GetAllCharts() ([]map[string]interface{}, error) {
	endpoint := "/api/v1/chart/?q=(page_size:5000)"
	resp, err := c.DoRequest("GET", endpoint, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to fetch charts from Superset, status code: %d, response: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Result []map[string]interface{} `json:"result"`
	}
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return nil, err
	}

	return result.Result, nil
}

// GetChart fetches a specific chart by ID.
func (c *Client) GetChart(id int64) (*Chart, error) {
	endpoint := fmt.Sprintf("/api/v1/chart/%d", id)
	resp, err := c.DoRequest("GET", endpoint, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("chart with ID %d not found", id)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to fetch chart, status code: %d, response: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Result map[string]interface{} `json:"result"`
	}
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return nil, err
	}

	// Map the response to Chart struct
	chart := &Chart{
		ID: id,
	}

	if sliceName, ok := result.Result["slice_name"].(string); ok {
		chart.SliceName = sliceName
	}
	if vizType, ok := result.Result["viz_type"].(string); ok {
		chart.VizType = vizType
	}
	if description, ok := result.Result["description"].(string); ok {
		chart.Description = description
	}
	if params, ok := result.Result["params"].(string); ok {
		chart.Params = params
	}
	if cacheTimeout, ok := result.Result["cache_timeout"].(float64); ok {
		ct := int64(cacheTimeout)
		chart.CacheTimeout = &ct
	}
	if certifiedBy, ok := result.Result["certified_by"].(string); ok {
		chart.CertifiedBy = certifiedBy
	}
	if certDetails, ok := result.Result["certification_details"].(string); ok {
		chart.CertDetails = certDetails
	}

	// Extract datasource info from query_context JSON
	// The API returns datasource info inside query_context.datasource, not as top-level fields
	if queryContext, ok := result.Result["query_context"].(string); ok && queryContext != "" {
		var qc struct {
			Datasource struct {
				ID   int64  `json:"id"`
				Type string `json:"type"`
			} `json:"datasource"`
		}
		if err := json.Unmarshal([]byte(queryContext), &qc); err == nil {
			chart.DatasourceID = qc.Datasource.ID
			chart.DatasourceType = qc.Datasource.Type
		}
	}

	// Fallback: try to get from params if query_context didn't work
	// Some Superset versions return an empty datasource.type even when datasource.id is present.
	// In that case, fall back to parsing params' "datasource" ("<id>__<type>") too.
	if chart.DatasourceID == 0 || chart.DatasourceType == "" {
		if params, ok := result.Result["params"].(string); ok && params != "" {
			var p struct {
				Datasource string `json:"datasource"` // format: "10__table"
			}
			if err := json.Unmarshal([]byte(params), &p); err == nil && p.Datasource != "" {
				// Parse "10__table" format
				parts := strings.Split(p.Datasource, "__")
				if len(parts) == 2 {
					if id, err := strconv.ParseInt(parts[0], 10, 64); err == nil {
						chart.DatasourceID = id
						chart.DatasourceType = parts[1]
					}
				}
			}
		}
	}

	return chart, nil
}

// CreateChart creates a new chart in Superset.
func (c *Client) CreateChart(req ChartCreateRequest) (int64, error) {
	csrfToken, cookies, err := c.GetCSRFToken()
	if err != nil {
		return 0, err
	}

	headers := map[string]string{
		"X-CSRFToken": csrfToken,
		"Referer":     c.Host,
	}

	endpoint := "/api/v1/chart/"
	resp, err := c.DoRequestWithHeadersAndCookies("POST", endpoint, req, headers, cookies)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("failed to create chart, status code: %d, response: %s", resp.StatusCode, string(body))
	}

	var result map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return 0, err
	}

	id, ok := result["id"].(float64)
	if !ok {
		return 0, fmt.Errorf("failed to retrieve chart ID from response")
	}

	return int64(id), nil
}

// UpdateChart updates an existing chart.
func (c *Client) UpdateChart(id int64, req ChartUpdateRequest) error {
	csrfToken, cookies, err := c.GetCSRFToken()
	if err != nil {
		return err
	}

	headers := map[string]string{
		"X-CSRFToken": csrfToken,
		"Referer":     c.Host,
	}

	endpoint := fmt.Sprintf("/api/v1/chart/%d", id)
	resp, err := c.DoRequestWithHeadersAndCookies("PUT", endpoint, req, headers, cookies)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to update chart, status code: %d, response: %s", resp.StatusCode, string(body))
	}

	return nil
}

// DeleteChart deletes a chart by ID.
func (c *Client) DeleteChart(id int64) error {
	csrfToken, cookies, err := c.GetCSRFToken()
	if err != nil {
		return err
	}

	headers := map[string]string{
		"X-CSRFToken": csrfToken,
		"Referer":     c.Host,
	}

	endpoint := fmt.Sprintf("/api/v1/chart/%d", id)
	resp, err := c.DoRequestWithHeadersAndCookies("DELETE", endpoint, nil, headers, cookies)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to delete chart, status code: %d, response: %s", resp.StatusCode, string(body))
	}

	return nil
}

// ============================================================================
// Dashboard API Methods
// ============================================================================

// Dashboard represents a dashboard in the Superset application.
type Dashboard struct {
	ID            int64   `json:"id"`
	DashboardTitle string `json:"dashboard_title"`
	Slug          string  `json:"slug"`
	Published     bool    `json:"published"`
	JsonMetadata  string  `json:"json_metadata"`
	PositionJSON  string  `json:"position_json"`
	CSS           string  `json:"css"`
	CertifiedBy   string  `json:"certified_by"`
	CertDetails   string  `json:"certification_details"`
}

// DashboardCreateRequest represents the request structure for creating a dashboard.
type DashboardCreateRequest struct {
	DashboardTitle string  `json:"dashboard_title"`
	Slug           string  `json:"slug,omitempty"`
	Published      bool    `json:"published,omitempty"`
	JsonMetadata   string  `json:"json_metadata,omitempty"`
	PositionJSON   string  `json:"position_json,omitempty"`
	CSS            string  `json:"css,omitempty"`
	CertifiedBy    string  `json:"certified_by,omitempty"`
	CertDetails    string  `json:"certification_details,omitempty"`
	Owners         []int64 `json:"owners,omitempty"`
	Roles          []int64 `json:"roles,omitempty"`
}

// DashboardUpdateRequest represents the request structure for updating a dashboard.
type DashboardUpdateRequest struct {
	DashboardTitle string  `json:"dashboard_title,omitempty"`
	Slug           string  `json:"slug,omitempty"`
	Published      *bool   `json:"published,omitempty"`
	JsonMetadata   string  `json:"json_metadata,omitempty"`
	PositionJSON   string  `json:"position_json,omitempty"`
	CSS            string  `json:"css,omitempty"`
	CertifiedBy    string  `json:"certified_by,omitempty"`
	CertDetails    string  `json:"certification_details,omitempty"`
	Owners         []int64 `json:"owners,omitempty"`
	Roles          []int64 `json:"roles,omitempty"`
}

// GetAllDashboards fetches all dashboards from Superset.
func (c *Client) GetAllDashboards() ([]map[string]interface{}, error) {
	endpoint := "/api/v1/dashboard/?q=(page_size:5000)"
	resp, err := c.DoRequest("GET", endpoint, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to fetch dashboards from Superset, status code: %d, response: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Result []map[string]interface{} `json:"result"`
	}
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return nil, err
	}

	return result.Result, nil
}

// GetDashboardIDBySlug finds dashboard ID by slug using the dashboards list endpoint.
func (c *Client) GetDashboardIDBySlug(slug string) (int64, error) {
	if slug == "" {
		return 0, fmt.Errorf("slug is empty")
	}

	dashboards, err := c.GetAllDashboards()
	if err != nil {
		return 0, fmt.Errorf("failed to fetch dashboards: %w", err)
	}

	for _, d := range dashboards {
		if s, ok := d["slug"].(string); ok && s == slug {
			if id, ok := d["id"].(float64); ok {
				return int64(id), nil
			}
		}
	}

	return 0, fmt.Errorf("dashboard with slug '%s' not found", slug)
}

// GetDashboard fetches a specific dashboard by ID.
func (c *Client) GetDashboard(id int64) (*Dashboard, error) {
	endpoint := fmt.Sprintf("/api/v1/dashboard/%d", id)
	resp, err := c.DoRequest("GET", endpoint, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("dashboard with ID %d not found", id)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to fetch dashboard, status code: %d, response: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Result map[string]interface{} `json:"result"`
	}
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return nil, err
	}

	// Map the response to Dashboard struct
	dashboard := &Dashboard{
		ID: id,
	}

	if title, ok := result.Result["dashboard_title"].(string); ok {
		dashboard.DashboardTitle = title
	}
	if slug, ok := result.Result["slug"].(string); ok {
		dashboard.Slug = slug
	}
	if published, ok := result.Result["published"].(bool); ok {
		dashboard.Published = published
	}
	if jsonMetadata, ok := result.Result["json_metadata"].(string); ok {
		dashboard.JsonMetadata = jsonMetadata
	}
	if positionJSON, ok := result.Result["position_json"].(string); ok {
		dashboard.PositionJSON = positionJSON
	}
	if css, ok := result.Result["css"].(string); ok {
		dashboard.CSS = css
	}
	if certifiedBy, ok := result.Result["certified_by"].(string); ok {
		dashboard.CertifiedBy = certifiedBy
	}
	if certDetails, ok := result.Result["certification_details"].(string); ok {
		dashboard.CertDetails = certDetails
	}

	return dashboard, nil
}

// mergePositionsIntoMetadata adds positions to json_metadata to trigger chart linking.
// Superset's UpdateDashboardCommand calls set_dash_metadata when json_metadata contains
// a "positions" key, which properly populates the dashboard_slices table.
func mergePositionsIntoMetadata(jsonMetadata, positionJSON string) string {
	if positionJSON == "" {
		return jsonMetadata
	}

	// Parse position_json
	var positions map[string]interface{}
	if err := json.Unmarshal([]byte(positionJSON), &positions); err != nil {
		return jsonMetadata
	}

	// Parse existing json_metadata or create new
	var metadata map[string]interface{}
	if jsonMetadata != "" {
		if err := json.Unmarshal([]byte(jsonMetadata), &metadata); err != nil {
			metadata = make(map[string]interface{})
		}
	} else {
		metadata = make(map[string]interface{})
	}

	// Add positions to metadata - this triggers chart linking in Superset
	metadata["positions"] = positions

	// Marshal back to JSON
	result, err := json.Marshal(metadata)
	if err != nil {
		return jsonMetadata
	}

	return string(result)
}

// CreateDashboard creates a new dashboard in Superset.
func (c *Client) CreateDashboard(req DashboardCreateRequest) (int64, error) {
	// Idempotency: slug is unique in Superset. If a dashboard already exists with
	// the requested slug, update it and return its ID.
	if req.Slug != "" {
		existingID, err := c.GetDashboardIDBySlug(req.Slug)
		if err == nil && existingID > 0 {
			published := req.Published
			updateReq := DashboardUpdateRequest{
				DashboardTitle: req.DashboardTitle,
				Slug:           req.Slug,
				Published:      &published,
				JsonMetadata:   req.JsonMetadata,
				PositionJSON:   req.PositionJSON,
				CSS:            req.CSS,
				CertifiedBy:    req.CertifiedBy,
				CertDetails:    req.CertDetails,
			}
			if updateErr := c.UpdateDashboard(existingID, updateReq); updateErr != nil {
				return 0, fmt.Errorf("dashboard with slug '%s' already exists (id=%d) but update failed: %w", req.Slug, existingID, updateErr)
			}
			return existingID, nil
		}
	}

	csrfToken, cookies, err := c.GetCSRFToken()
	if err != nil {
		return 0, err
	}

	headers := map[string]string{
		"X-CSRFToken": csrfToken,
		"Referer":     c.Host,
	}

	endpoint := "/api/v1/dashboard/"
	resp, err := c.DoRequestWithHeadersAndCookies("POST", endpoint, req, headers, cookies)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("failed to create dashboard, status code: %d, response: %s", resp.StatusCode, string(body))
	}

	var result map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return 0, err
	}

	id, ok := result["id"].(float64)
	if !ok {
		return 0, fmt.Errorf("failed to retrieve dashboard ID from response")
	}

	dashboardID := int64(id)

	// Link charts by updating with json_metadata containing positions
	// Superset's UpdateDashboardCommand.run() calls set_dash_metadata when
	// json_metadata contains a "positions" key, which populates dashboard_slices
	if req.PositionJSON != "" {
		mergedMetadata := mergePositionsIntoMetadata(req.JsonMetadata, req.PositionJSON)
		updateReq := DashboardUpdateRequest{
			JsonMetadata: mergedMetadata,
		}
		// Ignore error - dashboard was created successfully, chart linking is best effort
		_ = c.UpdateDashboard(dashboardID, updateReq)
	}

	return dashboardID, nil
}

// UpdateDashboard updates an existing dashboard.
func (c *Client) UpdateDashboard(id int64, req DashboardUpdateRequest) error {
	csrfToken, cookies, err := c.GetCSRFToken()
	if err != nil {
		return err
	}

	headers := map[string]string{
		"X-CSRFToken": csrfToken,
		"Referer":     c.Host,
	}

	// Merge positions into json_metadata to trigger chart linking
	// Superset's UpdateDashboardCommand calls set_dash_metadata when
	// json_metadata contains a "positions" key, which populates dashboard_slices
	if req.PositionJSON != "" {
		req.JsonMetadata = mergePositionsIntoMetadata(req.JsonMetadata, req.PositionJSON)
	}

	endpoint := fmt.Sprintf("/api/v1/dashboard/%d", id)
	resp, err := c.DoRequestWithHeadersAndCookies("PUT", endpoint, req, headers, cookies)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to update dashboard, status code: %d, response: %s", resp.StatusCode, string(body))
	}

	return nil
}

// DeleteDashboard deletes a dashboard by ID.
func (c *Client) DeleteDashboard(id int64) error {
	csrfToken, cookies, err := c.GetCSRFToken()
	if err != nil {
		return err
	}

	headers := map[string]string{
		"X-CSRFToken": csrfToken,
		"Referer":     c.Host,
	}

	endpoint := fmt.Sprintf("/api/v1/dashboard/%d", id)
	resp, err := c.DoRequestWithHeadersAndCookies("DELETE", endpoint, nil, headers, cookies)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to delete dashboard, status code: %d, response: %s", resp.StatusCode, string(body))
	}

	return nil
}

// LinkDashboardSlices links chart slices to a dashboard.
func (c *Client) LinkDashboardSlices(dashboardID int64, sliceIDs []int64) error {
	if len(sliceIDs) == 0 {
		return nil
	}

	csrfToken, cookies, err := c.GetCSRFToken()
	if err != nil {
		return err
	}

	headers := map[string]string{
		"X-CSRFToken": csrfToken,
		"Referer":     c.Host,
	}

	// The Superset API accepts slices as an array of chart IDs
	payload := map[string]interface{}{
		"slices": sliceIDs,
	}

	endpoint := fmt.Sprintf("/api/v1/dashboard/%d", dashboardID)
	resp, err := c.DoRequestWithHeadersAndCookies("PUT", endpoint, payload, headers, cookies)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to link slices to dashboard, status code: %d, response: %s", resp.StatusCode, string(body))
	}

	return nil
}
