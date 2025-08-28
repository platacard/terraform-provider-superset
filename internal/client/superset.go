package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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

// Client represents a client for Superset API.
type Client struct {
	Host     string
	Username string
	Password string
	Token    string
	Cookies  []*http.Cookie
}

// NewClient creates a new Superset client with the specified host, username, and password.
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

// DoRequest sends an HTTP request to the specified endpoint using the specified method.
// It takes the HTTP method, endpoint URL, and payload as input parameters.
// If a payload is provided, it will be serialized to JSON before sending the request.
// The function returns the HTTP response and an error, if any.
func (c *Client) DoRequest(method, endpoint string, payload interface{}) (*http.Response, error) {
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
		fmt.Printf("Role with ID %d already has the name '%s'. No update necessary.\n", id, name)
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

	fmt.Printf("Role with ID %d successfully updated to name '%s'.\n", id, name)
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
	endpoint := "/api/v1/security/permissions-resources?q=(page_size:5000)"
	resp, err := c.DoRequest("GET", endpoint, nil)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("failed to fetch permissions resources from Superset, status code: %d", resp.StatusCode)
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
		return 0, err
	}

	for _, resource := range result.Resources {
		if resource.Permission.Name == permissionName && resource.ViewMenu.Name == viewMenuName {
			return resource.ID, nil
		}
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
		fmt.Printf("DEBUG GetAllDatabases: Using global cached result with %d databases (age: %v)\n",
			len(globalDatabasesCache), time.Since(globalDatabasesCacheTime))
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
		fmt.Printf("DEBUG GetAllDatabases: Using global cached result (double-check) with %d databases\n", len(globalDatabasesCache))
		return globalDatabasesCache, nil
	}

	endpoint := "/api/v1/database/?q=(page_size:5000)"
	fmt.Printf("DEBUG GetAllDatabases: Making API call to %s\n", endpoint)
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

	fmt.Printf("DEBUG GetAllDatabases: Retrieved and cached globally %d databases total\n", len(result.Result))
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
// The function returns a map[string]interface{} containing the response from the API and an error, if any.
func (c *Client) CreateDatabase(payload map[string]interface{}) (map[string]interface{}, error) {
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
func (c *Client) CreateDataset(dataset DatasetRequest) (*map[string]interface{}, error) {
	endpoint := "/api/v1/dataset/"

	// Debug: log the request payload
	fmt.Printf("DEBUG CreateDataset: Sending request to %s with payload: %+v\n", endpoint, dataset)

	resp, err := c.DoRequest("POST", endpoint, dataset)
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
	fmt.Printf("DEBUG ClearGlobalDatabaseCache: Global database cache cleared\n")
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
	endpoint := fmt.Sprintf("/api/v1/dataset/%d", id)

	updateReq := DatasetUpdateRequest{
		TableName: tableName,
		Schema:    schema,
		SQL:       sql,
	}

	// Debug: log the update request payload
	fmt.Printf("DEBUG UpdateDataset: Sending UPDATE request to %s with payload: %+v\n", endpoint, updateReq)

	resp, err := c.DoRequest("PUT", endpoint, updateReq)
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
	endpoint := fmt.Sprintf("/api/v1/dataset/%d", id)
	resp, err := c.DoRequest("DELETE", endpoint, nil)
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

	// For debugging, also try to get full database info from list endpoint
	fmt.Printf("DEBUG GetMetaDatabase: Trying to get extra field from list endpoint for ID %d\n", id)
	allDBs, listErr := c.GetAllDatabases()
	if listErr == nil {
		fmt.Printf("DEBUG GetMetaDatabase: Successfully got %d databases from list endpoint\n", len(allDBs))
		found := false
		for _, db := range allDBs {
			if dbIDFloat, ok := db["id"].(float64); ok && int64(dbIDFloat) == id {
				found = true
				fmt.Printf("DEBUG GetMetaDatabase: Found matching database in list: %+v\n", db)
				if extraStr, ok := db["extra"].(string); ok {
					if extraStr != "" {
						fmt.Printf("DEBUG GetMetaDatabase: Found extra field from list endpoint: %q\n", extraStr)
						metaDB.Extra = extraStr
					} else {
						fmt.Printf("DEBUG GetMetaDatabase: Extra field is empty even in list endpoint\n")
					}
				} else {
					fmt.Printf("DEBUG GetMetaDatabase: No extra field found in list endpoint response\n")
				}
				break
			}
		}
		if !found {
			fmt.Printf("DEBUG GetMetaDatabase: Database ID %d not found in list of %d databases\n", id, len(allDBs))
		}
	} else {
		fmt.Printf("DEBUG GetMetaDatabase: Failed to get from list endpoint: %v\n", listErr)
	}

	// Parse allowed_dbs from extra field
	fmt.Printf("DEBUG GetMetaDatabase: Raw extra field = %q\n", metaDB.Extra)
	if metaDB.Extra != "" {
		var extraData map[string]interface{}
		if err := json.Unmarshal([]byte(metaDB.Extra), &extraData); err == nil {
			fmt.Printf("DEBUG GetMetaDatabase: Parsed extraData = %+v\n", extraData)
			if engineParams, ok := extraData["engine_params"].(map[string]interface{}); ok {
				fmt.Printf("DEBUG GetMetaDatabase: Found engine_params = %+v\n", engineParams)
				if allowedDBs, ok := engineParams["allowed_dbs"].([]interface{}); ok {
					fmt.Printf("DEBUG GetMetaDatabase: Found allowed_dbs = %+v (length: %d)\n", allowedDBs, len(allowedDBs))
					metaDB.AllowedDBs = make([]string, len(allowedDBs))
					for i, db := range allowedDBs {
						if dbStr, ok := db.(string); ok {
							metaDB.AllowedDBs[i] = dbStr
							fmt.Printf("DEBUG GetMetaDatabase: Added allowed_db[%d] = %q\n", i, dbStr)
						} else {
							fmt.Printf("DEBUG GetMetaDatabase: Failed to convert allowed_db[%d] to string: %+v (type: %T)\n", i, db, db)
						}
					}
				} else {
					fmt.Printf("DEBUG GetMetaDatabase: No 'allowed_dbs' found in engine_params\n")
				}
			} else {
				fmt.Printf("DEBUG GetMetaDatabase: No 'engine_params' found in extraData\n")
			}
		} else {
			fmt.Printf("DEBUG GetMetaDatabase: Failed to unmarshal extra field as JSON: %v\n", err)
		}
	} else {
		fmt.Printf("DEBUG GetMetaDatabase: Extra field is empty\n")
	}
	fmt.Printf("DEBUG GetMetaDatabase: Final AllowedDBs = %+v (length: %d)\n", metaDB.AllowedDBs, len(metaDB.AllowedDBs))

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
