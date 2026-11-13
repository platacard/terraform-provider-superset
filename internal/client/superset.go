package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// RequestTimeout bounds every request to Superset, including reading the response, so that a
// server that stops answering fails the operation instead of blocking it forever.
const RequestTimeout = 5 * time.Minute

// databasesCacheTTL is how long a client reuses the list of database connections.
const databasesCacheTTL = 5 * time.Minute

// httpClient sends the requests of every Client. It uses http.DefaultTransport, so proxy
// settings from the environment apply.
var httpClient = &http.Client{Timeout: RequestTimeout}

// Superset limits the requests of each client address (RATELIMIT_APPLICATION, 50 per second by
// default since Superset 4.1, and AUTH_RATE_LIMIT for the login). A request that Superset
// answers with 429 Too Many Requests is sent again after a pause: the time that the
// Retry-After header names, or else 1, 2, 4 and 8 seconds.
const (
	maxRateLimitRetries = 4
	maxRateLimitDelay   = 30 * time.Second
)

// sleep pauses between retries. Tests replace it.
var sleep = time.Sleep

// jitter adds up to a quarter of the delay at random, so that clients that Superset limited at
// the same moment do not all try again at the same moment. Tests replace it.
var jitter = func(d time.Duration) time.Duration {
	return d + time.Duration(rand.Int64N(int64(d)/4+1))
}

// doWithRetry sends the request that newRequest builds, and sends it again while Superset
// answers 429 Too Many Requests, up to maxRateLimitRetries times. It returns the last response.
func doWithRetry(newRequest func() (*http.Request, error)) (*http.Response, error) {
	for attempt := 0; ; attempt++ {
		req, err := newRequest()
		if err != nil {
			return nil, err
		}
		resp, err := httpClient.Do(req)
		if err != nil || resp.StatusCode != http.StatusTooManyRequests || attempt == maxRateLimitRetries {
			return resp, err
		}
		delay := jitter(rateLimitDelay(resp.Header.Get("Retry-After"), attempt))
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		sleep(delay)
	}
}

// rateLimitDelay returns how long to wait before the next try: the seconds of a Retry-After
// header, at most maxRateLimitDelay, or else 1 second doubled with each try.
func rateLimitDelay(retryAfter string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(strings.TrimSpace(retryAfter)); err == nil && seconds >= 0 {
		if delay := time.Duration(seconds) * time.Second; delay < maxRateLimitDelay {
			return delay
		}
		return maxRateLimitDelay
	}
	return time.Second << attempt
}

// Client represents a client for Superset API.
//
// The client authenticates lazily: no request is sent until the first API call
// (or an explicit Authenticate call). The access token is shared by all callers
// and is safe for concurrent use, e.g. when Terraform runs with parallelism > 1.
type Client struct {
	Host     string
	Username string
	Password string
	// CSRFEnabled makes write requests carry a CSRF token (see csrfHeaders). Set from the provider's csrf_enabled attribute.
	CSRFEnabled bool
	// AuthProvider is the authentication provider named in the login request, "db" (the default) or "ldap".
	AuthProvider string

	mu    sync.Mutex
	token string

	permissions permissionIndex

	// databases caches the list of database connections of this client, see GetAllDatabases.
	databases struct {
		sync.Mutex
		rows    []map[string]interface{}
		fetched time.Time
	}
}

// NewClient creates a new Superset client with the specified host, username, and password.
// It does not send any request: authentication happens on the first API call or on Authenticate.
func NewClient(host, username, password string) *Client {
	return &Client{
		Host:        host,
		Username:    username,
		Password:    password,
		CSRFEnabled: true,
	}
}

// authProvider returns the authentication provider of the login, "db" unless another one is set.
func (c *Client) authProvider() string {
	if c.AuthProvider == "" {
		return "db"
	}
	return c.AuthProvider
}

// Authenticate logs in to Superset unless the client already holds an access token.
// It can be used to validate the credentials before any API call is made.
func (c *Client) Authenticate() error {
	_, err := c.accessToken()
	return err
}

// accessToken returns the current access token, logging in first if there is none yet.
// Concurrent callers wait for a single login instead of logging in on their own.
func (c *Client) accessToken() (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.token == "" {
		if err := c.login(); err != nil {
			return "", err
		}
	}
	return c.token, nil
}

// refreshAccessToken logs in again after the given token was rejected by Superset.
// If another caller has already replaced that token, the new one is returned as is.
func (c *Client) refreshAccessToken(rejected string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.token == rejected {
		if err := c.login(); err != nil {
			return "", err
		}
	}
	return c.token, nil
}

// validateSettings reports the connection settings that are not set.
func (c *Client) validateSettings() error {
	var missing []string
	if c.Host == "" {
		missing = append(missing, "host (SUPERSET_HOST)")
	}
	if c.Username == "" {
		missing = append(missing, "username (SUPERSET_USERNAME)")
	}
	if c.Password == "" {
		missing = append(missing, "password (SUPERSET_PASSWORD)")
	}
	if len(missing) > 0 {
		return fmt.Errorf("superset provider is not configured, missing: %s", strings.Join(missing, ", "))
	}
	return nil
}

// login sends an authentication request to the Superset API and stores the access token.
// The caller must hold c.mu.
func (c *Client) login() error {
	if err := c.validateSettings(); err != nil {
		return err
	}

	url := fmt.Sprintf("%s/api/v1/security/login", c.Host)
	payload := map[string]string{
		"username": c.Username,
		"password": c.Password,
		"provider": c.authProvider(),
	}
	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	resp, err := doWithRetry(func() (*http.Request, error) {
		req, err := http.NewRequest("POST", url, bytes.NewReader(jsonPayload))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		return req, nil
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return responseError("authenticate with Superset", resp, body)
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

	c.token = token
	return nil
}

// send performs an authenticated request to the given endpoint.
// If Superset rejects the access token (401, e.g. it has expired), the client logs in again
// and retries the request once. CSRF tokens and their cookies are tied to the session cookie,
// not to the access token, so they stay valid for the retry.
func (c *Client) send(method, endpoint string, body []byte, headers map[string]string, cookies []*http.Cookie) (*http.Response, error) {
	token, err := c.accessToken()
	if err != nil {
		return nil, err
	}

	resp, err := c.sendWithToken(token, method, endpoint, body, headers, cookies)
	if err != nil || resp.StatusCode != http.StatusUnauthorized {
		return resp, err
	}
	resp.Body.Close()

	token, err = c.refreshAccessToken(token)
	if err != nil {
		return nil, err
	}
	return c.sendWithToken(token, method, endpoint, body, headers, cookies)
}

func (c *Client) sendWithToken(token, method, endpoint string, body []byte, headers map[string]string, cookies []*http.Cookie) (*http.Response, error) {
	return doWithRetry(func() (*http.Request, error) {
		req, err := http.NewRequest(method, fmt.Sprintf("%s%s", c.Host, endpoint), bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
		for key, value := range headers {
			req.Header.Set(key, value)
		}
		for _, cookie := range cookies {
			req.AddCookie(cookie)
		}
		return req, nil
	})
}

// marshalPayload serializes a request payload to JSON; a nil payload produces an empty body.
func marshalPayload(payload interface{}) ([]byte, error) {
	if payload == nil {
		return nil, nil
	}
	return json.Marshal(payload)
}

// DoRequest sends an HTTP request to the specified endpoint using the specified method.
// It takes the HTTP method, endpoint URL, and payload as input parameters.
// If a payload is provided, it will be serialized to JSON before sending the request.
// The function returns the HTTP response and an error, if any.
func (c *Client) DoRequest(method, endpoint string, payload interface{}) (*http.Response, error) {
	body, err := marshalPayload(payload)
	if err != nil {
		return nil, err
	}
	return c.send(method, endpoint, body, nil, nil)
}

// DoRequestWithHeadersAndCookies performs an HTTP request with additional headers and cookies.
func (c *Client) DoRequestWithHeadersAndCookies(method, endpoint string, payload interface{}, headers map[string]string, cookies []*http.Cookie) (*http.Response, error) {
	body, err := marshalPayload(payload)
	if err != nil {
		return nil, err
	}
	return c.send(method, endpoint, body, headers, cookies)
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

// csrfHeaders returns the headers and cookies a write request needs to pass the Superset CSRF check.
// When CSRF is disabled on the client, nothing is fetched and the request carries the access token only.
func (c *Client) csrfHeaders() (map[string]string, []*http.Cookie, error) {
	if !c.CSRFEnabled {
		return nil, nil, nil
	}

	csrfToken, cookies, err := c.GetCSRFToken()
	if err != nil {
		return nil, nil, err
	}

	headers := map[string]string{
		"X-CSRFToken": csrfToken,
		"Referer":     c.Host,
	}
	return headers, cookies, nil
}

// GetRoleIDByName returns the id of the role with the given name.
func (c *Client) GetRoleIDByName(roleName string) (int64, error) {
	id, err := c.findIDByName("/api/v1/security/roles/", roleName)
	if err != nil {
		return 0, fmt.Errorf("role %s: %w", roleName, err)
	}
	return id, nil
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

// CreateRole creates a role with the specified name in the Superset application.
// If the role already exists, it returns the existing role ID.
// It returns the ID of the created role and any error encountered.
func (c *Client) CreateRole(name string) (int64, error) {
	// Check if role already exists
	existingID, err := c.GetRoleIDByName(name)
	if err == nil {
		return existingID, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return 0, err
	}

	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return 0, err
	}

	endpoint := "/api/v1/security/roles/"
	payload := map[string]string{"name": name}
	resp, err := c.DoRequestWithHeadersAndCookies("POST", endpoint, payload, headers, cookies)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body) // Read the response body
		return 0, responseError("create role", resp, body)
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
		return nil, objectError("fetch role", resp, body)
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

	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	endpoint := fmt.Sprintf("/api/v1/security/roles/%d", id)
	payload := map[string]string{"name": name}
	resp, err := c.DoRequestWithHeadersAndCookies("PUT", endpoint, payload, headers, cookies)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body) // Read the response body
		return responseError("update role", resp, body)
	}

	return nil
}

// DeleteRole deletes a role with the specified ID from the Superset server.
// It sends a DELETE request to the Superset API endpoint for deleting roles.
// If the request is successful and the role is deleted, it returns nil.
// If there is an error or the response status code is not 204 (No Content) or 200 (OK),
// it returns an error with the corresponding status code and response body.
func (c *Client) DeleteRole(id int64) error {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	endpoint := fmt.Sprintf("/api/v1/security/roles/%d", id)
	resp, err := c.DoRequestWithHeadersAndCookies("DELETE", endpoint, nil, headers, cookies)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body) // Read the response body
		return objectError("delete role", resp, body)
	}

	return nil
}

// UpdateRolePermissions updates the permissions of a role in the Superset application.
// It takes the role ID and a slice of permission IDs as parameters.
// The function sends a POST request to the Superset API to update the role permissions.
// It returns an error if the request fails or if the response status code is not 200 OK.
func (c *Client) UpdateRolePermissions(roleID int64, permissionIDs []int64) error {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	endpoint := fmt.Sprintf("/api/v1/security/roles/%d/permissions", roleID)
	payload := map[string][]int64{"permission_view_menu_ids": permissionIDs}
	resp, err := c.DoRequestWithHeadersAndCookies("POST", endpoint, payload, headers, cookies)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return responseError("update role permissions", resp, body)
	}

	return nil
}

// ClearRolePermissions clears the permissions for a given role ID in Superset.
// It sends a POST request to the Superset API to update the role's permissions.
// The function returns an error if the request fails or if the response status code is not 200 OK.
func (c *Client) ClearRolePermissions(roleID int64) error {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	endpoint := fmt.Sprintf("/api/v1/security/roles/%d/permissions", roleID)
	payload := map[string]interface{}{
		"permission_view_menu_ids": []int64{},
	}
	resp, err := c.DoRequestWithHeadersAndCookies("POST", endpoint, payload, headers, cookies)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body) // Read the response body
		return responseError("clear role permissions", resp, body)
	}

	return nil
}

// FetchRoles returns every role of the Superset instance.
func (c *Client) FetchRoles() ([]rawRoleModel, error) {
	return listInto[rawRoleModel](c, "/api/v1/security/roles/")
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

// GetAllDatabases returns every database connection of the Superset instance. The list is
// cached by the client for databasesCacheTTL, and the cache is dropped whenever the client
// creates, updates or deletes a database connection. The cache belongs to the client, so
// clients of different Superset instances or users never share it.
func (c *Client) GetAllDatabases() ([]map[string]interface{}, error) {
	c.databases.Lock()
	defer c.databases.Unlock()

	if c.databases.rows != nil && time.Since(c.databases.fetched) < databasesCacheTTL {
		return c.databases.rows, nil
	}

	databases, err := listInto[map[string]interface{}](c, "/api/v1/database/")
	if err != nil {
		return nil, err
	}
	c.databases.rows = databases
	c.databases.fetched = time.Now()
	return databases, nil
}

// forgetDatabases drops the cached list of database connections after a change.
func (c *Client) forgetDatabases() {
	c.databases.Lock()
	defer c.databases.Unlock()
	c.databases.rows = nil
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

// DeleteDatabase deletes a database with the given databaseID.
// It sends a DELETE request to the Superset API to delete the database.
// If the request is successful, it returns nil. Otherwise, it returns an error.
func (c *Client) DeleteDatabase(databaseID int64) error {
	defer c.forgetDatabases()
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	resp, err := c.DoRequestWithHeadersAndCookies("DELETE", fmt.Sprintf("/api/v1/database/%d", databaseID), nil, headers, cookies)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return objectError("delete database", resp, body)
	}

	return nil
}

// GetAllDatasets returns every dataset of the Superset instance.
func (c *Client) GetAllDatasets() ([]map[string]interface{}, error) {
	return listInto[map[string]interface{}](c, "/api/v1/dataset/")
}

// GetDatabaseIDByName returns the id of the database connection with the given name, looked up
// with a filtered request, or ErrNotFound.
func (c *Client) GetDatabaseIDByName(databaseName string) (int64, error) {
	database, err := c.FindDatabaseByName(databaseName)
	if err != nil {
		return 0, err
	}
	return database.ID, nil
}

// DatabaseSummary is a database connection as the list endpoint describes it.
type DatabaseSummary struct {
	ID             int64  `json:"id"`
	Name           string `json:"database_name"`
	Backend        string `json:"backend"`
	UUID           string `json:"uuid"`
	ExposeInSQLLab bool   `json:"expose_in_sqllab"`
	AllowDML       bool   `json:"allow_dml"`
	AllowRunAsync  bool   `json:"allow_run_async"`
}

// FindDatabaseByName returns the database connection with exactly the given name, or
// ErrNotFound. The server filters on equality; the name is compared again because the
// comparison can be case-insensitive depending on the metadata database.
func (c *Client) FindDatabaseByName(databaseName string) (*DatabaseSummary, error) {
	rows, err := listInto[DatabaseSummary](c, databasesPath, equals("database_name", databaseName))
	if err != nil {
		return nil, fmt.Errorf("database %q: %w", databaseName, err)
	}
	for _, row := range rows {
		if row.Name == databaseName {
			return &row, nil
		}
	}
	return nil, fmt.Errorf("database %q: %w", databaseName, ErrNotFound)
}

// GetDatabaseNameByID returns the name of the database connection with the given id. It reads
// the connection itself rather than the cached list, so a connection created or renamed a
// moment ago is found with its current name.
func (c *Client) GetDatabaseNameByID(databaseID int64) (string, error) {
	resp, err := c.DoRequest("GET", fmt.Sprintf("/api/v1/database/%d", databaseID), nil)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode == http.StatusNotFound {
		return "", fmt.Errorf("database with ID %d: %w", databaseID, ErrNotFound)
	}
	if resp.StatusCode != http.StatusOK {
		return "", responseError("read database", resp, body)
	}

	var result struct {
		Result struct {
			DatabaseName string `json:"database_name"`
		} `json:"result"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", fmt.Errorf("failed to decode database %d: %w", databaseID, err)
	}
	return result.Result.DatabaseName, nil
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
	defer c.forgetDatabases()
	headers, cookies, err := c.csrfHeaders()
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

	resp, err := c.DoRequestWithHeadersAndCookies("POST", "/api/v1/database/", payload, headers, cookies)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return 0, responseError("create meta database", resp, body)
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
		return nil, responseError("fetch meta database", resp, body)
	}

	var result struct {
		Result MetaDatabase `json:"result"`
	}
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return nil, err
	}

	metaDB := &result.Result

	// The detail endpoint does not return the extra field of every Superset version, so the
	// extra field of the list of connections is used when it has one.
	if allDBs, err := c.GetAllDatabases(); err == nil {
		for _, db := range allDBs {
			if dbID, ok := db["id"].(float64); ok && int64(dbID) == id {
				if extra, ok := db["extra"].(string); ok && extra != "" {
					metaDB.Extra = extra
				}
				break
			}
		}
	}

	// The allowed databases are the engine_params.allowed_dbs of the extra field.
	if metaDB.Extra != "" {
		var extraData struct {
			EngineParams struct {
				AllowedDBs []interface{} `json:"allowed_dbs"`
			} `json:"engine_params"`
		}
		if err := json.Unmarshal([]byte(metaDB.Extra), &extraData); err == nil && extraData.EngineParams.AllowedDBs != nil {
			metaDB.AllowedDBs = make([]string, 0, len(extraData.EngineParams.AllowedDBs))
			for _, db := range extraData.EngineParams.AllowedDBs {
				if name, ok := db.(string); ok {
					metaDB.AllowedDBs = append(metaDB.AllowedDBs, name)
				}
			}
		}
	}

	return metaDB, nil
}

// UpdateMetaDatabase updates a meta database with the given ID.
func (c *Client) UpdateMetaDatabase(id int64, metaDB *MetaDatabase) error {
	defer c.forgetDatabases()
	headers, cookies, err := c.csrfHeaders()
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

	resp, err := c.DoRequestWithHeadersAndCookies("PUT", fmt.Sprintf("/api/v1/database/%d", id), payload, headers, cookies)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return responseError("update meta database", resp, body)
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

// FetchUsers returns every user of the Superset instance.
func (c *Client) FetchUsers() ([]rawUserModel, error) {
	return listInto[rawUserModel](c, "/api/v1/security/users/")
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
		return nil, objectError("fetch user", resp, body)
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
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return 0, err
	}

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

	resp, err := c.DoRequestWithHeadersAndCookies("POST", endpoint, payload, headers, cookies)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return 0, responseError("create user", resp, body)
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
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

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

	resp, err := c.DoRequestWithHeadersAndCookies("PUT", endpoint, payload, headers, cookies)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return responseError("update user", resp, body)
	}

	return nil
}

// DeleteUser deletes a user with the specified ID from the Superset server.
// It sends a DELETE request to the Superset API endpoint for deleting users.
// If the request is successful and the user is deleted, it returns nil.
// If there is an error or the response status code is not 204 (No Content) or 200 (OK),
// it returns an error with the corresponding status code and response body.
func (c *Client) DeleteUser(id int64) error {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	endpoint := fmt.Sprintf("/api/v1/security/users/%d", id)
	resp, err := c.DoRequestWithHeadersAndCookies("DELETE", endpoint, nil, headers, cookies)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return objectError("delete user", resp, body)
	}

	return nil
}
