package client

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRefreshOIDCTokenIfNeeded_NoOIDC(t *testing.T) {
	// Client without OIDC should be a no-op
	client := &Client{
		Host:     "http://localhost",
		Username: "user",
		Password: "pass",
		Token:    "some-token",
	}

	err := client.refreshOIDCTokenIfNeeded()
	if err != nil {
		t.Errorf("Expected no error for non-OIDC client, got: %v", err)
	}
}

func TestRefreshOIDCTokenIfNeeded_TokenNotExpired(t *testing.T) {
	client := &Client{
		Host:         "http://localhost",
		OIDCTokenURL: "http://idp.example.com/token",
		ClientID:     "test-client",
		ClientSecret: "test-secret",
		Token:        "valid-token",
		TokenExpiry:  time.Now().Add(5 * time.Minute), // Token valid for 5 more minutes
	}

	err := client.refreshOIDCTokenIfNeeded()
	if err != nil {
		t.Errorf("Expected no error for non-expired token, got: %v", err)
	}

	// Token should not have changed
	if client.Token != "valid-token" {
		t.Errorf("Expected token to remain unchanged, got: %s", client.Token)
	}
}

func TestRefreshOIDCTokenIfNeeded_TokenExpired(t *testing.T) {
	// Create a mock OIDC token endpoint
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("Expected POST request, got %s", r.Method)
		}
		if r.Header.Get("Content-Type") != "application/x-www-form-urlencoded" {
			t.Errorf("Expected Content-Type application/x-www-form-urlencoded, got %s", r.Header.Get("Content-Type"))
		}

		// Parse form data
		if err := r.ParseForm(); err != nil {
			t.Errorf("Failed to parse form: %v", err)
		}
		if r.Form.Get("grant_type") != "client_credentials" {
			t.Errorf("Expected grant_type=client_credentials, got %s", r.Form.Get("grant_type"))
		}
		if r.Form.Get("client_id") != "test-client" {
			t.Errorf("Expected client_id=test-client, got %s", r.Form.Get("client_id"))
		}
		if r.Form.Get("client_secret") != "test-secret" {
			t.Errorf("Expected client_secret=test-secret, got %s", r.Form.Get("client_secret"))
		}

		// Return a new token
		response := map[string]interface{}{
			"access_token": "new-refreshed-token",
			"expires_in":   3600,
			"token_type":   "Bearer",
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client := &Client{
		Host:         "http://localhost",
		OIDCTokenURL: server.URL,
		ClientID:     "test-client",
		ClientSecret: "test-secret",
		Token:        "expired-token",
		TokenExpiry:  time.Now().Add(-1 * time.Minute), // Token expired 1 minute ago
	}

	err := client.refreshOIDCTokenIfNeeded()
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}

	// Token should have been refreshed
	if client.Token != "new-refreshed-token" {
		t.Errorf("Expected token to be 'new-refreshed-token', got: %s", client.Token)
	}

	// TokenExpiry should be updated
	if client.TokenExpiry.Before(time.Now()) {
		t.Errorf("Expected TokenExpiry to be in the future, got: %v", client.TokenExpiry)
	}
}

func TestNewClientWithOIDC(t *testing.T) {
	// Create a mock OIDC token endpoint
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := map[string]interface{}{
			"access_token": "oidc-access-token",
			"expires_in":   3600,
			"token_type":   "Bearer",
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client, err := NewClientWithOIDC("http://superset.example.com", server.URL, "my-client", "my-secret")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if client.Token != "oidc-access-token" {
		t.Errorf("Expected token 'oidc-access-token', got: %s", client.Token)
	}

	if client.OIDCTokenURL != server.URL {
		t.Errorf("Expected OIDCTokenURL '%s', got: %s", server.URL, client.OIDCTokenURL)
	}

	if client.ClientID != "my-client" {
		t.Errorf("Expected ClientID 'my-client', got: %s", client.ClientID)
	}
}

func TestNewClientWithOIDC_Failure(t *testing.T) {
	// Create a mock OIDC token endpoint that returns an error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`{"error": "invalid_client", "error_description": "Invalid client credentials"}`))
	}))
	defer server.Close()

	_, err := NewClientWithOIDC("http://superset.example.com", server.URL, "bad-client", "bad-secret")
	if err == nil {
		t.Fatal("Expected error for invalid credentials, got nil")
	}
}

func TestAuthenticateOIDC_MissingAccessToken(t *testing.T) {
	// Create a mock OIDC token endpoint that returns no access_token
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := map[string]interface{}{
			"token_type": "Bearer",
			// access_token is missing
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client := &Client{
		Host:         "http://localhost",
		OIDCTokenURL: server.URL,
		ClientID:     "test-client",
		ClientSecret: "test-secret",
	}

	err := client.authenticateOIDC()
	if err == nil {
		t.Fatal("Expected error for missing access_token, got nil")
	}

	if err.Error() != "OIDC token response did not contain access_token" {
		t.Errorf("Unexpected error message: %v", err)
	}
}

func TestClearGlobalDatabaseCache(t *testing.T) {
	// Set up some cache data
	globalDatabasesCacheMutex.Lock()
	globalDatabasesCache = []map[string]interface{}{
		{"id": float64(1), "database_name": "test"},
	}
	globalDatabasesCacheTime = time.Now()
	globalDatabasesCacheMutex.Unlock()

	// Clear the cache
	ClearGlobalDatabaseCache()

	// Verify cache is cleared
	globalDatabasesCacheMutex.RLock()
	defer globalDatabasesCacheMutex.RUnlock()

	if globalDatabasesCache != nil {
		t.Errorf("Expected cache to be nil, got: %v", globalDatabasesCache)
	}

	if !globalDatabasesCacheTime.IsZero() {
		t.Errorf("Expected cache time to be zero, got: %v", globalDatabasesCacheTime)
	}
}

