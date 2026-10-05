package client

import (
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/jarcoal/httpmock"
)

const testHost = "http://superset-host"

// registerLogin makes the login endpoint return the given tokens one by one (the last one is repeated).
func registerLogin(tokens ...string) {
	var mu sync.Mutex
	i := 0
	httpmock.RegisterResponder("POST", testHost+"/api/v1/security/login",
		func(req *http.Request) (*http.Response, error) {
			mu.Lock()
			defer mu.Unlock()
			token := tokens[i]
			if i < len(tokens)-1 {
				i++
			}
			return httpmock.NewJsonResponse(200, map[string]string{"access_token": token})
		})
}

// registerDatabases accepts only requests authenticated with the given token.
func registerDatabases(validToken string) {
	httpmock.RegisterResponder("GET", testHost+"/api/v1/database/",
		func(req *http.Request) (*http.Response, error) {
			if req.Header.Get("Authorization") != "Bearer "+validToken {
				return httpmock.NewStringResponse(401, `{"msg":"Token has expired"}`), nil
			}
			return httpmock.NewJsonResponse(200, map[string]interface{}{"result": []interface{}{}})
		})
}

func loginCalls() int {
	return httpmock.GetCallCountInfo()["POST "+testHost+"/api/v1/security/login"]
}

func TestNewClientDoesNotSendRequests(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	NewClient(testHost, "user", "pass")

	if n := httpmock.GetTotalCallCount(); n != 0 {
		t.Fatalf("expected no requests, got %d", n)
	}
}

func TestConcurrentRequestsLogInOnce(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	registerLogin("token-1")
	registerDatabases("token-1")

	c := NewClient(testHost, "user", "pass")
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := c.DoRequest("GET", "/api/v1/database/", nil)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			resp.Body.Close()
			if resp.StatusCode != 200 {
				t.Errorf("expected 200, got %d", resp.StatusCode)
			}
		}()
	}
	wg.Wait()

	if n := loginCalls(); n != 1 {
		t.Fatalf("expected a single login, got %d", n)
	}
}

func TestExpiredTokenIsRefreshedOnceForConcurrentRequests(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	registerLogin("expired-token", "fresh-token")
	registerDatabases("fresh-token")

	c := NewClient(testHost, "user", "pass")
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := c.DoRequest("GET", "/api/v1/database/", nil)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			resp.Body.Close()
			if resp.StatusCode != 200 {
				t.Errorf("expected 200 after token refresh, got %d", resp.StatusCode)
			}
		}()
	}
	wg.Wait()

	if n := loginCalls(); n != 2 {
		t.Fatalf("expected the initial login and a single refresh, got %d logins", n)
	}
}

func TestRequestIsRetriedOnlyOnce(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	registerLogin("token-1", "token-2", "token-3")
	registerDatabases("never-valid")

	c := NewClient(testHost, "user", "pass")
	resp, err := c.DoRequest("GET", "/api/v1/database/", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != 401 {
		t.Fatalf("expected the 401 to be returned after a single retry, got %d", resp.StatusCode)
	}
	if n := loginCalls(); n != 2 {
		t.Fatalf("expected 2 logins (initial + one refresh), got %d", n)
	}
}

func TestMissingSettingsAreReportedOnFirstUse(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	c := NewClient("", "", "")
	_, err := c.DoRequest("GET", "/api/v1/database/", nil)
	if err == nil {
		t.Fatal("expected an error for missing settings")
	}
	for _, want := range []string{"host (SUPERSET_HOST)", "username (SUPERSET_USERNAME)", "password (SUPERSET_PASSWORD)"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("expected error to mention %q, got: %v", want, err)
		}
	}
	if n := httpmock.GetTotalCallCount(); n != 0 {
		t.Fatalf("expected no requests, got %d", n)
	}
}
