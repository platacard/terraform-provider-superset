package client

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
)

// registerDatabaseList makes the list endpoint of host return the given connection names.
func registerDatabaseList(host string, names ...string) {
	httpmock.RegisterResponder("POST", host+"/api/v1/security/login",
		httpmock.NewJsonResponderOrPanic(200, map[string]string{"access_token": "token"}))
	rows := make([]interface{}, 0, len(names))
	for i, name := range names {
		rows = append(rows, map[string]interface{}{"id": i + 1, "database_name": name})
	}
	httpmock.RegisterResponder("GET", host+"/api/v1/database/",
		httpmock.NewJsonResponderOrPanic(200, map[string]interface{}{"count": len(rows), "result": rows}))
}

func databaseNames(t *testing.T, c *Client) []string {
	t.Helper()
	rows, err := c.GetAllDatabases()
	if err != nil {
		t.Fatalf("GetAllDatabases: %v", err)
	}
	names := make([]string, 0, len(rows))
	for _, row := range rows {
		names = append(names, row["database_name"].(string))
	}
	return names
}

// Clients of different Superset instances must not see each other's connections, as when
// one program serves several provider configurations.
func TestDatabaseListIsNotSharedBetweenClients(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	registerDatabaseList("http://superset-a", "warehouse-a")
	registerDatabaseList("http://superset-b", "warehouse-b")

	a := NewClient("http://superset-a", "user", "pass")
	b := NewClient("http://superset-b", "user", "pass")

	if got := databaseNames(t, a); len(got) != 1 || got[0] != "warehouse-a" {
		t.Fatalf("client a sees %v, want [warehouse-a]", got)
	}
	if got := databaseNames(t, b); len(got) != 1 || got[0] != "warehouse-b" {
		t.Fatalf("client b sees %v, want [warehouse-b]", got)
	}
}

func TestDatabaseListIsCachedUntilAConnectionChanges(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	registerDatabaseList(testHost, "warehouse")
	httpmock.RegisterResponder("GET", testHost+"/api/v1/security/csrf_token/",
		httpmock.NewJsonResponderOrPanic(200, map[string]string{"result": "csrf"}))
	httpmock.RegisterResponder("DELETE", testHost+"/api/v1/database/1",
		httpmock.NewJsonResponderOrPanic(200, map[string]string{"message": "OK"}))
	listCalls := func() int { return httpmock.GetCallCountInfo()["GET "+testHost+"/api/v1/database/"] }

	c := NewClient(testHost, "user", "pass")
	databaseNames(t, c)
	databaseNames(t, c)
	if n := listCalls(); n != 1 {
		t.Fatalf("expected the list to be fetched once, got %d", n)
	}

	if err := c.DeleteDatabaseConnection(1); err != nil {
		t.Fatalf("DeleteDatabaseConnection: %v", err)
	}
	databaseNames(t, c)
	if n := listCalls(); n != 2 {
		t.Fatalf("expected the list to be fetched again after a change, got %d fetches", n)
	}
}

func TestGetDatabaseNameByIDReadsTheConnection(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	registerLogin("token")
	httpmock.RegisterResponder("GET", testHost+"/api/v1/database/7",
		httpmock.NewJsonResponderOrPanic(200, map[string]interface{}{"id": 7, "result": map[string]interface{}{"database_name": "renamed"}}))
	httpmock.RegisterResponder("GET", testHost+"/api/v1/database/8",
		httpmock.NewJsonResponderOrPanic(404, map[string]string{"message": "Not found"}))

	c := NewClient(testHost, "user", "pass")
	name, err := c.GetDatabaseNameByID(7)
	if err != nil || name != "renamed" {
		t.Fatalf("GetDatabaseNameByID(7) = %q, %v; want renamed", name, err)
	}
	if _, err := c.GetDatabaseNameByID(8); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetDatabaseNameByID(8) error = %v, want ErrNotFound", err)
	}
	if n := httpmock.GetCallCountInfo()["GET "+testHost+"/api/v1/database/"]; n != 0 {
		t.Fatalf("expected no list request, got %d", n)
	}
}

// A server that accepts the connection but never answers must fail the request.
func TestRequestsTimeOut(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/security/login" {
			_, _ = w.Write([]byte(`{"access_token":"token"}`))
			return
		}
		<-release
	}))
	defer server.Close()
	defer close(release)

	saved := httpClient
	httpClient = &http.Client{Timeout: 100 * time.Millisecond}
	defer func() { httpClient = saved }()

	c := NewClient(server.URL, "user", "pass")
	start := time.Now()
	_, err := c.DoRequest("GET", "/api/v1/chart/1", nil)
	if err == nil || !strings.Contains(err.Error(), "Client.Timeout") {
		t.Fatalf("expected a timeout error, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("the request took %v", elapsed)
	}
}
