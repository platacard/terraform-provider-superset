package client

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestCreateDashboard_IdempotentBySlug_UpdatesExisting(t *testing.T) {
	var getListCalls int32
	var csrfCalls int32
	var putCalls int32
	var postCalls int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/dashboard/" && r.URL.RawQuery == "q=(page_size:5000)":
			atomic.AddInt32(&getListCalls, 1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"result": []map[string]any{
					{"id": float64(333), "slug": "existing-dashboard"},
				},
			})
			return

		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/security/csrf_token/":
			atomic.AddInt32(&csrfCalls, 1)
			_ = json.NewEncoder(w).Encode(map[string]any{"result": "csrf"})
			return

		case r.Method == http.MethodPut && r.URL.Path == "/api/v1/dashboard/333":
			atomic.AddInt32(&putCalls, 1)
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{"result": map[string]any{"dashboard_title": "Existing Dashboard"}})
			return

		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/dashboard/":
			atomic.AddInt32(&postCalls, 1)
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": float64(999)})
			return
		}

		http.NotFound(w, r)
	}))
	defer server.Close()

	c := &Client{Host: server.URL, Token: "token"}
	id, err := c.CreateDashboard(DashboardCreateRequest{
		DashboardTitle: "Existing Dashboard",
		Slug:           "existing-dashboard",
		Published:      true,
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if id != 333 {
		t.Fatalf("expected existing dashboard id 333, got %d", id)
	}
	if atomic.LoadInt32(&getListCalls) != 1 {
		t.Fatalf("expected 1 dashboards list call, got %d", getListCalls)
	}
	if atomic.LoadInt32(&csrfCalls) != 1 {
		t.Fatalf("expected 1 csrf call, got %d", csrfCalls)
	}
	if atomic.LoadInt32(&putCalls) != 1 {
		t.Fatalf("expected 1 put update call, got %d", putCalls)
	}
	if atomic.LoadInt32(&postCalls) != 0 {
		t.Fatalf("expected 0 post create calls, got %d", postCalls)
	}
}

func TestCreateDashboard_CreatesWhenSlugNotFound(t *testing.T) {
	var getListCalls int32
	var csrfCalls int32
	var postCalls int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/dashboard/" && r.URL.RawQuery == "q=(page_size:5000)":
			atomic.AddInt32(&getListCalls, 1)
			_ = json.NewEncoder(w).Encode(map[string]any{"result": []any{}})
			return

		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/security/csrf_token/":
			atomic.AddInt32(&csrfCalls, 1)
			_ = json.NewEncoder(w).Encode(map[string]any{"result": "csrf"})
			return

		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/dashboard/":
			atomic.AddInt32(&postCalls, 1)
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": float64(444)})
			return
		}

		http.NotFound(w, r)
	}))
	defer server.Close()

	c := &Client{Host: server.URL, Token: "token"}
	id, err := c.CreateDashboard(DashboardCreateRequest{
		DashboardTitle: "New Dashboard",
		Slug:           "new-dashboard",
		Published:      false,
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if id != 444 {
		t.Fatalf("expected created dashboard id 444, got %d", id)
	}
	if atomic.LoadInt32(&getListCalls) != 1 {
		t.Fatalf("expected 1 dashboards list call, got %d", getListCalls)
	}
	if atomic.LoadInt32(&csrfCalls) != 1 {
		t.Fatalf("expected 1 csrf call, got %d", csrfCalls)
	}
	if atomic.LoadInt32(&postCalls) != 1 {
		t.Fatalf("expected 1 post create call, got %d", postCalls)
	}
}

