package client

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestDecodeResponse(t *testing.T) {
	response := func(status int, body string) *http.Response {
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body))}
	}
	var out struct {
		ID int64 `json:"id"`
	}

	for _, status := range []int{200, 201, 204} {
		if err := decodeResponse(response(status, `{"id": 7}`), &out, "update"); err != nil {
			t.Errorf("status %d: unexpected error: %v", status, err)
		}
	}
	if out.ID != 7 {
		t.Errorf("expected the body to be decoded, got id %d", out.ID)
	}
	if err := decodeResponse(response(200, `{}`), nil, "delete"); err != nil {
		t.Errorf("nil out: unexpected error: %v", err)
	}
	if err := decodeResponse(response(404, `{"message": "Not found"}`), &out, "fetch"); !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound for 404, got %v", err)
	}
	err := decodeResponse(response(500, `{"message": "boom"}`), &out, "create")
	if err == nil || !strings.Contains(err.Error(), "status code: 500") || !strings.Contains(err.Error(), "boom") {
		t.Errorf("expected the status and body in the error, got %v", err)
	}
	if err := decodeResponse(response(200, `not json`), &out, "fetch"); err == nil {
		t.Error("expected a decoding error for a malformed body")
	}
}

func TestDecodeResponseMissingEndpoint(t *testing.T) {
	flaskNotFound := `{"errors": [{"message": "404 Not Found: The requested URL was not found on the server. If you entered the URL manually please check your spelling and try again."}]}`
	cases := []struct {
		name, path, body string
		wantHint         bool
		wantNotFound     bool
	}{
		{"security endpoint missing", "/api/v1/security/users/", flaskNotFound, true, false},
		{"other endpoint missing", "/api/v1/theme/", flaskNotFound, false, false},
		{"security object missing", "/api/v1/security/users/7", `{"message": "Not found"}`, false, true},
		{"login route is not the security API", "/api/v1/security/login", flaskNotFound, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, _ := http.NewRequest(http.MethodGet, "http://superset-host"+tc.path, nil)
			resp := &http.Response{StatusCode: http.StatusNotFound, Body: io.NopCloser(strings.NewReader(tc.body)), Request: req}
			err := decodeResponse(resp, nil, "fetch user")
			if err == nil {
				t.Fatal("expected an error")
			}
			if errors.Is(err, ErrNotFound) != tc.wantNotFound {
				t.Fatalf("ErrNotFound = %v, want %v (%v)", errors.Is(err, ErrNotFound), tc.wantNotFound, err)
			}
			if strings.Contains(err.Error(), "FAB_ADD_SECURITY_API") != tc.wantHint {
				t.Fatalf("hint present = %v, want %v (%v)", !tc.wantHint, tc.wantHint, err)
			}
		})
	}
}
