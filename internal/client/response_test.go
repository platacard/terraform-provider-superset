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
