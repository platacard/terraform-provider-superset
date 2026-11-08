package client

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// decodeResponse closes the response body, maps 404 to ErrNotFound, reports any other status
// than want together with the response body, and decodes the body into out when out is not nil.
func decodeResponse(resp *http.Response, want int, out any, action string) error {
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("failed to %s: %w", action, ErrNotFound)
	}
	if resp.StatusCode != want {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to %s, status code: %d, response: %s", action, resp.StatusCode, string(body))
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("failed to decode the %s response: %w", action, err)
	}
	return nil
}
