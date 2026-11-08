package client

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// decodeResponse closes the response body, maps 404 to ErrNotFound, reports any status outside
// the 2xx range together with the response body, and decodes the body into out when out is not
// nil. Any 2xx counts as success because the status differs between Superset versions for the
// same operation (an update returns 200 on recent versions and 201 on 3.1).
func decodeResponse(resp *http.Response, out any, action string) error {
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("failed to %s: %w", action, ErrNotFound)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
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
