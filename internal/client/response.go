package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// securityAPIHint explains the 404 that Superset returns for the users, roles and permissions
// endpoints when the Flask-AppBuilder security API is not enabled.
const securityAPIHint = "Superset does not expose this endpoint: the users, roles and permissions endpoints belong to the " +
	"Flask-AppBuilder security API, which Superset only enables with FAB_ADD_SECURITY_API = True in superset_config.py."

// supersetSixHint explains the 404 of the endpoints that only recent versions of Superset have.
const supersetSixHint = "This endpoint does not exist on this version of Superset: themes, groups and the permission sync " +
	"of a database need Superset 6."

// supersetSixEndpoint reports whether the request went to an endpoint of Superset 6.
func supersetSixEndpoint(resp *http.Response) bool {
	if resp.Request == nil || resp.Request.URL == nil {
		return false
	}
	path := resp.Request.URL.Path
	return strings.HasPrefix(path, "/api/v1/theme/") || strings.HasPrefix(path, "/api/v1/security/groups/") || strings.HasSuffix(path, "/sync_permissions/")
}

// missingEndpoint reports whether the response is the 404 of a route that does not exist, as
// opposed to the 404 of an object that does not exist.
func missingEndpoint(resp *http.Response, body []byte) bool {
	return resp.StatusCode == http.StatusNotFound && strings.Contains(string(body), "The requested URL was not found on the server")
}

// securityEndpoint reports whether the request went to the Flask-AppBuilder security API.
func securityEndpoint(resp *http.Response) bool {
	if resp.Request == nil || resp.Request.URL == nil {
		return false
	}
	path := resp.Request.URL.Path
	for _, own := range []string{"/api/v1/security/login", "/api/v1/security/refresh", "/api/v1/security/csrf_token/", "/api/v1/security/guest_token/"} {
		if strings.HasPrefix(path, own) {
			return false
		}
	}
	return strings.HasPrefix(path, "/api/v1/security/")
}

// responseError describes a failed request with its status and body, and adds the security API
// hint when a security endpoint does not exist on the instance.
func responseError(action string, resp *http.Response, body []byte) error {
	message := fmt.Sprintf("failed to %s, status code: %d, response: %s", action, resp.StatusCode, string(body))
	switch {
	case missingEndpoint(resp, body) && supersetSixEndpoint(resp):
		message += "\n" + supersetSixHint
	case missingEndpoint(resp, body) && securityEndpoint(resp):
		message += "\n" + securityAPIHint
	}
	return errors.New(message)
}

// objectError describes a failed request for one object. The 404 of an object that does not
// exist is ErrNotFound; the 404 of an endpoint that does not exist is an error with a hint.
func objectError(action string, resp *http.Response, body []byte) error {
	if resp.StatusCode == http.StatusNotFound && !missingEndpoint(resp, body) {
		return fmt.Errorf("failed to %s: %w", action, ErrNotFound)
	}
	return responseError(action, resp, body)
}

// decodeResponse closes the response body, maps the 404 of a missing object to ErrNotFound,
// reports any status outside the 2xx range together with the response body, and decodes the
// body into out when out is not nil. Any 2xx counts as success because the status differs
// between Superset versions for the same operation (an update returns 200 on recent versions
// and 201 on 3.1). The 404 of a missing endpoint is reported as an error, not as ErrNotFound.
func decodeResponse(resp *http.Response, out any, action string) error {
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode == http.StatusNotFound && !missingEndpoint(resp, body) {
			return fmt.Errorf("failed to %s: %w", action, ErrNotFound)
		}
		return responseError(action, resp, body)
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("failed to decode the %s response: %w", action, err)
	}
	return nil
}

// marshal is json.Marshal under a name that does not collide with the MarshalJSON methods of payloads.
func marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}
