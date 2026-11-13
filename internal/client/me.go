package client

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
)

// CurrentUser is the user the provider is authenticated as.
type CurrentUser struct {
	ID        int64  `json:"id"`
	Username  string `json:"username"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	Email     string `json:"email"`
	IsActive  bool   `json:"is_active"`
	// Roles are the names of the roles of the user, sorted.
	Roles []string
}

// currentUserHint explains the 401 of the current user endpoints on versions before 6, which
// only answer to a browser session there.
const currentUserHint = "The current user endpoints answer a bearer token on Superset 6 only; on older versions they need a browser session."

// GetCurrentUser returns the user the client is authenticated as, with the names of its roles,
// on Superset 6.
func (c *Client) GetCurrentUser() (*CurrentUser, error) {
	var me struct {
		Result CurrentUser `json:"result"`
	}
	if err := c.getMe("/api/v1/me/", &me, "fetch the current user"); err != nil {
		return nil, err
	}
	var roles struct {
		Result struct {
			Roles map[string]json.RawMessage `json:"roles"`
		} `json:"result"`
	}
	if err := c.getMe("/api/v1/me/roles/", &roles, "fetch the roles of the current user"); err != nil {
		return nil, err
	}

	user := me.Result
	user.Roles = make([]string, 0, len(roles.Result.Roles))
	for name := range roles.Result.Roles {
		user.Roles = append(user.Roles, name)
	}
	sort.Strings(user.Roles)
	return &user, nil
}

// getMe fetches one of the current user endpoints, explaining the 401 of older versions.
func (c *Client) getMe(path string, out any, action string) error {
	resp, err := c.DoRequest(http.MethodGet, path, nil)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusUnauthorized {
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to %s, status code: %d, response: %s\n%s", action, resp.StatusCode, string(body), currentUserHint)
	}
	return decodeResponse(resp, out, action)
}
