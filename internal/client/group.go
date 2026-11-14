package client

import (
	"fmt"
	"net/http"
)

const groupsPath = "/api/v1/security/groups/"

// Group is a group of users that grants its roles to its members.
type Group struct {
	ID          int64
	Name        string
	Label       string
	Description string
	Roles       []int64
	// Users are the ids of the members. A nil slice leaves the members as they are, which
	// keeps the memberships the users set themselves; an empty one removes every member.
	Users []int64
}

// groupPayload is the body of the create and update requests. The nullable fields are sent as
// null when empty, so an update can clear them; the roles are always sent, so an update can
// revoke them. Users is a pointer so an empty list is sent, which removes the members, while
// a nil one is left out, which keeps them.
type groupPayload struct {
	Name        string   `json:"name"`
	Label       *string  `json:"label"`
	Description *string  `json:"description"`
	Roles       []int64  `json:"roles"`
	Users       *[]int64 `json:"users,omitempty"`
}

func newGroupPayload(group Group) groupPayload {
	payload := groupPayload{
		Name:        group.Name,
		Label:       nullIfEmpty(group.Label),
		Description: nullIfEmpty(group.Description),
		Roles:       nonNil(group.Roles),
	}
	if group.Users != nil {
		users := nonNil(group.Users)
		payload.Users = &users
	}
	return payload
}

type groupResult struct {
	Name        string      `json:"name"`
	Label       string      `json:"label"`
	Description string      `json:"description"`
	Roles       []namedItem `json:"roles"`
	Users       []namedItem `json:"users"`
}

func groupPath(id int64) string {
	return fmt.Sprintf("%s%d", groupsPath, id)
}

// CreateGroup creates a group and returns its id.
func (c *Client) CreateGroup(group Group) (int64, error) {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return 0, err
	}

	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodPost, groupsPath, newGroupPayload(group), headers, cookies)
	if err != nil {
		return 0, err
	}

	var created struct {
		ID int64 `json:"id"`
	}
	if err := decodeResponse(resp, &created, "create group"); err != nil {
		return 0, err
	}
	return created.ID, nil
}

// GetGroup returns the group with the given id, or ErrNotFound.
func (c *Client) GetGroup(id int64) (*Group, error) {
	resp, err := c.DoRequest(http.MethodGet, groupPath(id), nil)
	if err != nil {
		return nil, err
	}

	var body struct {
		Result groupResult `json:"result"`
	}
	if err := decodeResponse(resp, &body, "fetch group"); err != nil {
		return nil, err
	}
	return &Group{
		ID:          id,
		Name:        body.Result.Name,
		Label:       body.Result.Label,
		Description: body.Result.Description,
		Roles:       ids(body.Result.Roles),
		Users:       ids(body.Result.Users),
	}, nil
}

// UpdateGroup replaces the attributes and roles of a group, and its members when they are set.
func (c *Client) UpdateGroup(id int64, group Group) error {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodPut, groupPath(id), newGroupPayload(group), headers, cookies)
	if err != nil {
		return err
	}
	return decodeResponse(resp, nil, "update group")
}

// DeleteGroup deletes a group; a group that no longer exists is reported as ErrNotFound.
func (c *Client) DeleteGroup(id int64) error {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodDelete, groupPath(id), nil, headers, cookies)
	if err != nil {
		return err
	}
	return decodeResponse(resp, nil, "delete group")
}

// SyncDatabasePermissions makes Superset refresh the permissions of the catalogs and schemas
// of a database connection. A connection that no longer exists is reported as ErrNotFound.
func (c *Client) SyncDatabasePermissions(databaseID int64) error {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodPost, fmt.Sprintf("/api/v1/database/%d/sync_permissions/", databaseID), nil, headers, cookies)
	if err != nil {
		return err
	}
	return decodeResponse(resp, nil, "sync the permissions of the database")
}

// FindGroupByName returns the group with exactly the given name, or ErrNotFound. Group names
// are unique; the name is compared again because the server comparison can be
// case-insensitive.
func (c *Client) FindGroupByName(name string) (*Group, error) {
	type row struct {
		ID int64 `json:"id"`
		groupResult
	}
	rows, err := listInto[row](c, groupsPath, equals("name", name))
	if err != nil {
		return nil, fmt.Errorf("groups named %q: %w", name, err)
	}
	for _, r := range rows {
		if r.Name == name {
			return &Group{
				ID:          r.ID,
				Name:        r.Name,
				Label:       r.Label,
				Description: r.Description,
				Roles:       ids(r.Roles),
				Users:       ids(r.Users),
			}, nil
		}
	}
	return nil, ErrNotFound
}
