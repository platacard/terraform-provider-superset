package client

import (
	"fmt"
	"net/http"
)

const (
	permissionsPath     = "/api/v1/security/permissions/"
	viewMenusPath       = "/api/v1/security/resources/"
	permissionViewsPath = "/api/v1/security/permissions-resources/"
)

// PermissionView is a permission granted on a view menu, the pair that roles are given.
type PermissionView struct {
	ID             int64
	PermissionName string
	ViewMenuName   string
}

// FindPermissionID returns the id of the permission with exactly the given name, or
// ErrNotFound. Permissions come from the code of Superset and of its security manager; the
// API cannot create them.
func (c *Client) FindPermissionID(name string) (int64, error) {
	id, err := c.findIDByName(permissionsPath, name)
	if err != nil {
		return 0, fmt.Errorf("permission %q: %w", name, err)
	}
	return id, nil
}

// FindViewMenuID returns the id of the view menu with exactly the given name, or ErrNotFound.
func (c *Client) FindViewMenuID(name string) (int64, error) {
	id, err := c.findIDByName(viewMenusPath, name)
	if err != nil {
		return 0, fmt.Errorf("view menu %q: %w", name, err)
	}
	return id, nil
}

// CreateViewMenu creates a view menu and returns its id. The endpoint fails on a name that
// exists, so callers look the name up first.
func (c *Client) CreateViewMenu(name string) (int64, error) {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return 0, err
	}

	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodPost, viewMenusPath, map[string]string{"name": name}, headers, cookies)
	if err != nil {
		return 0, err
	}

	var created struct {
		ID int64 `json:"id"`
	}
	if err := decodeResponse(resp, &created, "create view menu"); err != nil {
		return 0, err
	}
	return created.ID, nil
}

// DeleteViewMenu deletes a view menu; one that no longer exists is reported as ErrNotFound.
func (c *Client) DeleteViewMenu(id int64) error {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodDelete, fmt.Sprintf("%s%d", viewMenusPath, id), nil, headers, cookies)
	if err != nil {
		return err
	}
	return decodeResponse(resp, nil, "delete view menu")
}

// ViewMenuPermissionViews returns the permissions granted on the view menu, keyed by
// permission name, straight from the server.
func (c *Client) ViewMenuPermissionViews(viewMenuID int64) (map[string]int64, error) {
	rows, err := listInto[permissionView](c, permissionViewsPath, relatedTo("view_menu", viewMenuID))
	if err != nil {
		return nil, err
	}
	pairs := make(map[string]int64, len(rows))
	for _, row := range rows {
		pairs[row.Permission.Name] = row.ID
	}
	return pairs, nil
}

// CreatePermissionView grants the permission on the view menu and returns the id of the
// pair. The endpoint fails on a pair that exists, so callers look the pair up first. The
// permissions of the view menu cached for the role permissions are dropped.
func (c *Client) CreatePermissionView(permissionID, viewMenuID int64, viewMenuName string) (int64, error) {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return 0, err
	}

	payload := map[string]int64{"permission_id": permissionID, "view_menu_id": viewMenuID}
	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodPost, permissionViewsPath, payload, headers, cookies)
	if err != nil {
		return 0, err
	}

	var created struct {
		ID int64 `json:"id"`
	}
	if err := decodeResponse(resp, &created, "create permission view"); err != nil {
		return 0, err
	}
	c.permissions.forget(viewMenuName)
	return created.ID, nil
}

// GetPermissionView returns the pair with the given id, or ErrNotFound.
func (c *Client) GetPermissionView(id int64) (*PermissionView, error) {
	resp, err := c.DoRequest(http.MethodGet, fmt.Sprintf("%s%d", permissionViewsPath, id), nil)
	if err != nil {
		return nil, err
	}

	var body struct {
		Result struct {
			Permission namedItem `json:"permission"`
			ViewMenu   namedItem `json:"view_menu"`
		} `json:"result"`
	}
	if err := decodeResponse(resp, &body, "fetch permission view"); err != nil {
		return nil, err
	}
	return &PermissionView{ID: id, PermissionName: body.Result.Permission.Name, ViewMenuName: body.Result.ViewMenu.Name}, nil
}

// DeletePermissionView removes the pair; one that no longer exists is reported as ErrNotFound.
// The permissions of the view menu cached for the role permissions are dropped.
func (c *Client) DeletePermissionView(id int64, viewMenuName string) error {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodDelete, fmt.Sprintf("%s%d", permissionViewsPath, id), nil, headers, cookies)
	if err != nil {
		return err
	}
	c.permissions.forget(viewMenuName)
	return decodeResponse(resp, nil, "delete permission view")
}
