package client

import (
	"fmt"
	"net/http"
)

const themesPath = "/api/v1/theme/"

// Theme is a theme of the Superset UI: a JSON document of design tokens that dashboards can
// apply, and that can be the default or the dark theme of the whole instance.
type Theme struct {
	ID            int64
	Name          string
	JSONData      string
	UUID          string
	IsSystem      bool
	SystemDefault bool
	SystemDark    bool
}

type themePayload struct {
	Name     string `json:"theme_name"`
	JSONData string `json:"json_data"`
}

type themeResult struct {
	Name          string `json:"theme_name"`
	JSONData      string `json:"json_data"`
	UUID          string `json:"uuid"`
	IsSystem      bool   `json:"is_system"`
	SystemDefault bool   `json:"is_system_default"`
	SystemDark    bool   `json:"is_system_dark"`
}

func themePath(id int64) string {
	return fmt.Sprintf("%s%d", themesPath, id)
}

// CreateTheme creates a theme and returns its id.
func (c *Client) CreateTheme(theme Theme) (int64, error) {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return 0, err
	}

	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodPost, themesPath, themePayload{Name: theme.Name, JSONData: theme.JSONData}, headers, cookies)
	if err != nil {
		return 0, err
	}

	var created struct {
		ID int64 `json:"id"`
	}
	if err := decodeResponse(resp, &created, "create theme"); err != nil {
		return 0, err
	}
	return created.ID, nil
}

// GetTheme returns the theme with the given id, or ErrNotFound.
func (c *Client) GetTheme(id int64) (*Theme, error) {
	resp, err := c.DoRequest(http.MethodGet, themePath(id), nil)
	if err != nil {
		return nil, err
	}

	var body struct {
		Result themeResult `json:"result"`
	}
	if err := decodeResponse(resp, &body, "fetch theme"); err != nil {
		return nil, err
	}
	return &Theme{
		ID:            id,
		Name:          body.Result.Name,
		JSONData:      body.Result.JSONData,
		UUID:          body.Result.UUID,
		IsSystem:      body.Result.IsSystem,
		SystemDefault: body.Result.SystemDefault,
		SystemDark:    body.Result.SystemDark,
	}, nil
}

// UpdateTheme replaces the name and the tokens of a theme.
func (c *Client) UpdateTheme(id int64, theme Theme) error {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodPut, themePath(id), themePayload{Name: theme.Name, JSONData: theme.JSONData}, headers, cookies)
	if err != nil {
		return err
	}
	return decodeResponse(resp, nil, "update theme")
}

// SetSystemTheme makes the theme the default theme of the instance, or its dark theme.
func (c *Client) SetSystemTheme(id int64, dark bool) error {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodPut, themePath(id)+"/set_system_"+systemThemeKind(dark), nil, headers, cookies)
	if err != nil {
		return err
	}
	return decodeResponse(resp, nil, "set the system "+systemThemeKind(dark)+" theme")
}

// UnsetSystemTheme removes the default theme of the instance, or its dark theme, whichever
// theme holds the place.
func (c *Client) UnsetSystemTheme(dark bool) error {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodDelete, themesPath+"unset_system_"+systemThemeKind(dark), nil, headers, cookies)
	if err != nil {
		return err
	}
	return decodeResponse(resp, nil, "unset the system "+systemThemeKind(dark)+" theme")
}

func systemThemeKind(dark bool) string {
	if dark {
		return "dark"
	}
	return "default"
}

// DeleteTheme deletes a theme; a theme that no longer exists is reported as ErrNotFound.
func (c *Client) DeleteTheme(id int64) error {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodDelete, themePath(id), nil, headers, cookies)
	if err != nil {
		return err
	}
	return decodeResponse(resp, nil, "delete theme")
}

// FindThemes returns the themes with exactly the given name. The name is compared again
// because the server comparison can be case-insensitive.
func (c *Client) FindThemes(name string) ([]Theme, error) {
	type row struct {
		ID int64 `json:"id"`
		themeResult
	}
	rows, err := listInto[row](c, themesPath, equals("theme_name", name))
	if err != nil {
		return nil, fmt.Errorf("themes named %q: %w", name, err)
	}
	matches := make([]Theme, 0, len(rows))
	for _, r := range rows {
		if r.Name == name {
			matches = append(matches, Theme{
				ID:            r.ID,
				Name:          r.Name,
				JSONData:      r.JSONData,
				UUID:          r.UUID,
				IsSystem:      r.IsSystem,
				SystemDefault: r.SystemDefault,
				SystemDark:    r.SystemDark,
			})
		}
	}
	return matches, nil
}
