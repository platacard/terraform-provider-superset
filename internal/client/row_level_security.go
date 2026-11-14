package client

import (
	"fmt"
	"net/http"
)

const rowLevelSecurityPath = "/api/v1/rowlevelsecurity/"

// RowLevelSecurity is a row level security rule: a SQL clause that Superset appends to the
// queries of the given datasets for the given roles.
type RowLevelSecurity struct {
	ID          int64
	Name        string
	Description string
	FilterType  string
	GroupKey    string
	Clause      string
	TableIDs    []int64
	RoleIDs     []int64
}

// rowLevelSecurityPayload is the body of the create and update requests. Optional text fields
// are sent as null when empty, so an update can clear them.
type rowLevelSecurityPayload struct {
	Name        string  `json:"name"`
	Description *string `json:"description"`
	FilterType  string  `json:"filter_type"`
	GroupKey    *string `json:"group_key"`
	Clause      string  `json:"clause"`
	Tables      []int64 `json:"tables"`
	Roles       []int64 `json:"roles"`
}

func newRowLevelSecurityPayload(rule RowLevelSecurity) rowLevelSecurityPayload {
	return rowLevelSecurityPayload{
		Name:        rule.Name,
		Description: nullIfEmpty(rule.Description),
		FilterType:  rule.FilterType,
		GroupKey:    nullIfEmpty(rule.GroupKey),
		Clause:      rule.Clause,
		Tables:      nonNil(rule.TableIDs),
		Roles:       nonNil(rule.RoleIDs),
	}
}

// rowLevelSecurityResult is the rule as the API returns it.
type rowLevelSecurityResult struct {
	Name        string      `json:"name"`
	Description string      `json:"description"`
	FilterType  string      `json:"filter_type"`
	GroupKey    string      `json:"group_key"`
	Clause      string      `json:"clause"`
	Tables      []namedItem `json:"tables"`
	Roles       []namedItem `json:"roles"`
}

func rowLevelSecurityRulePath(id int64) string {
	return fmt.Sprintf("%s%d", rowLevelSecurityPath, id)
}

// CreateRowLevelSecurity creates a rule and returns its id.
func (c *Client) CreateRowLevelSecurity(rule RowLevelSecurity) (int64, error) {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return 0, err
	}

	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodPost, rowLevelSecurityPath, newRowLevelSecurityPayload(rule), headers, cookies)
	if err != nil {
		return 0, err
	}

	var created struct {
		ID int64 `json:"id"`
	}
	if err := decodeResponse(resp, &created, "create row level security rule"); err != nil {
		return 0, err
	}
	return created.ID, nil
}

// GetRowLevelSecurity returns the rule with the given id, or ErrNotFound.
func (c *Client) GetRowLevelSecurity(id int64) (*RowLevelSecurity, error) {
	resp, err := c.DoRequest(http.MethodGet, rowLevelSecurityRulePath(id), nil)
	if err != nil {
		return nil, err
	}

	var body struct {
		Result rowLevelSecurityResult `json:"result"`
	}
	if err := decodeResponse(resp, &body, "fetch row level security rule"); err != nil {
		return nil, err
	}

	rule := &RowLevelSecurity{
		ID:          id,
		Name:        body.Result.Name,
		Description: body.Result.Description,
		FilterType:  body.Result.FilterType,
		GroupKey:    body.Result.GroupKey,
		Clause:      body.Result.Clause,
		TableIDs:    ids(body.Result.Tables),
		RoleIDs:     ids(body.Result.Roles),
	}
	return rule, nil
}

// UpdateRowLevelSecurity replaces every attribute of the rule.
func (c *Client) UpdateRowLevelSecurity(id int64, rule RowLevelSecurity) error {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodPut, rowLevelSecurityRulePath(id), newRowLevelSecurityPayload(rule), headers, cookies)
	if err != nil {
		return err
	}
	return decodeResponse(resp, nil, "update row level security rule")
}

// DeleteRowLevelSecurity deletes a rule; a rule that no longer exists is reported as ErrNotFound.
func (c *Client) DeleteRowLevelSecurity(id int64) error {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodDelete, rowLevelSecurityRulePath(id), nil, headers, cookies)
	if err != nil {
		return err
	}
	return decodeResponse(resp, nil, "delete row level security rule")
}

func nullIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// nonNil turns a nil slice into an empty one, so it is sent as [] rather than null.
func nonNil(values []int64) []int64 {
	if values == nil {
		return []int64{}
	}
	return values
}

func ids(items []namedItem) []int64 {
	result := make([]int64, 0, len(items))
	for _, item := range items {
		result = append(result, item.ID)
	}
	return result
}

// FindRowLevelSecurityByName returns the rule with exactly the given name, or ErrNotFound.
// Rule names are unique in Superset; the name is compared again because the server
// comparison can be case-insensitive.
func (c *Client) FindRowLevelSecurityByName(name string) (*RowLevelSecurity, error) {
	type row struct {
		ID int64 `json:"id"`
		rowLevelSecurityResult
	}
	rows, err := listInto[row](c, rowLevelSecurityPath, equals("name", name))
	if err != nil {
		return nil, fmt.Errorf("row level security rules named %q: %w", name, err)
	}
	for _, r := range rows {
		if r.Name == name {
			return &RowLevelSecurity{
				ID:          r.ID,
				Name:        r.Name,
				Description: r.Description,
				FilterType:  r.FilterType,
				GroupKey:    r.GroupKey,
				Clause:      r.Clause,
				TableIDs:    ids(r.Tables),
				RoleIDs:     ids(r.Roles),
			}, nil
		}
	}
	return nil, ErrNotFound
}
