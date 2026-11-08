package client

import (
	"fmt"
	"net/http"
)

const savedQueriesPath = "/api/v1/saved_query/"

// SavedQuery is a query saved in SQL Lab.
type SavedQuery struct {
	ID                 int64
	DatabaseID         int64
	Label              string
	SQL                string
	Description        string
	Schema             string
	Catalog            string
	TemplateParameters string
}

// savedQueryPayload is the body of the create and update requests. Optional text fields are
// sent as null when empty, so an update can clear them. The catalog is only sent when it is
// set or has to be cleared, because Superset versions before 4.1 reject the field.
type savedQueryPayload struct {
	DatabaseID         int64   `json:"db_id"`
	Label              string  `json:"label"`
	SQL                string  `json:"sql"`
	Description        *string `json:"description"`
	Schema             *string `json:"schema"`
	Catalog            *string `json:"catalog,omitempty"`
	TemplateParameters *string `json:"template_parameters"`
	clearCatalog       bool
}

// MarshalJSON adds an explicit null catalog when the catalog has to be cleared.
func (p savedQueryPayload) MarshalJSON() ([]byte, error) {
	type plain savedQueryPayload
	if !p.clearCatalog {
		return marshal(plain(p))
	}
	return marshal(struct {
		plain
		Catalog *string `json:"catalog"`
	}{plain: plain(p)})
}

func newSavedQueryPayload(query SavedQuery, clearCatalog bool) savedQueryPayload {
	return savedQueryPayload{
		DatabaseID:         query.DatabaseID,
		Label:              query.Label,
		SQL:                query.SQL,
		Description:        nullIfEmpty(query.Description),
		Schema:             nullIfEmpty(query.Schema),
		Catalog:            nullIfEmpty(query.Catalog),
		TemplateParameters: nullIfEmpty(query.TemplateParameters),
		clearCatalog:       clearCatalog && query.Catalog == "",
	}
}

// savedQueryResult is the query as the API returns it.
type savedQueryResult struct {
	Database struct {
		ID int64 `json:"id"`
	} `json:"database"`
	Label              string `json:"label"`
	SQL                string `json:"sql"`
	Description        string `json:"description"`
	Schema             string `json:"schema"`
	Catalog            string `json:"catalog"`
	TemplateParameters string `json:"template_parameters"`
}

func savedQueryPath(id int64) string {
	return fmt.Sprintf("%s%d", savedQueriesPath, id)
}

// CreateSavedQuery creates a saved query and returns its id.
func (c *Client) CreateSavedQuery(query SavedQuery) (int64, error) {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return 0, err
	}

	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodPost, savedQueriesPath, newSavedQueryPayload(query, false), headers, cookies)
	if err != nil {
		return 0, err
	}

	var created struct {
		ID int64 `json:"id"`
	}
	if err := decodeResponse(resp, &created, "create saved query"); err != nil {
		return 0, err
	}
	return created.ID, nil
}

// GetSavedQuery returns the saved query with the given id, or ErrNotFound.
func (c *Client) GetSavedQuery(id int64) (*SavedQuery, error) {
	resp, err := c.DoRequest(http.MethodGet, savedQueryPath(id), nil)
	if err != nil {
		return nil, err
	}

	var body struct {
		Result savedQueryResult `json:"result"`
	}
	if err := decodeResponse(resp, &body, "fetch saved query"); err != nil {
		return nil, err
	}

	query := &SavedQuery{
		ID:                 id,
		DatabaseID:         body.Result.Database.ID,
		Label:              body.Result.Label,
		SQL:                body.Result.SQL,
		Description:        body.Result.Description,
		Schema:             body.Result.Schema,
		Catalog:            body.Result.Catalog,
		TemplateParameters: body.Result.TemplateParameters,
	}
	return query, nil
}

// UpdateSavedQuery replaces the attributes of a saved query. clearCatalog removes a catalog
// that was set before; it is false when the catalog was never set.
func (c *Client) UpdateSavedQuery(id int64, query SavedQuery, clearCatalog bool) error {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodPut, savedQueryPath(id), newSavedQueryPayload(query, clearCatalog), headers, cookies)
	if err != nil {
		return err
	}
	return decodeResponse(resp, nil, "update saved query")
}

// DeleteSavedQuery deletes a saved query; a query that no longer exists is reported as ErrNotFound.
func (c *Client) DeleteSavedQuery(id int64) error {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodDelete, savedQueryPath(id), nil, headers, cookies)
	if err != nil {
		return err
	}
	return decodeResponse(resp, nil, "delete saved query")
}
