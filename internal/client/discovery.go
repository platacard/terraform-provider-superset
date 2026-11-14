package client

import (
	"fmt"
	"net/http"
	"net/url"
	"sort"
)

// DatabaseTable is a table or a view of a schema, as the database endpoints list it.
type DatabaseTable struct {
	Name string `json:"value"`
	Type string `json:"type"`
}

// TableColumn is a column of a table, as the database metadata endpoint describes it.
type TableColumn struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	LongType string `json:"longType"`
	Comment  string `json:"comment"`
}

// TableMetadata is what Superset reads about a table from the database.
type TableMetadata struct {
	Name       string        `json:"name"`
	Comment    string        `json:"comment"`
	Columns    []TableColumn `json:"columns"`
	SelectStar string        `json:"selectStar"`
}

// DatabaseCatalogs returns the names of the catalogs of a database connection, sorted, on
// Superset 6; force makes Superset read them from the database instead of its cache.
func (c *Client) DatabaseCatalogs(databaseID int64, force bool) ([]string, error) {
	path := fmt.Sprintf("/api/v1/database/%d/catalogs/", databaseID)
	if force {
		path += "?" + url.Values{"q": {"(force:!t)"}}.Encode()
	}
	resp, err := c.DoRequest(http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}

	var body struct {
		Result []string `json:"result"`
	}
	if err := decodeResponse(resp, &body, fmt.Sprintf("list the catalogs of database %d", databaseID)); err != nil {
		return nil, err
	}
	sort.Strings(body.Result)
	return body.Result, nil
}

// DatabaseSchemas returns the names of the schemas of a database connection, sorted; of the
// given catalog when one is set, on Superset 6, which older versions ignore.
func (c *Client) DatabaseSchemas(databaseID int64, catalog string) ([]string, error) {
	path := fmt.Sprintf("/api/v1/database/%d/schemas/", databaseID)
	if catalog != "" {
		path += "?" + url.Values{"q": {"(catalog:" + risonString(catalog) + ")"}}.Encode()
	}
	resp, err := c.DoRequest(http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}

	var body struct {
		Result []string `json:"result"`
	}
	if err := decodeResponse(resp, &body, fmt.Sprintf("list the schemas of database %d", databaseID)); err != nil {
		return nil, err
	}
	sort.Strings(body.Result)
	return body.Result, nil
}

// DatabaseTables returns the tables and views of a schema, sorted by name; of the given
// catalog when one is set, on Superset 6, which older versions ignore.
func (c *Client) DatabaseTables(databaseID int64, schema, catalog string) ([]DatabaseTable, error) {
	selection := "(schema_name:" + risonString(schema) + ")"
	if catalog != "" {
		selection = "(catalog_name:" + risonString(catalog) + ",schema_name:" + risonString(schema) + ")"
	}
	query := url.Values{"q": {selection}}
	resp, err := c.DoRequest(http.MethodGet, fmt.Sprintf("/api/v1/database/%d/tables/?%s", databaseID, query.Encode()), nil)
	if err != nil {
		return nil, err
	}

	var body struct {
		Result []DatabaseTable `json:"result"`
	}
	if err := decodeResponse(resp, &body, fmt.Sprintf("list the tables of schema %q of database %d", schema, databaseID)); err != nil {
		return nil, err
	}
	sort.Slice(body.Result, func(i, j int) bool { return body.Result[i].Name < body.Result[j].Name })
	return body.Result, nil
}

// DatabaseTableMetadata returns what Superset reads about a table from the database.
func (c *Client) DatabaseTableMetadata(databaseID int64, schema, table string) (*TableMetadata, error) {
	resp, err := c.DoRequest(http.MethodGet, fmt.Sprintf("/api/v1/database/%d/table/%s/%s/", databaseID, url.PathEscape(table), url.PathEscape(schema)), nil)
	if err != nil {
		return nil, err
	}

	var metadata TableMetadata
	if err := decodeResponse(resp, &metadata, fmt.Sprintf("read table %q of schema %q of database %d", table, schema, databaseID)); err != nil {
		return nil, err
	}
	return &metadata, nil
}
