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

// DatabaseSchemas returns the names of the schemas of a database connection, sorted.
func (c *Client) DatabaseSchemas(databaseID int64) ([]string, error) {
	resp, err := c.DoRequest(http.MethodGet, fmt.Sprintf("/api/v1/database/%d/schemas/", databaseID), nil)
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

// DatabaseTables returns the tables and views of a schema, sorted by name.
func (c *Client) DatabaseTables(databaseID int64, schema string) ([]DatabaseTable, error) {
	query := url.Values{"q": {"(schema_name:" + risonString(schema) + ")"}}
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
