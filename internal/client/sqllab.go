package client

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

const sqlLabExecutePath = "/api/v1/sqllab/execute/"

// SQLQuery is a query to run through SQL Lab on a database connection.
type SQLQuery struct {
	DatabaseID int64
	SQL        string
	Schema     string
	// Catalog is the catalog of the connection to run in, on Superset 6; empty for the default.
	Catalog string
	// Limit caps the number of rows returned; zero leaves the cap to Superset (SQL_MAX_ROW).
	Limit int64
	// TemplateParams is a JSON object of the Jinja parameters of the query, when templates are
	// enabled; empty when none.
	TemplateParams string
}

// SQLColumn is a column of a query result.
type SQLColumn struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	IsDateTime bool   `json:"is_dttm"`
}

// SQLResult is the result of a query: its columns and its rows, each a map from the column
// name to the value as Superset serialised it.
type SQLResult struct {
	Columns []SQLColumn
	Rows    []map[string]json.RawMessage
	// RawRows is the rows exactly as Superset serialised them, a JSON array of objects.
	RawRows json.RawMessage
	// Limited reports that the rows were cut by the limit of the query.
	Limited bool
	// QueryID is the id of the run in the query history of SQL Lab.
	QueryID int64
}

type sqlLabPayload struct {
	DatabaseID     int64  `json:"database_id"`
	SQL            string `json:"sql"`
	Schema         string `json:"schema,omitempty"`
	Catalog        string `json:"catalog,omitempty"`
	QueryLimit     int64  `json:"queryLimit,omitempty"`
	TemplateParams string `json:"templateParams,omitempty"`
	RunAsync       bool   `json:"runAsync"`
}

// sqlLabError is one of the errors SQL Lab reports when a query fails.
type sqlLabError struct {
	Message   string `json:"message"`
	ErrorType string `json:"error_type"`
}

// ExecuteSQL runs a query synchronously through SQL Lab and returns its result. The
// connection has to be exposed in SQL Lab, and a query that changes data needs DML allowed on
// it. The errors Superset reports for a failed query are returned as the error message.
func (c *Client) ExecuteSQL(query SQLQuery) (*SQLResult, error) {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return nil, err
	}

	payload := sqlLabPayload{
		DatabaseID:     query.DatabaseID,
		SQL:            query.SQL,
		Schema:         query.Schema,
		Catalog:        query.Catalog,
		QueryLimit:     query.Limit,
		TemplateParams: query.TemplateParams,
	}
	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodPost, sqlLabExecutePath, payload, headers, cookies)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("run the query: read the response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, sqlLabFailure(resp, body)
	}

	var result struct {
		Status  string          `json:"status"`
		QueryID int64           `json:"query_id"`
		Columns []SQLColumn     `json:"columns"`
		Data    json.RawMessage `json:"data"`
		Query   struct {
			LimitingFactor string `json:"limitingFactor"`
		} `json:"query"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("run the query: decode the response: %w", err)
	}
	if result.Status != "" && result.Status != "success" {
		return nil, fmt.Errorf("run the query: Superset reported the status %q", result.Status)
	}
	var rows []map[string]json.RawMessage
	if len(result.Data) > 0 {
		if err := json.Unmarshal(result.Data, &rows); err != nil {
			return nil, fmt.Errorf("run the query: decode the rows: %w", err)
		}
	}
	if rows == nil {
		rows = []map[string]json.RawMessage{}
		result.Data = json.RawMessage("[]")
	}
	return &SQLResult{
		Columns: result.Columns,
		Rows:    rows,
		RawRows: result.Data,
		Limited: result.Query.LimitingFactor != "" && result.Query.LimitingFactor != "NOT_LIMITED",
		QueryID: result.QueryID,
	}, nil
}

// sqlLabFailure turns the errors of a failed query into an error with their messages, or
// into the generic response error when the body has no errors list.
func sqlLabFailure(resp *http.Response, body []byte) error {
	var failure struct {
		Errors []sqlLabError `json:"errors"`
	}
	if json.Unmarshal(body, &failure) != nil || len(failure.Errors) == 0 {
		return responseError("run the query", resp, body)
	}
	messages := make([]string, 0, len(failure.Errors))
	for _, e := range failure.Errors {
		message := strings.TrimSpace(e.Message)
		if e.ErrorType != "" {
			message = fmt.Sprintf("%s (%s)", message, e.ErrorType)
		}
		messages = append(messages, message)
	}
	return fmt.Errorf("run the query: %s", strings.Join(messages, "; "))
}

// SQLValueText renders a value of a query result as text: strings as they are, numbers and
// booleans as Superset wrote them, and anything else as its JSON. It returns ok false for a
// null value.
func SQLValueText(value json.RawMessage) (string, bool) {
	trimmed := strings.TrimSpace(string(value))
	if trimmed == "" || trimmed == "null" {
		return "", false
	}
	var text string
	if trimmed[0] == '"' && json.Unmarshal(value, &text) == nil {
		return text, true
	}
	return trimmed, true
}
