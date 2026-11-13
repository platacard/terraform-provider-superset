package provider

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"sync"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
	"github.com/jarcoal/httpmock"
)

// fakeSQLLab serves the SQL Lab execute endpoint of the fake Superset host and keeps the last
// request body. A query on the table nosuch fails the way Superset reports it.
type fakeSQLLab struct {
	mu         sync.Mutex
	last       map[string]any
	executions []string
}

func (f *fakeSQLLab) serve() {
	serveLookups()
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/csrf_token/",
		httpmock.NewStringResponder(200, `{"result": "fake-csrf-token"}`))
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/sqllab/execute/",
		func(req *http.Request) (*http.Response, error) {
			var payload map[string]any
			if err := json.NewDecoder(req.Body).Decode(&payload); err != nil {
				return httpmock.NewStringResponse(400, err.Error()), nil
			}
			sql, _ := payload["sql"].(string)
			f.mu.Lock()
			f.last = payload
			f.executions = append(f.executions, sql)
			f.mu.Unlock()
			if regexp.MustCompile(`nosuch`).MatchString(sql) {
				return httpmock.NewStringResponse(500, `{"errors": [{"message": "postgresql error: relation \"nosuch\" does not exist\n", "error_type": "GENERIC_DB_ENGINE_ERROR", "level": "error", "extra": {}}]}`), nil
			}
			if limit, _ := payload["queryLimit"].(float64); limit == 1 {
				return httpmock.NewStringResponse(200, `{"status": "success", "query_id": 9,
					"columns": [{"column_name": "region", "name": "region", "type": "STRING", "type_generic": 1, "is_dttm": false}],
					"data": [{"region": "north"}],
					"query": {"id": 9, "limit": 1, "limitingFactor": "DROPDOWN", "rows": 1}}`), nil
			}
			return httpmock.NewStringResponse(200, `{"status": "success", "query_id": 8,
				"columns": [{"column_name": "region", "name": "region", "type": "STRING", "type_generic": 1, "is_dttm": false}, {"column_name": "total", "name": "total", "type": "DECIMAL", "type_generic": 0, "is_dttm": false}, {"column_name": "day", "name": "day", "type": "DATE", "type_generic": 2, "is_dttm": true}, {"column_name": "open", "name": "open", "type": "BOOLEAN", "type_generic": 3, "is_dttm": false}],
				"data": [{"region": "north", "total": 10.5, "day": "2026-01-01", "open": true}, {"region": "south", "total": null, "day": null, "open": false}],
				"query": {"id": 8, "limit": 1000, "limitingFactor": "NOT_LIMITED", "rows": 2}}`), nil
		})
}

func (f *fakeSQLLab) sent(key string) any {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.last[key]
}

// executed returns the statements run so far.
func (f *fakeSQLLab) executed() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.executions...)
}

func TestAccSQLQueryDataSource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeSQLLab{}
	fake.serve()
	sent := func(key string, want any) resource.TestCheckFunc {
		return func(*terraform.State) error {
			if got := fake.sent(key); got != want {
				return fmt.Errorf("%s sent as %#v, want %#v", key, got, want)
			}
			return nil
		}
	}

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
data "superset_sql_query" "test" {
  database_name = "Warehouse"
  schema        = "public"
  sql           = "SELECT region, sum(amount) AS total, min(day) AS day, bool_or(open) AS open FROM sales GROUP BY region"
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.superset_sql_query.test", "database_id", "3"),
					resource.TestCheckResourceAttr("data.superset_sql_query.test", "limit", "1000"),
					resource.TestCheckResourceAttr("data.superset_sql_query.test", "row_count", "2"),
					resource.TestCheckResourceAttr("data.superset_sql_query.test", "limited", "false"),
					resource.TestCheckResourceAttr("data.superset_sql_query.test", "columns.#", "4"),
					resource.TestCheckResourceAttr("data.superset_sql_query.test", "columns.1.name", "total"),
					resource.TestCheckResourceAttr("data.superset_sql_query.test", "columns.1.type", "DECIMAL"),
					resource.TestCheckResourceAttr("data.superset_sql_query.test", "columns.2.is_datetime", "true"),
					resource.TestCheckResourceAttr("data.superset_sql_query.test", "rows.#", "2"),
					resource.TestCheckResourceAttr("data.superset_sql_query.test", "rows.0.%", "4"),
					resource.TestCheckResourceAttr("data.superset_sql_query.test", "rows.0.region", "north"),
					resource.TestCheckResourceAttr("data.superset_sql_query.test", "rows.0.total", "10.5"),
					resource.TestCheckResourceAttr("data.superset_sql_query.test", "rows.0.day", "2026-01-01"),
					resource.TestCheckResourceAttr("data.superset_sql_query.test", "rows.0.open", "true"),
					// Null values are left out of the map.
					resource.TestCheckResourceAttr("data.superset_sql_query.test", "rows.1.%", "2"),
					resource.TestCheckResourceAttr("data.superset_sql_query.test", "rows.1.region", "south"),
					resource.TestCheckResourceAttr("data.superset_sql_query.test", "rows.1.open", "false"),
					resource.TestCheckResourceAttr("data.superset_sql_query.test", "rows_json", `[{"region": "north", "total": 10.5, "day": "2026-01-01", "open": true}, {"region": "south", "total": null, "day": null, "open": false}]`),
					sent("database_id", float64(3)),
					sent("schema", "public"),
					sent("queryLimit", float64(1000)),
					sent("runAsync", false),
					sent("templateParams", nil),
				),
			},
			{
				// The connection by id, with a limit and template parameters.
				Config: providerConfig + `
data "superset_sql_query" "test" {
  database_id     = 3
  sql             = "SELECT region FROM sales WHERE region = '{{ region }}'"
  limit           = 1
  template_params = jsonencode({ region = "north" })
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.superset_sql_query.test", "database_id", "3"),
					resource.TestCheckNoResourceAttr("data.superset_sql_query.test", "database_name"),
					resource.TestCheckResourceAttr("data.superset_sql_query.test", "limit", "1"),
					resource.TestCheckResourceAttr("data.superset_sql_query.test", "limited", "true"),
					resource.TestCheckResourceAttr("data.superset_sql_query.test", "row_count", "1"),
					resource.TestCheckResourceAttr("data.superset_sql_query.test", "rows.0.region", "north"),
					sent("queryLimit", float64(1)),
					sent("templateParams", `{"region":"north"}`),
					sent("schema", nil),
				),
			},
			{
				Config: providerConfig + `
data "superset_sql_query" "test" {
  database_name = "Warehouse"
  sql           = "SELECT * FROM nosuch"
}
`,
				ExpectError: regexp.MustCompile(`relation "nosuch" does not exist`),
			},
			{
				Config: providerConfig + `
data "superset_sql_query" "test" {
  database_name = "Lake"
  sql           = "SELECT 1"
}
`,
				ExpectError: regexp.MustCompile(`database "Lake": not found`),
			},
			{
				Config: providerConfig + `
data "superset_sql_query" "test" {
  database_name = "Warehouse"
  database_id   = 3
  sql           = "SELECT 1"
}
`,
				ExpectError: regexp.MustCompile(`Conflicting Attributes`),
			},
			{
				Config: providerConfig + `
data "superset_sql_query" "test" {
  database_id     = 3
  sql             = "SELECT 1"
  template_params = "[1]"
}
`,
				ExpectError: regexp.MustCompile(`Invalid Template Parameters`),
			},
		},
	})
}
