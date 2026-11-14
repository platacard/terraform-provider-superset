package provider

import (
	"net/http"
	"regexp"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/jarcoal/httpmock"
)

// serveLookups serves list endpoints that honour the equality filters of the lookups, so the
// tests prove which filters are sent: a row is only returned when the query carries the
// filters of its key.
func serveLookups() {
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))
	type listing struct {
		path string
		rows map[string]string
	}
	listings := []listing{
		{path: "/api/v1/database/", rows: map[string]string{
			"(col:database_name,opr:eq,value:'Warehouse')": `{"id": 3, "database_name": "Warehouse", "backend": "postgresql", "uuid": "0850dc2e-cca5-43d7-87c7-988fd77b3564", "expose_in_sqllab": true, "allow_dml": false, "allow_run_async": true}`,
		}},
		{path: "/api/v1/dataset/", rows: map[string]string{
			"(col:table_name,opr:eq,value:'sales')":                                                                       `{"id": 10, "table_name": "sales", "schema": "public", "sql": null, "kind": "physical", "database": {"id": 3, "database_name": "Warehouse"}}, {"id": 11, "table_name": "sales", "schema": "archive", "sql": null, "kind": "physical", "database": {"id": 3, "database_name": "Warehouse"}}`,
			"(col:table_name,opr:eq,value:'sales'),(col:schema,opr:eq,value:'archive')":                                   `{"id": 11, "table_name": "sales", "schema": "archive", "sql": null, "kind": "physical", "database": {"id": 3, "database_name": "Warehouse"}}`,
			"(col:table_name,opr:eq,value:'sales'),(col:schema,opr:eq,value:'public'),(col:database,opr:rel_o_m,value:3)": `{"id": 10, "table_name": "sales", "schema": "public", "sql": null, "kind": "physical", "database": {"id": 3, "database_name": "Warehouse"}}`,
			"(col:table_name,opr:eq,value:'signups_per_day')":                                                             `{"id": 12, "table_name": "signups_per_day", "schema": null, "sql": "SELECT day, count(*) FROM signups GROUP BY day", "kind": "virtual", "database": {"id": 3, "database_name": "Warehouse"}}`,
		}},
		{path: "/api/v1/chart/", rows: map[string]string{
			"(col:slice_name,opr:eq,value:'Sales by day')":                                     `{"id": 20, "slice_name": "Sales by day", "viz_type": "table", "datasource_id": 10, "datasource_type": "table", "description": "Daily", "url": "/explore/?slice_id=20"}, {"id": 21, "slice_name": "Sales by day", "viz_type": "table", "datasource_id": 11, "datasource_type": "table", "description": null, "url": "/explore/?slice_id=21"}`,
			"(col:slice_name,opr:eq,value:'Sales by day'),(col:datasource_id,opr:eq,value:11)": `{"id": 21, "slice_name": "Sales by day", "viz_type": "table", "datasource_id": 11, "datasource_type": "table", "description": null, "url": "/explore/?slice_id=21"}`,
		}},
		{path: "/api/v1/dashboard/", rows: map[string]string{
			"(col:slug,opr:eq,value:'sales')":            `{"id": 30, "dashboard_title": "Sales", "slug": "sales", "published": true, "status": "published", "url": "/superset/dashboard/sales/"}`,
			"(col:dashboard_title,opr:eq,value:'Sales')": `{"id": 30, "dashboard_title": "Sales", "slug": "sales", "published": true, "status": "published", "url": "/superset/dashboard/sales/"}, {"id": 31, "dashboard_title": "Sales", "slug": null, "published": false, "status": "draft", "url": "/superset/dashboard/31/"}`,
		}},
		{path: "/api/v1/security/users/", rows: map[string]string{
			"(col:username,opr:eq,value:'analyst')": `{"id": 7, "username": "analyst", "first_name": "Ana", "last_name": "Lyst", "email": "analyst@example.com", "active": true, "roles": [{"id": 4, "name": "Gamma"}, {"id": 5, "name": "sql_lab"}], "groups": [{"id": 2, "name": "analysts"}]}`,
			"(col:username,opr:eq,value:'legacy')":  `{"id": 8, "username": "legacy", "first_name": "Old", "last_name": "User", "email": "legacy@example.com", "active": false, "roles": []}`,
		}},
		{path: "/api/v1/security/groups/", rows: map[string]string{
			"(col:name,opr:eq,value:'analysts')": `{"id": 2, "name": "analysts", "label": "Analysts", "description": null, "roles": [{"id": 4, "name": "Gamma"}], "users": [{"id": 7, "name": "analyst"}]}`,
		}},
		{path: "/api/v1/rowlevelsecurity/", rows: map[string]string{
			"(col:name,opr:eq,value:'Region')": `{"id": 5, "name": "Region", "description": "Rows of the region of the user", "filter_type": "Regular", "group_key": "region", "clause": "region = '{{ current_username() }}'", "tables": [{"id": 10, "schema": "public", "table_name": "sales"}], "roles": [{"id": 4, "name": "Gamma"}]}`,
		}},
		{path: "/api/v1/saved_query/", rows: map[string]string{
			"(col:label,opr:eq,value:'Top customers')":                                    `{"id": 40, "label": "Top customers", "schema": "public", "catalog": null, "sql": "SELECT * FROM customers LIMIT 10", "description": "Ten best", "database": {"id": 3, "database_name": "Warehouse"}}, {"id": 41, "label": "Top customers", "schema": null, "catalog": "pgdb", "sql": "SELECT 1", "description": null, "database": {"id": 4, "database_name": "Staging"}}`,
			"(col:label,opr:eq,value:'Top customers'),(col:database,opr:rel_o_m,value:4)": `{"id": 41, "label": "Top customers", "schema": null, "catalog": "pgdb", "sql": "SELECT 1", "description": null, "database": {"id": 4, "database_name": "Staging"}}`,
		}},
		{path: "/api/v1/annotation_layer/", rows: map[string]string{
			"(col:name,opr:eq,value:'Releases')": `{"id": 50, "name": "Releases", "descr": "Production releases"}`,
			"(col:name,opr:eq,value:'Twice')":    `{"id": 51, "name": "Twice", "descr": ""}, {"id": 52, "name": "Twice", "descr": ""}`,
		}},
		{path: "/api/v1/tag/", rows: map[string]string{
			"(col:name,opr:eq,value:'finance')": `{"id": 60, "name": "finance", "description": "Finance team", "type": 1}`,
		}},
		{path: "/api/v1/report/", rows: map[string]string{
			"(col:name,opr:eq,value:'Weekly sales')":                                 `{"id": 70, "name": "Weekly sales", "type": "Report", "description": "Every Monday", "crontab": "0 9 * * 1", "timezone": "UTC", "active": true, "chart_id": null, "dashboard_id": 30, "last_state": "Success"}, {"id": 71, "name": "Weekly sales", "type": "Alert", "description": null, "crontab": "0 * * * *", "timezone": "Europe/Berlin", "active": false, "chart_id": 20, "dashboard_id": null, "last_state": "Not triggered"}`,
			"(col:name,opr:eq,value:'Weekly sales'),(col:type,opr:eq,value:'Alert')": `{"id": 71, "name": "Weekly sales", "type": "Alert", "description": null, "crontab": "0 * * * *", "timezone": "Europe/Berlin", "active": false, "chart_id": 20, "dashboard_id": null, "last_state": "Not triggered"}`,
		}},
		{path: "/api/v1/theme/", rows: map[string]string{
			"(col:theme_name,opr:eq,value:'Corporate')": `{"id": 80, "theme_name": "Corporate", "json_data": "{\"token\": {\"colorPrimary\": \"#1f77b4\"}}", "uuid": "ad3b15d6-d271-410b-8781-c8e76073b594", "is_system": false, "is_system_default": true, "is_system_dark": false}`,
		}},
	}
	for _, l := range listings {
		rows := l.rows
		httpmock.RegisterResponder("GET", listEndpoint(l.path),
			func(req *http.Request) (*http.Response, error) {
				q := req.URL.Query().Get("q")
				for filters, result := range rows {
					if strings.Contains(q, "filters:!("+filters+")") {
						return httpmock.NewStringResponse(200, `{"count": 1, "result": [`+result+`]}`), nil
					}
				}
				return httpmock.NewStringResponse(200, `{"count": 0, "result": []}`), nil
			})
	}
}

func TestAccDatabaseDataSource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	serveLookups()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
data "superset_database" "test" {
  database_name = "Warehouse"
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.superset_database.test", "id", "3"),
					resource.TestCheckResourceAttr("data.superset_database.test", "backend", "postgresql"),
					resource.TestCheckResourceAttr("data.superset_database.test", "uuid", "0850dc2e-cca5-43d7-87c7-988fd77b3564"),
					resource.TestCheckResourceAttr("data.superset_database.test", "expose_in_sqllab", "true"),
					resource.TestCheckResourceAttr("data.superset_database.test", "allow_dml", "false"),
					resource.TestCheckResourceAttr("data.superset_database.test", "allow_run_async", "true"),
				),
			},
			{
				Config: providerConfig + `
data "superset_database" "missing" {
  database_name = "warehouse"
}
`,
				ExpectError: regexp.MustCompile(`Could not look up database "warehouse": database "warehouse": not found`),
			},
		},
	})
}

func TestAccDatasetDataSource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	serveLookups()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
data "superset_dataset" "test" {
  table_name    = "sales"
  schema        = "public"
  database_name = "Warehouse"
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.superset_dataset.test", "id", "10"),
					resource.TestCheckResourceAttr("data.superset_dataset.test", "database_id", "3"),
					resource.TestCheckResourceAttr("data.superset_dataset.test", "kind", "physical"),
					resource.TestCheckNoResourceAttr("data.superset_dataset.test", "sql"),
					resource.TestCheckResourceAttr("data.superset_dataset.test", "perm", "[Warehouse].[sales](id:10)"),
					resource.TestCheckResourceAttr("data.superset_dataset.test", "schema_perm", "[Warehouse].[public]"),
				),
			},
			{
				Config: providerConfig + `
data "superset_dataset" "test" {
  table_name = "sales"
  schema     = "archive"
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.superset_dataset.test", "id", "11"),
					resource.TestCheckResourceAttr("data.superset_dataset.test", "database_name", "Warehouse"),
				),
			},
			{
				// A virtual dataset has a query and no schema.
				Config: providerConfig + `
data "superset_dataset" "test" {
  table_name = "signups_per_day"
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.superset_dataset.test", "id", "12"),
					resource.TestCheckResourceAttr("data.superset_dataset.test", "kind", "virtual"),
					resource.TestCheckResourceAttr("data.superset_dataset.test", "sql", "SELECT day, count(*) FROM signups GROUP BY day"),
					resource.TestCheckNoResourceAttr("data.superset_dataset.test", "schema"),
					resource.TestCheckNoResourceAttr("data.superset_dataset.test", "schema_perm"),
				),
			},
			{
				Config: providerConfig + `
data "superset_dataset" "ambiguous" {
  table_name = "sales"
}
`,
				ExpectError: regexp.MustCompile(`2 datasets are named "sales" \(ids 10, 11\)`),
			},
			{
				Config: providerConfig + `
data "superset_dataset" "missing" {
  table_name = "orders"
}
`,
				ExpectError: regexp.MustCompile(`Could not look up dataset "orders": not found`),
			},
			{
				Config: providerConfig + `
data "superset_dataset" "missing" {
  table_name    = "sales"
  database_name = "Lake"
}
`,
				ExpectError: regexp.MustCompile(`database "Lake": not found`),
			},
		},
	})
}

func TestAccChartDataSource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	serveLookups()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
data "superset_chart" "test" {
  name          = "Sales by day"
  datasource_id = 11
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.superset_chart.test", "id", "21"),
					resource.TestCheckResourceAttr("data.superset_chart.test", "viz_type", "table"),
					resource.TestCheckResourceAttr("data.superset_chart.test", "datasource_type", "table"),
					resource.TestCheckNoResourceAttr("data.superset_chart.test", "description"),
					resource.TestCheckResourceAttr("data.superset_chart.test", "url", "/explore/?slice_id=21"),
				),
			},
			{
				Config: providerConfig + `
data "superset_chart" "ambiguous" {
  name = "Sales by day"
}
`,
				ExpectError: regexp.MustCompile(`2 charts are named "Sales by day" \(ids 20, 21\)`),
			},
			{
				Config: providerConfig + `
data "superset_chart" "missing" {
  name = "sales by day"
}
`,
				ExpectError: regexp.MustCompile(`Could not look up chart "sales by day": not found`),
			},
		},
	})
}

func TestAccDashboardDataSource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	serveLookups()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
data "superset_dashboard" "test" {
  slug = "sales"
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.superset_dashboard.test", "id", "30"),
					resource.TestCheckResourceAttr("data.superset_dashboard.test", "title", "Sales"),
					resource.TestCheckResourceAttr("data.superset_dashboard.test", "published", "true"),
					resource.TestCheckResourceAttr("data.superset_dashboard.test", "status", "published"),
					resource.TestCheckResourceAttr("data.superset_dashboard.test", "url", "/superset/dashboard/sales/"),
				),
			},
			{
				Config: providerConfig + `
data "superset_dashboard" "ambiguous" {
  title = "Sales"
}
`,
				ExpectError: regexp.MustCompile(`2 dashboards are titled "Sales" \(ids 30, 31\)`),
			},
			{
				Config: providerConfig + `
data "superset_dashboard" "missing" {
  slug = "orders"
}
`,
				ExpectError: regexp.MustCompile(`Could not look up dashboard "orders": not found`),
			},
			{
				Config: providerConfig + `
data "superset_dashboard" "invalid" {
  slug  = "sales"
  title = "Sales"
}
`,
				ExpectError: regexp.MustCompile(`Set either slug or title`),
			},
			{
				Config: providerConfig + `
data "superset_dashboard" "invalid" {
}
`,
				ExpectError: regexp.MustCompile(`Set either slug or title`),
			},
		},
	})
}
