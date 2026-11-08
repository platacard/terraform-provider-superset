package provider

import (
	"net/http"
	"regexp"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/jarcoal/httpmock"
)

// serveDiscovery serves the database list with its name filter and the discovery endpoints of
// database 3 of the fake Superset host.
func serveDiscovery() {
	serveLookups()
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/database/3/schemas/",
		httpmock.NewStringResponder(200, `{"result": ["public", "archive"]}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/database/3/tables/",
		func(req *http.Request) (*http.Response, error) {
			if req.URL.Query().Get("q") == "(schema_name:'public')" {
				return httpmock.NewStringResponse(200, `{"count": 2, "result": [{"value": "sales", "type": "table", "extra": null}, {"value": "daily_sales", "type": "view", "extra": null}]}`), nil
			}
			return httpmock.NewStringResponse(200, `{"count": 0, "result": []}`), nil
		})
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/database/3/table/sales/public/",
		httpmock.NewStringResponder(200, `{"name": "sales", "comment": "One row per sale", "columns": [{"name": "id", "type": "INTEGER", "longType": "INTEGER", "comment": null, "keys": []}, {"name": "amount", "type": "NUMERIC", "longType": "NUMERIC(10, 2)", "comment": "In euros", "keys": []}], "selectStar": "SELECT * FROM public.sales LIMIT 100"}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/database/3/table/orders/public/",
		httpmock.NewStringResponder(422, `{"message": "orders"}`))
}

func TestAccDatabaseSchemasDataSource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	serveDiscovery()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
data "superset_database_schemas" "test" {
  database_name = "Warehouse"
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.superset_database_schemas.test", "database_id", "3"),
					resource.TestCheckResourceAttr("data.superset_database_schemas.test", "schemas.#", "2"),
					resource.TestCheckResourceAttr("data.superset_database_schemas.test", "schemas.0", "archive"),
					resource.TestCheckResourceAttr("data.superset_database_schemas.test", "schemas.1", "public"),
				),
			},
			{
				Config: providerConfig + `
data "superset_database_schemas" "missing" {
  database_name = "Lake"
}
`,
				ExpectError: regexp.MustCompile(`database "Lake": not found`),
			},
		},
	})
}

func TestAccDatabaseTablesDataSource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	serveDiscovery()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
data "superset_database_tables" "test" {
  database_name = "Warehouse"
  schema        = "public"
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.superset_database_tables.test", "database_id", "3"),
					resource.TestCheckResourceAttr("data.superset_database_tables.test", "tables.#", "2"),
					resource.TestCheckResourceAttr("data.superset_database_tables.test", "tables.0.name", "daily_sales"),
					resource.TestCheckResourceAttr("data.superset_database_tables.test", "tables.0.type", "view"),
					resource.TestCheckResourceAttr("data.superset_database_tables.test", "tables.1.name", "sales"),
					resource.TestCheckResourceAttr("data.superset_database_tables.test", "tables.1.type", "table"),
				),
			},
			{
				Config: providerConfig + `
data "superset_database_tables" "empty" {
  database_name = "Warehouse"
  schema        = "archive"
}
`,
				Check: resource.TestCheckResourceAttr("data.superset_database_tables.empty", "tables.#", "0"),
			},
		},
	})
}

func TestAccDatabaseTableDataSource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	serveDiscovery()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
data "superset_database_table" "test" {
  database_name = "Warehouse"
  schema        = "public"
  table_name    = "sales"
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.superset_database_table.test", "database_id", "3"),
					resource.TestCheckResourceAttr("data.superset_database_table.test", "comment", "One row per sale"),
					resource.TestCheckResourceAttr("data.superset_database_table.test", "columns.#", "2"),
					resource.TestCheckResourceAttr("data.superset_database_table.test", "columns.0.name", "id"),
					resource.TestCheckResourceAttr("data.superset_database_table.test", "columns.0.type", "INTEGER"),
					resource.TestCheckNoResourceAttr("data.superset_database_table.test", "columns.0.comment"),
					resource.TestCheckResourceAttr("data.superset_database_table.test", "columns.1.name", "amount"),
					resource.TestCheckResourceAttr("data.superset_database_table.test", "columns.1.long_type", "NUMERIC(10, 2)"),
					resource.TestCheckResourceAttr("data.superset_database_table.test", "columns.1.comment", "In euros"),
				),
			},
			{
				Config: providerConfig + `
data "superset_database_table" "missing" {
  database_name = "Warehouse"
  schema        = "public"
  table_name    = "orders"
}
`,
				ExpectError: regexp.MustCompile(`(?s)read table "orders".*422`),
			},
		},
	})
}
