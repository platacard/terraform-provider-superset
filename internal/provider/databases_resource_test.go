package provider

import (
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/jarcoal/httpmock"
)

func TestAccDatabaseResource(t *testing.T) {
	// Activate httpmock
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	// Mock the Superset API login response
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))

	// Mock the Superset API CSRF token response
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/csrf_token/",
		httpmock.NewStringResponder(200, `{"result": "fake-csrf-token"}`))

	// Mock the Superset API response for creating a database
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/database/",
		httpmock.NewStringResponder(201, `{
			"id": 208,
			"result": {
				"allow_ctas": false,
				"allow_cvas": false,
				"allow_dml": false,
				"allow_run_async": true,
				"cache_timeout": null,
				"configuration_method": "sqlalchemy_form",
				"database_name": "DWH_database_connection4",
				"driver": "psycopg2",
				"expose_in_sqllab": true,
				"extra": "{\"client_encoding\": \"utf8\"}",
				"parameters": {
					"database": "superset_db",
					"encryption": false,
					"host": "pg.db.ro.domain.com",
					"password": "XXXXXXXXXX",
					"port": 5432,
					"query": {},
					"username": "superset_user"
				},
				"sqlalchemy_uri": "postgresql://superset_user:XXXXXXXXXX@pg.db.ro.domain.com:5432/superset_db",
				"uuid": "f5007595-5a43-45d8-a1da-9612bdb12b22"
			}
		}`))

	// Mock the Superset API response for reading a database connection
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/database/208/connection",
		httpmock.NewStringResponder(200, `{
			"result": {
				"allow_ctas": false,
				"allow_cvas": false,
				"allow_dml": false,
				"allow_run_async": true,
				"cache_timeout": null,
				"configuration_method": "sqlalchemy_form",
				"database_name": "DWH_database_connection4",
				"driver": "psycopg2",
				"expose_in_sqllab": true,
				"extra": "{\"client_encoding\": \"utf8\"}",
				"parameters": {
					"database": "superset_db",
					"encryption": false,
					"host": "pg.db.ro.domain.com",
					"password": "XXXXXXXXXX",
					"port": 5432,
					"query": {},
					"username": "superset_user"
				},
				"sqlalchemy_uri": "postgresql://superset_user:XXXXXXXXXX@pg.db.ro.domain.com:5432/superset_db",
				"uuid": "f5007595-5a43-45d8-a1da-9612bdb12b22"
			}
		}`))

	// Mock the Superset API response for updating a database connection
	httpmock.RegisterResponder("PUT", "http://superset-host/api/v1/database/208",
		httpmock.NewStringResponder(200, `{
			"id": 208,
			"result": {
				"allow_ctas": false,
				"allow_cvas": false,
				"allow_dml": false,
				"allow_run_async": true,
				"cache_timeout": null,
				"configuration_method": "sqlalchemy_form",
				"database_name": "DWH_database_connection4",
				"driver": "psycopg2",
				"expose_in_sqllab": false,
				"extra": "{\"client_encoding\": \"utf8\"}",
				"parameters": {
					"database": "superset_db",
					"encryption": false,
					"host": "pg.db.ro.domain.com",
					"password": "XXXXXXXXXX",
					"port": 5432,
					"query": {},
					"username": "superset_user"
				},
				"sqlalchemy_uri": "postgresql://superset_user:XXXXXXXXXX@pg.db.ro.domain.com:5432/superset_db",
				"uuid": "f5007595-5a43-45d8-a1da-9612bdb12b22"
			}
		}`))

	// Mock the Superset API response for deleting a database
	httpmock.RegisterResponder("DELETE", "http://superset-host/api/v1/database/208",
		httpmock.NewStringResponder(200, ""))

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			// Create and Read testing
			{
				Config: providerConfig + testAccDatabaseResourceConfig,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_database.test", "connection_name", "DWH_database_connection4"),
					resource.TestCheckResourceAttr("superset_database.test", "db_engine", "postgresql"),
					resource.TestCheckResourceAttr("superset_database.test", "db_user", "superset_user"),
					resource.TestCheckResourceAttr("superset_database.test", "db_host", "pg.db.ro.domain.com"),
					resource.TestCheckResourceAttr("superset_database.test", "db_port", "5432"),
					resource.TestCheckResourceAttr("superset_database.test", "db_name", "superset_db"),
					resource.TestCheckResourceAttr("superset_database.test", "allow_ctas", "false"),
					resource.TestCheckResourceAttr("superset_database.test", "allow_cvas", "false"),
					resource.TestCheckResourceAttr("superset_database.test", "allow_dml", "false"),
					resource.TestCheckResourceAttr("superset_database.test", "allow_run_async", "true"),
					resource.TestCheckResourceAttr("superset_database.test", "expose_in_sqllab", "true"),
				),
			},
		},
	})
}

const testAccDatabaseResourceConfig = `
resource "superset_database" "test" {
  connection_name = "DWH_database_connection4"
  db_engine = "postgresql"
  db_user = "superset_user"
  db_pass = "dbpassword"
  db_host = "pg.db.ro.domain.com"
  db_port = 5432
  db_name = "superset_db"
  allow_ctas = false
  allow_cvas = false
  allow_dml = false
  allow_run_async = true
  expose_in_sqllab = true
}
`
