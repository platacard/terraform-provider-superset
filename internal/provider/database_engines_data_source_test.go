package provider

import (
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/jarcoal/httpmock"
)

func TestAccDatabaseEnginesDataSource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))
	// The order of the response is not the order of the data source: preferred engines come
	// first, then the others by name.
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/database/available/",
		httpmock.NewStringResponder(200, `{"databases": [
  {"available_drivers": ["rest"], "engine": "trino", "engine_information": {"disable_ssh_tunneling": false, "supports_file_upload": true}, "name": "Trino", "parameters": {}, "preferred": false, "sqlalchemy_uri_placeholder": "engine+driver://user:password@host:port/dbname"},
  {"available_drivers": ["pysqlite"], "engine": "sqlite", "engine_information": {"disable_ssh_tunneling": true, "supports_file_upload": true}, "name": "SQLite", "preferred": true, "sqlalchemy_uri_placeholder": "engine+driver://user:password@host:port/dbname"},
  {"available_drivers": ["psycopg2"], "default_driver": "auroradataapi", "engine": "postgresql", "engine_information": {"disable_ssh_tunneling": false, "supports_file_upload": true}, "name": "Aurora PostgreSQL (Data API)", "preferred": false, "sqlalchemy_uri_placeholder": "postgresql+auroradataapi://{aws_access_id}:{aws_secret_access_key}@/{database_name}"},
  {"available_drivers": ["psycopg2"], "default_driver": "psycopg2", "engine": "postgresql", "engine_information": {"disable_ssh_tunneling": false, "supports_dynamic_catalog": true, "supports_file_upload": true, "supports_oauth2": false}, "name": "PostgreSQL", "preferred": true, "sqlalchemy_uri_placeholder": "postgresql://user:password@host:port/dbname[?key=value&key=value...]", "supports_oauth2": false}
]}`))

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
data "superset_database_engines" "all" {}

data "superset_database_engines" "postgres" {
  engine = "postgresql"
}

data "superset_database_engines" "none" {
  engine = "oracle"
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.superset_database_engines.all", "engines.#", "4"),
					resource.TestCheckResourceAttr("data.superset_database_engines.all", "engines.0.name", "PostgreSQL"),
					resource.TestCheckResourceAttr("data.superset_database_engines.all", "engines.0.engine", "postgresql"),
					resource.TestCheckResourceAttr("data.superset_database_engines.all", "engines.0.default_driver", "psycopg2"),
					resource.TestCheckResourceAttr("data.superset_database_engines.all", "engines.0.available_drivers.#", "1"),
					resource.TestCheckResourceAttr("data.superset_database_engines.all", "engines.0.available_drivers.0", "psycopg2"),
					resource.TestCheckResourceAttr("data.superset_database_engines.all", "engines.0.sqlalchemy_uri_placeholder", "postgresql://user:password@host:port/dbname[?key=value&key=value...]"),
					resource.TestCheckResourceAttr("data.superset_database_engines.all", "engines.0.preferred", "true"),
					resource.TestCheckResourceAttr("data.superset_database_engines.all", "engines.0.supports_file_upload", "true"),
					resource.TestCheckResourceAttr("data.superset_database_engines.all", "engines.0.supports_ssh_tunneling", "true"),
					resource.TestCheckResourceAttr("data.superset_database_engines.all", "engines.1.name", "SQLite"),
					resource.TestCheckNoResourceAttr("data.superset_database_engines.all", "engines.1.default_driver"),
					resource.TestCheckResourceAttr("data.superset_database_engines.all", "engines.1.supports_ssh_tunneling", "false"),
					resource.TestCheckResourceAttr("data.superset_database_engines.all", "engines.2.name", "Aurora PostgreSQL (Data API)"),
					resource.TestCheckResourceAttr("data.superset_database_engines.all", "engines.3.name", "Trino"),
					resource.TestCheckResourceAttr("data.superset_database_engines.postgres", "engines.#", "2"),
					resource.TestCheckResourceAttr("data.superset_database_engines.postgres", "engines.0.name", "PostgreSQL"),
					resource.TestCheckResourceAttr("data.superset_database_engines.postgres", "engines.1.default_driver", "auroradataapi"),
					resource.TestCheckResourceAttr("data.superset_database_engines.none", "engines.#", "0"),
				),
			},
		},
	})
}
