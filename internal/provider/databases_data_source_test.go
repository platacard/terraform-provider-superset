package provider

import (
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/jarcoal/httpmock"
)

func TestAccDatabasesDataSource(t *testing.T) {
	// Activate httpmock
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	// Mock the Superset API login response
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))

	// Mock the Superset API response for fetching databases
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/database/",
		httpmock.NewStringResponder(200, `{
			"result": [
				{"id": 34, "database_name": "Trino"},
				{"id": 1, "database_name": "SelfPostgreSQL"},
				{"id": 141, "database_name": "DWH_database_connection3"},
				{"id": 140, "database_name": "DWH_database_connection2"},
				{"id": 139, "database_name": "DWH_database_connection"},
				{"id": 174, "database_name": "DWH_database_connection4"}
			]
		}`))

	// Mock the Superset API response for fetching database connections
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/database/34/connection",
		httpmock.NewStringResponder(200, `{
			"result": {
				"sqlalchemy_uri": "trino://dev:XXXXXXXXXX@mongo.database.domain:443/mongo",
				"database_name": "Trino"
			}
		}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/database/1/connection",
		httpmock.NewStringResponder(200, `{
			"result": {
				"sqlalchemy_uri": "postgresql+psycopg2://d_cloud_superset_user:XXXXXXXXXX@database.domain:5432/d_cloud_superset_db",
				"database_name": "SelfPostgreSQL"
			}
		}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/database/141/connection",
		httpmock.NewStringResponder(200, `{
			"result": {
				"sqlalchemy_uri": "postgresql://d_cloud_superset_user:XXXXXXXXXX@database.domain:5432/d_cloud_superset_db",
				"database_name": "DWH_database_connection3"
			}
		}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/database/140/connection",
		httpmock.NewStringResponder(200, `{
			"result": {
				"sqlalchemy_uri": "postgresql://d_cloud_superset_user:XXXXXXXXXX@database.domain:5432/d_cloud_superset_db",
				"database_name": "DWH_database_connection2"
			}
		}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/database/139/connection",
		httpmock.NewStringResponder(200, `{
			"result": {
				"sqlalchemy_uri": "postgresql://d_cloud_superset_user:XXXXXXXXXX@database.domain:5432/d_cloud_superset_db",
				"database_name": "DWH_database_connection"
			}
		}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/database/174/connection",
		httpmock.NewStringResponder(200, `{
			"result": {
				"sqlalchemy_uri": "postgresql://d_cloud_superset_user:XXXXXXXXXX@database.domain:5432/d_cloud_superset_db",
				"database_name": "DWH_database_connection4"
			}
		}`))

	// Mock the Superset API response for fetching database schemas
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/database/34/schemas/",
		httpmock.NewStringResponder(200, `{
			"result": ["devoriginationzestorage", "devpagoarcuspay", "devpagoreference", "devplatformidentitymanager", "devposapploans"]
		}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/database/1/schemas/",
		httpmock.NewStringResponder(200, `{
			"result": ["information_schema", "public"]
		}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/database/141/schemas/",
		httpmock.NewStringResponder(200, `{
			"result": ["information_schema", "public"]
		}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/database/140/schemas/",
		httpmock.NewStringResponder(200, `{
			"result": ["information_schema", "public"]
		}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/database/139/schemas/",
		httpmock.NewStringResponder(200, `{
			"result": ["information_schema", "public"]
		}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/database/174/schemas/",
		httpmock.NewStringResponder(200, `{
			"result": ["information_schema", "public"]
		}`))

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			// Read testing
			{
				Config: providerConfig + testAccDatabasesDataSourceConfig,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.superset_databases.test", "databases.#", "6"),
					resource.TestCheckResourceAttr("data.superset_databases.test", "databases.0.id", "34"),
					resource.TestCheckResourceAttr("data.superset_databases.test", "databases.0.database_name", "Trino"),
					resource.TestCheckResourceAttr("data.superset_databases.test", "databases.0.sqlalchemy_uri", "trino://dev:XXXXXXXXXX@mongo.database.domain:443/mongo"),
					resource.TestCheckResourceAttr("data.superset_databases.test", "databases.0.schemas.#", "5"),
					resource.TestCheckResourceAttr("data.superset_databases.test", "databases.1.id", "1"),
					resource.TestCheckResourceAttr("data.superset_databases.test", "databases.1.database_name", "SelfPostgreSQL"),
					resource.TestCheckResourceAttr("data.superset_databases.test", "databases.1.sqlalchemy_uri", "postgresql+psycopg2://d_cloud_superset_user:XXXXXXXXXX@database.domain:5432/d_cloud_superset_db"),
					resource.TestCheckResourceAttr("data.superset_databases.test", "databases.1.schemas.#", "2"),
				),
			},
		},
	})
}

const testAccDatabasesDataSourceConfig = `
data "superset_databases" "test" {}
`
