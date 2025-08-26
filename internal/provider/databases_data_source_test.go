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
					resource.TestCheckResourceAttr("data.superset_databases.test", "databases.1.id", "1"),
					resource.TestCheckResourceAttr("data.superset_databases.test", "databases.1.database_name", "SelfPostgreSQL"),
				),
			},
		},
	})
}

const testAccDatabasesDataSourceConfig = `
data "superset_databases" "test" {}
`
