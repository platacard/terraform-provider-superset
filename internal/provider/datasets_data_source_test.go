package provider

import (
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/jarcoal/httpmock"
)

func TestAccDatasetsDataSource(t *testing.T) {
	// Activate httpmock
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	// Mock the Superset API login response
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))

	// Mock the Superset API response for fetching datasets
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/dataset/",
		httpmock.NewStringResponder(200, `{
			"result": [
				{
					"id": 5,
					"table_name": "casbin_rule",
					"database": {"id": 152, "database_name": "[Cloud]-Backstage-Dev-RO[d_cloud_backstage_db]"},
					"schema": "public",
					"sql": "SELECT * FROM casbin_rule",
					"kind": "virtual",
					"owners": [{"id": 5, "first_name": "John", "last_name": "Doe"}]
				},
				{
					"id": 6,
					"table_name": "example_table",
					"database": {"id": 153, "database_name": "[Cloud]-Backstage-Dev-RO[d_cloud_example_db]"},
					"schema": "public",
					"sql": "SELECT * FROM example_table",
					"kind": "virtual",
					"owners": [{"id": 6, "first_name": "John", "last_name": "Doe"}]
				}
			]
		}`))

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			// Read testing
			{
				Config: providerConfig + testAccDatasetsDataSourceConfig,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.superset_datasets.test", "datasets.#", "2"),
					resource.TestCheckResourceAttr("data.superset_datasets.test", "datasets.0.id", "5"),
					resource.TestCheckResourceAttr("data.superset_datasets.test", "datasets.0.table_name", "casbin_rule"),
					resource.TestCheckResourceAttr("data.superset_datasets.test", "datasets.0.database_id", "152"),
					resource.TestCheckResourceAttr("data.superset_datasets.test", "datasets.0.database_name", "[Cloud]-Backstage-Dev-RO[d_cloud_backstage_db]"),
					resource.TestCheckResourceAttr("data.superset_datasets.test", "datasets.0.schema", "public"),
					resource.TestCheckResourceAttr("data.superset_datasets.test", "datasets.0.sql", "SELECT * FROM casbin_rule"),
					resource.TestCheckResourceAttr("data.superset_datasets.test", "datasets.0.kind", "virtual"),
					resource.TestCheckResourceAttr("data.superset_datasets.test", "datasets.0.owners.#", "1"),
					resource.TestCheckResourceAttr("data.superset_datasets.test", "datasets.0.owners.0.id", "5"),
					resource.TestCheckResourceAttr("data.superset_datasets.test", "datasets.0.owners.0.first_name", "John"),
					resource.TestCheckResourceAttr("data.superset_datasets.test", "datasets.0.owners.0.last_name", "Doe"),
					resource.TestCheckResourceAttr("data.superset_datasets.test", "datasets.1.id", "6"),
					resource.TestCheckResourceAttr("data.superset_datasets.test", "datasets.1.table_name", "example_table"),
					resource.TestCheckResourceAttr("data.superset_datasets.test", "datasets.1.database_id", "153"),
					resource.TestCheckResourceAttr("data.superset_datasets.test", "datasets.1.database_name", "[Cloud]-Backstage-Dev-RO[d_cloud_example_db]"),
					resource.TestCheckResourceAttr("data.superset_datasets.test", "datasets.1.schema", "public"),
					resource.TestCheckResourceAttr("data.superset_datasets.test", "datasets.1.sql", "SELECT * FROM example_table"),
					resource.TestCheckResourceAttr("data.superset_datasets.test", "datasets.1.kind", "virtual"),
					resource.TestCheckResourceAttr("data.superset_datasets.test", "datasets.1.owners.#", "1"),
					resource.TestCheckResourceAttr("data.superset_datasets.test", "datasets.1.owners.0.id", "6"),
					resource.TestCheckResourceAttr("data.superset_datasets.test", "datasets.1.owners.0.first_name", "John"),
					resource.TestCheckResourceAttr("data.superset_datasets.test", "datasets.1.owners.0.last_name", "Doe"),
				),
			},
		},
	})
}

const testAccDatasetsDataSourceConfig = `
data "superset_datasets" "test" {}
`
