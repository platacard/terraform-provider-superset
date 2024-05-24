package provider

import (
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/jarcoal/httpmock"
)

func TestAccRolesDataSource(t *testing.T) {
	// Activate httpmock
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	// Mock the Superset API login response
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))

	// Mock the Superset API response for fetching roles
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/roles?q=(page_size:5000)",
		httpmock.NewStringResponder(200, `{
			"result": [
				{"id": 1, "name": "Admin"},
				{"id": 2, "name": "Public"},
				{"id": 3, "name": "Alpha"},
				{"id": 4, "name": "Gamma"},
				{"id": 5, "name": "sql_lab"},
				{"id": 38, "name": "Trino_Table-Role"},
				{"id": 71, "name": "Custom-DWH"},
				{"id": 73, "name": "Role for DWH"},
				{"id": 555, "name": "Toronto-Team-Role"},
				{"id": 129, "name": "DWH-DB-Connect"}
			]
		}`))

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			// Read testing
			{
				Config: providerConfig + testAccRolesDataSourceConfig,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.superset_roles.test", "roles.#", "10"), // Adjust the expected number of roles
					resource.TestCheckResourceAttr("data.superset_roles.test", "roles.0.id", "1"),
					resource.TestCheckResourceAttr("data.superset_roles.test", "roles.0.name", "Admin"),
					resource.TestCheckResourceAttr("data.superset_roles.test", "roles.1.id", "2"),
					resource.TestCheckResourceAttr("data.superset_roles.test", "roles.1.name", "Public"),
					resource.TestCheckResourceAttr("data.superset_roles.test", "roles.2.id", "3"),
					resource.TestCheckResourceAttr("data.superset_roles.test", "roles.2.name", "Alpha"),
					resource.TestCheckResourceAttr("data.superset_roles.test", "roles.3.id", "4"),
					resource.TestCheckResourceAttr("data.superset_roles.test", "roles.3.name", "Gamma"),
					resource.TestCheckResourceAttr("data.superset_roles.test", "roles.4.id", "5"),
					resource.TestCheckResourceAttr("data.superset_roles.test", "roles.4.name", "sql_lab"),
					resource.TestCheckResourceAttr("data.superset_roles.test", "roles.5.id", "38"),
					resource.TestCheckResourceAttr("data.superset_roles.test", "roles.5.name", "Trino_Table-Role"),
					resource.TestCheckResourceAttr("data.superset_roles.test", "roles.6.id", "71"),
					resource.TestCheckResourceAttr("data.superset_roles.test", "roles.6.name", "Custom-DWH"),
					resource.TestCheckResourceAttr("data.superset_roles.test", "roles.7.id", "73"),
					resource.TestCheckResourceAttr("data.superset_roles.test", "roles.7.name", "Role for DWH"),
					resource.TestCheckResourceAttr("data.superset_roles.test", "roles.8.id", "555"),
					resource.TestCheckResourceAttr("data.superset_roles.test", "roles.8.name", "Toronto-Team-Role"),
					resource.TestCheckResourceAttr("data.superset_roles.test", "roles.9.id", "129"),
					resource.TestCheckResourceAttr("data.superset_roles.test", "roles.9.name", "DWH-DB-Connect"),
				),
			},
		},
	})
}

const testAccRolesDataSourceConfig = `
data "superset_roles" "test" {}
`
