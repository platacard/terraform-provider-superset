package provider

import (
	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/jarcoal/httpmock"
	"testing"
)

func TestAccRolePermissionsDataSource(t *testing.T) {
	// Activate httpmock
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	// Mock the Superset API login response
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))

	// Mock the Superset API response for getting role ID by name
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/roles?q=(page_size:5000)",
		httpmock.NewStringResponder(200, `{"result": [{"id": 1, "name": "DWH-DB-Connect"}]}`))

	// Mock the Superset API response for getting role permissions
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/roles/1/permissions/",
		httpmock.NewStringResponder(200, `{
			"result": [
				{"id": 240, "permission_name": "database_access", "view_menu_name": "[Trino].(id:34)"},
				{"id": 241, "permission_name": "schema_access", "view_menu_name": "[Trino].[devoriginationzestorage]"}
			]
		}`))

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			// Read testing
			{
				Config: providerConfig + testAccRolePermissionsDataSourceConfig,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.superset_role_permissions.example", "role_name", "DWH-DB-Connect"),
					resource.TestCheckResourceAttr("data.superset_role_permissions.example", "permissions.#", "2"),
					resource.TestCheckResourceAttr("data.superset_role_permissions.example", "permissions.0.id", "240"),
					resource.TestCheckResourceAttr("data.superset_role_permissions.example", "permissions.0.permission_name", "database_access"),
					resource.TestCheckResourceAttr("data.superset_role_permissions.example", "permissions.0.view_menu_name", "[Trino].(id:34)"),
					resource.TestCheckResourceAttr("data.superset_role_permissions.example", "permissions.1.id", "241"),
					resource.TestCheckResourceAttr("data.superset_role_permissions.example", "permissions.1.permission_name", "schema_access"),
					resource.TestCheckResourceAttr("data.superset_role_permissions.example", "permissions.1.view_menu_name", "[Trino].[devoriginationzestorage]"),
				),
			},
		},
	})
}

const testAccRolePermissionsDataSourceConfig = `
data "superset_role_permissions" "example" {
  role_name = "DWH-DB-Connect"
}
`
