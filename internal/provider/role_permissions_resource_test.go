package provider

import (
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/jarcoal/httpmock"
)

func TestAccRolePermissionsResource(t *testing.T) {

	t.Run("CreateRead", func(t *testing.T) {
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()

		// Mock the Superset API login response
		httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
			httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))

		// Mock the Superset API response for reading roles by ID
		httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/roles/129",
			httpmock.NewStringResponder(200, `{"result": {"id": 129, "name": "DWH-DB-Connect"}}`))

		// Mock the Superset API response for fetching roles
		httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/roles?q=(page_size:5000)",
			httpmock.NewStringResponder(200, `{
				"result": [
					{"id": 129, "name": "DWH-DB-Connect"}
				]
			}`))

		// Mock the Superset API response for fetching permissions resources
		httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/permissions-resources?q=(page_size:5000)",
			httpmock.NewStringResponder(200, `{ "result": [
				{
					"id": 240,
					"permission": {
						"name": "database_access"
					},
					"view_menu": {
						"name": "[SelfPostgreSQL].(id:1)"
					}
				}
		]}`))

		// Mock the Superset API response for fetching a specific permission by name and view
		httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/permissions?q=(filters:[(permission_name:eq:database_access),(view_menu_name:eq:[SelfPostgreSQL].(id:1))])",
			httpmock.NewStringResponder(200, `{ "result": [
				{
					"id": 240
				}
		]}`))

		// Mock the Superset API response for updating role permissions
		httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/roles/129/permissions",
			httpmock.NewStringResponder(200, `{"status": "success"}`))

		// Mock the Superset API response for fetching role permissions
		httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/roles/129/permissions/",
			httpmock.NewStringResponder(200, `{ "result": [
				{
					"id": 240,
					"permission_name": "database_access",
					"view_menu_name": "[SelfPostgreSQL].(id:1)"
				}
		]}`))

		// Mock the Superset API response for deleting role permissions
		httpmock.RegisterResponder("DELETE", "http://superset-host/api/v1/security/roles/129/permissions",
			httpmock.NewStringResponder(204, ""))

		resource.Test(t, resource.TestCase{
			ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
			Steps: []resource.TestStep{
				// Create and Read testing
				{
					Config: providerConfig + `
	resource "superset_role_permissions" "team" {
	role_name            = "DWH-DB-Connect"
	resource_permissions = [
		{
		permission = "database_access"
		view_menu  = "[SelfPostgreSQL].(id:1)"
		}
	]
	}
	`,
					Check: resource.ComposeAggregateTestCheckFunc(
						resource.TestCheckResourceAttr("superset_role_permissions.team", "role_name", "DWH-DB-Connect"),
						resource.TestCheckResourceAttr("superset_role_permissions.team", "resource_permissions.#", "1"),
						resource.TestCheckResourceAttr("superset_role_permissions.team", "resource_permissions.0.permission", "database_access"),
						resource.TestCheckResourceAttr("superset_role_permissions.team", "resource_permissions.0.view_menu", "[SelfPostgreSQL].(id:1)"),
					),
				},
				// ImportState testing
				{
					ResourceName:            "superset_role_permissions.team",
					ImportState:             true,
					ImportStateVerify:       true,
					ImportStateVerifyIgnore: []string{"last_updated"},
				},
			},
		})
	})

	t.Run("UpdateRead", func(t *testing.T) {
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()

		// Mock the Superset API login response
		httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
			httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))

		// Mock the Superset API response for fetching roles
		httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/roles?q=(page_size:5000)",
			httpmock.NewStringResponder(200, `{
				"result": [
					{"id": 129, "name": "DWH-DB-Connect"}
				]
			}`))

		// Mock the Superset API response for fetching permissions resources
		httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/permissions-resources?q=(page_size:5000)",
			httpmock.NewStringResponder(200, `{ "result": [
				{
					"id": 240,
					"permission": {
						"name": "database_access"
					},
					"view_menu": {
						"name": "[SelfPostgreSQL].(id:1)"
					}
				},
				{
					"id": 241,
					"permission": {
						"name": "schema_access"
					},
					"view_menu": {
						"name": "[Trino].[devoriginationzestorage]"
					}
				}
		]}`))

		// Mock the Superset API response for fetching a specific permission by name and view
		httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/permissions?q=(filters:[(permission_name:eq:database_access),(view_menu_name:eq:[SelfPostgreSQL].(id:1))])",
			httpmock.NewStringResponder(200, `{ "result": [
				{
					"id": 240
				}
		]}`))

		httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/permissions?q=(filters:[(permission_name:eq:schema_access),(view_menu_name:eq:[Trino].[devoriginationzestorage])])",
			httpmock.NewStringResponder(200, `{ "result": [
				{
					"id": 241
				}
		]}`))

		// Mock the Superset API response for updating role permissions
		httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/roles/129/permissions",
			httpmock.NewStringResponder(200, `{"status": "success"}`))

		// Mock the Superset API response for fetching role permissions
		httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/roles/129/permissions/",
			httpmock.NewStringResponder(200, `{ "result": [
				{
					"id": 240,
					"permission_name": "database_access",
					"view_menu_name": "[SelfPostgreSQL].(id:1)"
				},
				{
					"id": 241,
					"permission_name": "schema_access",
					"view_menu_name": "[Trino].[devoriginationzestorage]"
				}
		]}`))

		// Mock the Superset API response for deleting role permissions
		httpmock.RegisterResponder("DELETE", "http://superset-host/api/v1/security/roles/129/permissions",
			httpmock.NewStringResponder(204, ""))

		resource.Test(t, resource.TestCase{
			ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
			Steps: []resource.TestStep{
				// Update and Read testing
				{
					Config: providerConfig + `
	resource "superset_role_permissions" "team" {
	role_name            = "DWH-DB-Connect"
	resource_permissions = [
		{
			permission = "database_access"
			view_menu  = "[SelfPostgreSQL].(id:1)"
		},
		{
			permission = "schema_access"
			view_menu  = "[Trino].[devoriginationzestorage]"
		},
	]
	}
	`,
					Check: resource.ComposeAggregateTestCheckFunc(
						resource.TestCheckResourceAttr("superset_role_permissions.team", "role_name", "DWH-DB-Connect"),
						resource.TestCheckResourceAttr("superset_role_permissions.team", "resource_permissions.#", "2"),
						resource.TestCheckResourceAttr("superset_role_permissions.team", "resource_permissions.1.permission", "schema_access"),
						resource.TestCheckResourceAttr("superset_role_permissions.team", "resource_permissions.1.view_menu", "[Trino].[devoriginationzestorage]"),
					),
				},
			},
		})
	})
}
