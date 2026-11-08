package provider

import (
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/jarcoal/httpmock"
)

func TestAccUsersDataSource(t *testing.T) {
	// Activate httpmock
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	// Mock the Superset API login response
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))

	// Mock the Superset API response for fetching users
	httpmock.RegisterResponder("GET", listEndpoint("/api/v1/security/users/"),
		httpmock.NewStringResponder(200, `{
			"result": [
				{
					"id": 1,
					"username": "admin",
					"first_name": "Admin",
					"last_name": "User",
					"email": "admin@example.com",
					"active": true,
					"roles": [
						{"id": 1, "name": "Admin"}
					]
				},
				{
					"id": 2,
					"username": "test.user",
					"first_name": "Test",
					"last_name": "User",
					"email": "test.user@example.com",
					"active": true,
					"roles": [
						{"id": 4, "name": "Gamma"}
					]
				}
			]
		}`))

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			// Read testing
			{
				Config: providerConfig + testAccUsersDataSourceConfig,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.superset_users.test", "users.#", "2"),
					resource.TestCheckResourceAttr("data.superset_users.test", "users.0.id", "1"),
					resource.TestCheckResourceAttr("data.superset_users.test", "users.0.username", "admin"),
					resource.TestCheckResourceAttr("data.superset_users.test", "users.0.first_name", "Admin"),
					resource.TestCheckResourceAttr("data.superset_users.test", "users.0.last_name", "User"),
					resource.TestCheckResourceAttr("data.superset_users.test", "users.0.email", "admin@example.com"),
					resource.TestCheckResourceAttr("data.superset_users.test", "users.0.active", "true"),
					resource.TestCheckResourceAttr("data.superset_users.test", "users.0.roles.#", "1"),
					resource.TestCheckResourceAttr("data.superset_users.test", "users.0.roles.0.id", "1"),
					resource.TestCheckResourceAttr("data.superset_users.test", "users.0.roles.0.name", "Admin"),
					resource.TestCheckResourceAttr("data.superset_users.test", "users.1.id", "2"),
					resource.TestCheckResourceAttr("data.superset_users.test", "users.1.username", "test.user"),
					resource.TestCheckResourceAttr("data.superset_users.test", "users.1.first_name", "Test"),
					resource.TestCheckResourceAttr("data.superset_users.test", "users.1.last_name", "User"),
					resource.TestCheckResourceAttr("data.superset_users.test", "users.1.email", "test.user@example.com"),
					resource.TestCheckResourceAttr("data.superset_users.test", "users.1.active", "true"),
					resource.TestCheckResourceAttr("data.superset_users.test", "users.1.roles.#", "1"),
					resource.TestCheckResourceAttr("data.superset_users.test", "users.1.roles.0.id", "4"),
					resource.TestCheckResourceAttr("data.superset_users.test", "users.1.roles.0.name", "Gamma"),
				),
			},
		},
	})
}

const testAccUsersDataSourceConfig = `
data "superset_users" "test" {}
`
