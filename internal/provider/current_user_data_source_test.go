package provider

import (
	"regexp"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/jarcoal/httpmock"
)

func TestAccCurrentUserDataSource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/me/",
		httpmock.NewStringResponder(200, `{"result": {"email": "admin@example.com", "first_name": "Admin", "id": 1, "is_active": true, "is_anonymous": false, "last_name": "User", "login_count": 241, "username": "admin"}}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/me/roles/",
		httpmock.NewStringResponder(200, `{"result": {"email": "admin@example.com", "firstName": "Admin", "isActive": true, "lastName": "User", "permissions": {}, "roles": {"Gamma": [["can_read", "Chart"]], "Admin": [["can_write", "Chart"]]}, "username": "admin"}}`))

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
data "superset_current_user" "me" {}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.superset_current_user.me", "id", "1"),
					resource.TestCheckResourceAttr("data.superset_current_user.me", "username", "admin"),
					resource.TestCheckResourceAttr("data.superset_current_user.me", "first_name", "Admin"),
					resource.TestCheckResourceAttr("data.superset_current_user.me", "last_name", "User"),
					resource.TestCheckResourceAttr("data.superset_current_user.me", "email", "admin@example.com"),
					resource.TestCheckResourceAttr("data.superset_current_user.me", "is_active", "true"),
					resource.TestCheckResourceAttr("data.superset_current_user.me", "roles.#", "2"),
					resource.TestCheckResourceAttr("data.superset_current_user.me", "roles.0", "Admin"),
					resource.TestCheckResourceAttr("data.superset_current_user.me", "roles.1", "Gamma"),
				),
			},
		},
	})
}

func TestAccCurrentUserDataSourceOnLegacySuperset(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/me/",
		httpmock.NewStringResponder(401, `{"message": "Not authorized"}`))

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
data "superset_current_user" "me" {}
`,
				ExpectError: regexp.MustCompile(`Superset 6 only`),
			},
		},
	})
}
