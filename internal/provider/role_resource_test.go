package provider

import (
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/jarcoal/httpmock"
)

func TestAccRoleResource(t *testing.T) {
	// Activate httpmock
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	// Mock the Superset API login response
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))

	// Mock the Superset API response for checking if role exists (for GetRoleIDByName)
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/roles?q=(page_size:5000)",
		httpmock.NewStringResponder(200, `{"result": [{"id": 1, "name": "Antifraud"}]}`))

	// Mock the Superset API response for creating roles
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/roles/",
		httpmock.NewStringResponder(201, `{"id": 1, "name": "Antifraud"}`))

	// Mock the Superset API response for reading roles by ID
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/roles/1",
		httpmock.NewStringResponder(200, `{"result": {"id": 1, "name": "Antifraud"}}`))

	// Mock the Superset API response for deleting roles
	httpmock.RegisterResponder("DELETE", "http://superset-host/api/v1/security/roles/1",
		httpmock.NewStringResponder(204, ""))

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			// Create and Read testing
			{
				Config: providerConfig + testAccRoleResourceConfig,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_role.team_antifraud", "name", "Antifraud"),
					resource.TestCheckResourceAttrSet("superset_role.team_antifraud", "id"),
					resource.TestCheckResourceAttrSet("superset_role.team_antifraud", "last_updated"),
				),
			},
			// ImportState testing
			{
				ResourceName:            "superset_role.team_antifraud",
				ImportState:             true,
				ImportStateVerify:       true,
				ImportStateVerifyIgnore: []string{"last_updated"},
			},
		},
	})
}

const testAccRoleResourceConfig = `
resource "superset_role" "team_antifraud" {
  name = "Antifraud"
}
`
