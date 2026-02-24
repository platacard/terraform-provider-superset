package provider

import (
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/jarcoal/httpmock"
)

func TestAccGroupsDataSource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	// Mock login
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))

	// Mock fetch groups
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/groups/?q=(page_size:5000)",
		httpmock.NewStringResponder(200, `{
			"result": [
				{"id": 1, "name": "Group A"},
				{"id": 2, "name": "Group B"}
			]
		}`))

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + testAccGroupsDataSourceConfig,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.superset_groups.test", "groups.#", "2"),
					resource.TestCheckResourceAttr("data.superset_groups.test", "groups.0.id", "1"),
					resource.TestCheckResourceAttr("data.superset_groups.test", "groups.0.name", "Group A"),
					resource.TestCheckResourceAttr("data.superset_groups.test", "groups.1.id", "2"),
					resource.TestCheckResourceAttr("data.superset_groups.test", "groups.1.name", "Group B"),
				),
			},
		},
	})
}

const testAccGroupsDataSourceConfig = `
data "superset_groups" "test" {}
`
