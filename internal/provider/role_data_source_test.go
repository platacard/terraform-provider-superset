package provider

import (
	"net/http"
	"regexp"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/jarcoal/httpmock"
)

func TestAccRoleDataSource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))

	// The role list honours the name filter, so the test proves the lookup is exact.
	httpmock.RegisterResponder("GET", listEndpoint("/api/v1/security/roles/"),
		func(req *http.Request) (*http.Response, error) {
			if strings.Contains(req.URL.Query().Get("q"), "(col:name,opr:eq,value:'Gamma')") {
				return httpmock.NewStringResponse(200, `{"count": 1, "result": [{"id": 4, "name": "Gamma"}]}`), nil
			}
			return httpmock.NewStringResponse(200, `{"count": 0, "result": []}`), nil
		})

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
data "superset_role" "gamma" {
  name = "Gamma"
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.superset_role.gamma", "name", "Gamma"),
					resource.TestCheckResourceAttr("data.superset_role.gamma", "id", "4"),
				),
			},
			{
				Config: providerConfig + `
data "superset_role" "missing" {
  name = "gamma"
}
`,
				ExpectError: regexp.MustCompile(`Could not look up role "gamma": role gamma: not found`),
			},
			{
				Config: providerConfig + `
data "superset_role" "empty" {
  name = ""
}
`,
				ExpectError: regexp.MustCompile(`The role name must not be empty`),
			},
		},
	})
}
