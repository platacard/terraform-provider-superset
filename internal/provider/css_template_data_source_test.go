package provider

import (
	"net/http"
	"regexp"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/jarcoal/httpmock"
)

func TestAccCSSTemplateDataSource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))

	// The list honours the name filter, so the test proves the lookup is exact.
	httpmock.RegisterResponder("GET", listEndpoint("/api/v1/css_template/"),
		func(req *http.Request) (*http.Response, error) {
			if strings.Contains(req.URL.Query().Get("q"), "(col:template_name,opr:eq,value:'Dark header')") {
				return httpmock.NewStringResponse(200, `{"count": 1, "result": [{"id": 7, "template_name": "Dark header", "css": ".dashboard-header { color: #fff; }"}]}`), nil
			}
			return httpmock.NewStringResponse(200, `{"count": 0, "result": []}`), nil
		})

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
data "superset_css_template" "test" {
  template_name = "Dark header"
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.superset_css_template.test", "id", "7"),
					resource.TestCheckResourceAttr("data.superset_css_template.test", "css", ".dashboard-header { color: #fff; }"),
				),
			},
			{
				Config: providerConfig + `
data "superset_css_template" "missing" {
  template_name = "dark header"
}
`,
				ExpectError: regexp.MustCompile(`Could not look up CSS template "dark header": not found`),
			},
		},
	})
}
