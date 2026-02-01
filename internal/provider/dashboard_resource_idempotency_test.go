package provider

import (
	"fmt"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
	"github.com/jarcoal/httpmock"
)

func TestAccDashboardResource_IdempotentCreateBySlug(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	// Mock authentication response
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token":"fake-token"}`))

	// CreateDashboard now checks for existing dashboards by slug.
	// Return an existing dashboard with the same slug so CreateDashboard updates it instead of POSTing.
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/dashboard/?q=(page_size:5000)",
		httpmock.NewStringResponder(200, `{"result":[{"id":333,"slug":"existing-dashboard"}]}`))

	// UpdateDashboard requires CSRF token
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/csrf_token/",
		httpmock.NewStringResponder(200, `{"result":"fake-csrf-token"}`))

	// Update existing dashboard
	httpmock.RegisterResponder("PUT", "http://superset-host/api/v1/dashboard/333",
		httpmock.NewStringResponder(200, `{"id":333,"result":{"dashboard_title":"Existing Dashboard"}}`))

	// Read back dashboard (resource Create reads after CreateDashboard returns)
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/dashboard/333",
		httpmock.NewStringResponder(200, `{"result":{"id":333,"dashboard_title":"Existing Dashboard","slug":"existing-dashboard","published":true}}`))

	httpmock.RegisterResponder("DELETE", "http://superset-host/api/v1/dashboard/333",
		httpmock.NewStringResponder(200, `{"message":"OK"}`))

	resource.Test(t, resource.TestCase{
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
resource "superset_dashboard" "test" {
  dashboard_title = "Existing Dashboard"
  slug            = "existing-dashboard"
  published       = true
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_dashboard.test", "dashboard_title", "Existing Dashboard"),
					resource.TestCheckResourceAttr("superset_dashboard.test", "slug", "existing-dashboard"),
					resource.TestCheckResourceAttr("superset_dashboard.test", "published", "true"),
					resource.TestCheckResourceAttrSet("superset_dashboard.test", "id"),
					func(_ *terraform.State) error {
						info := httpmock.GetCallCountInfo()
						if info["POST http://superset-host/api/v1/dashboard/"] != 0 {
							return fmt.Errorf("expected no POST dashboard create calls when slug already exists, got %d", info["POST http://superset-host/api/v1/dashboard/"])
						}
						if info["PUT http://superset-host/api/v1/dashboard/333"] < 1 {
							return fmt.Errorf("expected PUT update call for existing dashboard slug, got %d", info["PUT http://superset-host/api/v1/dashboard/333"])
						}
						return nil
					},
				),
			},
		},
	})
}

