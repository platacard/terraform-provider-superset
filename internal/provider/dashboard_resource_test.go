package provider

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/jarcoal/httpmock"
)

func TestAccDashboardResource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	// Mock authentication response
	mockLoginResponse := `{
		"access_token": "fake-token",
		"refresh_token": "fake-refresh-token"
	}`

	// Mock dashboard creation response
	mockDashboardCreateResponse := `{
		"id": 200,
		"result": {
			"dashboard_title": "Test Dashboard",
			"slug": "test-dashboard",
			"published": false
		}
	}`

	// Mock dashboard read response - initial version
	mockDashboardReadResponseInitial := `{
		"result": {
			"id": 200,
			"dashboard_title": "Test Dashboard",
			"slug": "test-dashboard",
			"published": false,
			"json_metadata": null,
			"position_json": null,
			"css": null,
			"certified_by": null,
			"certification_details": null
		}
	}`

	// Mock dashboard read response - after update
	mockDashboardReadResponseUpdated := `{
		"result": {
			"id": 200,
			"dashboard_title": "Updated Dashboard",
			"slug": "updated-dashboard",
			"published": true,
			"json_metadata": null,
			"position_json": null,
			"css": ".dashboard { color: red; }",
			"certified_by": null,
			"certification_details": null
		}
	}`

	// Mock dashboard update response
	mockDashboardUpdateResponse := `{
		"id": 200,
		"result": {
			"dashboard_title": "Updated Dashboard"
		}
	}`

	// Setup mocks
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, mockLoginResponse))

	// CreateDashboard now checks for existing dashboards by slug (idempotency).
	// For this test, return an empty list so the provider proceeds with POST create.
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/dashboard/?q=(page_size:5000)",
		httpmock.NewStringResponder(200, `{"result":[]}`))

	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/dashboard/",
		httpmock.NewStringResponder(201, mockDashboardCreateResponse))

	// Setup dynamic GET responder that returns different responses based on call count
	callCount := 0
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/dashboard/200",
		func(req *http.Request) (*http.Response, error) {
			callCount++
			if callCount <= 3 { // First three calls return initial values (create + read + import)
				return httpmock.NewStringResponse(200, mockDashboardReadResponseInitial), nil
			}
			// Subsequent calls return updated values
			return httpmock.NewStringResponse(200, mockDashboardReadResponseUpdated), nil
		})

	httpmock.RegisterResponder("PUT", "http://superset-host/api/v1/dashboard/200",
		httpmock.NewStringResponder(200, mockDashboardUpdateResponse))

	httpmock.RegisterResponder("DELETE", "http://superset-host/api/v1/dashboard/200",
		httpmock.NewStringResponder(200, `{"message": "OK"}`))

	resource.Test(t, resource.TestCase{
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			// Create and Read testing
			{
				Config: providerConfig + testAccDashboardResourceConfig("Test Dashboard", "test-dashboard", false),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_dashboard.test", "dashboard_title", "Test Dashboard"),
					resource.TestCheckResourceAttr("superset_dashboard.test", "slug", "test-dashboard"),
					resource.TestCheckResourceAttr("superset_dashboard.test", "published", "false"),
					resource.TestCheckResourceAttrSet("superset_dashboard.test", "id"),
				),
			},
			// ImportState testing
			{
				ResourceName:      "superset_dashboard.test",
				ImportState:       true,
				ImportStateVerify: true,
				ImportStateId:     "200",
			},
			// Update and Read testing
			{
				Config: providerConfig + testAccDashboardResourceConfigWithCSS("Updated Dashboard", "updated-dashboard", true, ".dashboard { color: red; }"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_dashboard.test", "dashboard_title", "Updated Dashboard"),
					resource.TestCheckResourceAttr("superset_dashboard.test", "slug", "updated-dashboard"),
					resource.TestCheckResourceAttr("superset_dashboard.test", "published", "true"),
					resource.TestCheckResourceAttr("superset_dashboard.test", "css", ".dashboard { color: red; }"),
				),
			},
			// Delete testing automatically occurs in TestCase
		},
	})
}

func TestAccDashboardResourceMinimal(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	// Mock authentication response
	mockLoginResponse := `{
		"access_token": "fake-token",
		"refresh_token": "fake-refresh-token"
	}`

	// Mock dashboard creation response
	mockDashboardCreateResponse := `{
		"id": 201
	}`

	// Mock dashboard read response
	mockDashboardReadResponse := `{
		"result": {
			"id": 201,
			"dashboard_title": "Minimal Dashboard",
			"slug": "",
			"published": false,
			"json_metadata": null,
			"position_json": null,
			"css": null,
			"certified_by": null,
			"certification_details": null
		}
	}`

	// Setup mocks
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, mockLoginResponse))

	// Minimal config has no slug, so CreateDashboard won't query dashboards list.
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/dashboard/",
		httpmock.NewStringResponder(201, mockDashboardCreateResponse))

	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/dashboard/201",
		httpmock.NewStringResponder(200, mockDashboardReadResponse))

	httpmock.RegisterResponder("DELETE", "http://superset-host/api/v1/dashboard/201",
		httpmock.NewStringResponder(200, `{"message": "OK"}`))

	resource.Test(t, resource.TestCase{
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			// Create with minimal config (only required fields)
			{
				Config: providerConfig + testAccDashboardResourceConfigMinimal("Minimal Dashboard"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_dashboard.test", "dashboard_title", "Minimal Dashboard"),
					resource.TestCheckResourceAttr("superset_dashboard.test", "published", "false"),
					resource.TestCheckResourceAttrSet("superset_dashboard.test", "id"),
				),
			},
		},
	})
}

func TestAccDashboardResourceWithMetadata(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	// Mock authentication response
	mockLoginResponse := `{
		"access_token": "fake-token",
		"refresh_token": "fake-refresh-token"
	}`

	// Mock dashboard creation response
	mockDashboardCreateResponse := `{
		"id": 202
	}`

	jsonMetadata := `{"refresh_frequency": 60}`

	// Mock dashboard read response
	mockDashboardReadResponse := fmt.Sprintf(`{
		"result": {
			"id": 202,
			"dashboard_title": "Dashboard with Metadata",
			"slug": "meta-dashboard",
			"published": true,
			"json_metadata": %q,
			"position_json": null,
			"css": null,
			"certified_by": "Admin",
			"certification_details": "Certified for production"
		}
	}`, jsonMetadata)

	// Setup mocks
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, mockLoginResponse))

	// CreateDashboard now checks for existing dashboards by slug (idempotency).
	// For this test, return an empty list so the provider proceeds with POST create.
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/dashboard/?q=(page_size:5000)",
		httpmock.NewStringResponder(200, `{"result":[]}`))

	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/dashboard/",
		httpmock.NewStringResponder(201, mockDashboardCreateResponse))

	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/dashboard/202",
		httpmock.NewStringResponder(200, mockDashboardReadResponse))

	httpmock.RegisterResponder("DELETE", "http://superset-host/api/v1/dashboard/202",
		httpmock.NewStringResponder(200, `{"message": "OK"}`))

	resource.Test(t, resource.TestCase{
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			// Create with metadata and certification
			{
				Config: providerConfig + testAccDashboardResourceConfigFull("Dashboard with Metadata", "meta-dashboard", true, jsonMetadata, "Admin", "Certified for production"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_dashboard.test", "dashboard_title", "Dashboard with Metadata"),
					resource.TestCheckResourceAttr("superset_dashboard.test", "slug", "meta-dashboard"),
					resource.TestCheckResourceAttr("superset_dashboard.test", "published", "true"),
					resource.TestCheckResourceAttr("superset_dashboard.test", "json_metadata", jsonMetadata),
					resource.TestCheckResourceAttr("superset_dashboard.test", "certified_by", "Admin"),
					resource.TestCheckResourceAttr("superset_dashboard.test", "certification_details", "Certified for production"),
					resource.TestCheckResourceAttrSet("superset_dashboard.test", "id"),
				),
			},
		},
	})
}

func testAccDashboardResourceConfig(title, slug string, published bool) string {
	return fmt.Sprintf(`
resource "superset_dashboard" "test" {
  dashboard_title = %[1]q
  slug            = %[2]q
  published       = %[3]t
}
`, title, slug, published)
}

func testAccDashboardResourceConfigWithCSS(title, slug string, published bool, css string) string {
	return fmt.Sprintf(`
resource "superset_dashboard" "test" {
  dashboard_title = %[1]q
  slug            = %[2]q
  published       = %[3]t
  css             = %[4]q
}
`, title, slug, published, css)
}

func testAccDashboardResourceConfigMinimal(title string) string {
	return fmt.Sprintf(`
resource "superset_dashboard" "test" {
  dashboard_title = %[1]q
}
`, title)
}

func testAccDashboardResourceConfigFull(title, slug string, published bool, jsonMetadata, certifiedBy, certDetails string) string {
	return fmt.Sprintf(`
resource "superset_dashboard" "test" {
  dashboard_title       = %[1]q
  slug                  = %[2]q
  published             = %[3]t
  json_metadata         = %[4]q
  certified_by          = %[5]q
  certification_details = %[6]q
}
`, title, slug, published, jsonMetadata, certifiedBy, certDetails)
}

