package provider

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/jarcoal/httpmock"
)

func TestAccChartResource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	// Mock authentication response
	mockLoginResponse := `{
		"access_token": "fake-token",
		"refresh_token": "fake-refresh-token"
	}`

	// Mock chart creation response
	mockChartCreateResponse := `{
		"id": 100,
		"result": {
			"slice_name": "Test Chart",
			"datasource_id": 1,
			"datasource_type": "table",
			"viz_type": "table"
		}
	}`

	// Mock chart read response - initial version
	mockChartReadResponseInitial := `{
		"result": {
			"id": 100,
			"slice_name": "Test Chart",
			"datasource_id": 1,
			"datasource_type": "table",
			"viz_type": "table",
			"description": "Test description",
			"params": null,
			"cache_timeout": null,
			"certified_by": null,
			"certification_details": null
		}
	}`

	// Mock chart read response - after update
	mockChartReadResponseUpdated := `{
		"result": {
			"id": 100,
			"slice_name": "Updated Chart",
			"datasource_id": 1,
			"datasource_type": "table",
			"viz_type": "big_number_total",
			"description": "Updated description",
			"params": null,
			"cache_timeout": 300,
			"certified_by": null,
			"certification_details": null
		}
	}`

	// Mock chart update response
	mockChartUpdateResponse := `{
		"id": 100,
		"result": {
			"slice_name": "Updated Chart"
		}
	}`

	// Setup mocks
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, mockLoginResponse))

	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/chart/",
		httpmock.NewStringResponder(201, mockChartCreateResponse))

	// Setup dynamic GET responder that returns different responses based on call count
	callCount := 0
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/chart/100",
		func(req *http.Request) (*http.Response, error) {
			callCount++
			if callCount <= 2 { // First two calls return initial values
				return httpmock.NewStringResponse(200, mockChartReadResponseInitial), nil
			}
			// Subsequent calls return updated values
			return httpmock.NewStringResponse(200, mockChartReadResponseUpdated), nil
		})

	httpmock.RegisterResponder("PUT", "http://superset-host/api/v1/chart/100",
		httpmock.NewStringResponder(200, mockChartUpdateResponse))

	httpmock.RegisterResponder("DELETE", "http://superset-host/api/v1/chart/100",
		httpmock.NewStringResponder(200, `{"message": "OK"}`))

	resource.Test(t, resource.TestCase{
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			// Create and Read testing
			{
				Config: providerConfig + testAccChartResourceConfig("Test Chart", 1, "table", "Test description"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_chart.test", "slice_name", "Test Chart"),
					resource.TestCheckResourceAttr("superset_chart.test", "datasource_id", "1"),
					resource.TestCheckResourceAttr("superset_chart.test", "datasource_type", "table"),
					resource.TestCheckResourceAttr("superset_chart.test", "viz_type", "table"),
					resource.TestCheckResourceAttr("superset_chart.test", "description", "Test description"),
					resource.TestCheckResourceAttrSet("superset_chart.test", "id"),
				),
			},
			// ImportState testing
			{
				ResourceName:      "superset_chart.test",
				ImportState:       true,
				ImportStateVerify: true,
				ImportStateId:     "100",
			},
			// Update and Read testing
			{
				Config: providerConfig + testAccChartResourceConfigWithCacheTimeout("Updated Chart", 1, "big_number_total", "Updated description", 300),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_chart.test", "slice_name", "Updated Chart"),
					resource.TestCheckResourceAttr("superset_chart.test", "viz_type", "big_number_total"),
					resource.TestCheckResourceAttr("superset_chart.test", "description", "Updated description"),
					resource.TestCheckResourceAttr("superset_chart.test", "cache_timeout", "300"),
				),
			},
			// Delete testing automatically occurs in TestCase
		},
	})
}

func TestAccChartResourceMinimal(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	// Mock authentication response
	mockLoginResponse := `{
		"access_token": "fake-token",
		"refresh_token": "fake-refresh-token"
	}`

	// Mock chart creation response
	mockChartCreateResponse := `{
		"id": 101
	}`

	// Mock chart read response
	mockChartReadResponse := `{
		"result": {
			"id": 101,
			"slice_name": "Minimal Chart",
			"datasource_id": 1,
			"datasource_type": "table",
			"viz_type": "table",
			"description": "",
			"params": null,
			"cache_timeout": null,
			"certified_by": null,
			"certification_details": null
		}
	}`

	// Setup mocks
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, mockLoginResponse))

	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/chart/",
		httpmock.NewStringResponder(201, mockChartCreateResponse))

	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/chart/101",
		httpmock.NewStringResponder(200, mockChartReadResponse))

	httpmock.RegisterResponder("DELETE", "http://superset-host/api/v1/chart/101",
		httpmock.NewStringResponder(200, `{"message": "OK"}`))

	resource.Test(t, resource.TestCase{
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			// Create with minimal config (only required fields)
			{
				Config: providerConfig + testAccChartResourceConfigMinimal("Minimal Chart", 1),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_chart.test", "slice_name", "Minimal Chart"),
					resource.TestCheckResourceAttr("superset_chart.test", "datasource_id", "1"),
					resource.TestCheckResourceAttr("superset_chart.test", "datasource_type", "table"),
					resource.TestCheckResourceAttr("superset_chart.test", "viz_type", "table"),
					resource.TestCheckResourceAttrSet("superset_chart.test", "id"),
				),
			},
		},
	})
}

func testAccChartResourceConfig(sliceName string, datasourceID int, vizType, description string) string {
	return fmt.Sprintf(`
resource "superset_chart" "test" {
  slice_name      = %[1]q
  datasource_id   = %[2]d
  datasource_type = "table"
  viz_type        = %[3]q
  description     = %[4]q
}
`, sliceName, datasourceID, vizType, description)
}

func testAccChartResourceConfigWithCacheTimeout(sliceName string, datasourceID int, vizType, description string, cacheTimeout int) string {
	return fmt.Sprintf(`
resource "superset_chart" "test" {
  slice_name      = %[1]q
  datasource_id   = %[2]d
  datasource_type = "table"
  viz_type        = %[3]q
  description     = %[4]q
  cache_timeout   = %[5]d
}
`, sliceName, datasourceID, vizType, description, cacheTimeout)
}

func testAccChartResourceConfigMinimal(sliceName string, datasourceID int) string {
	return fmt.Sprintf(`
resource "superset_chart" "test" {
  slice_name    = %[1]q
  datasource_id = %[2]d
}
`, sliceName, datasourceID)
}

