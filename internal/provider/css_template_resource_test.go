package provider

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/jarcoal/httpmock"
)

// fakeCSSTemplate serves the CSS template endpoints of the fake Superset host for one template
// with id 7, remembering the values written by the create and update requests.
type fakeCSSTemplate struct {
	mu      sync.Mutex
	name    string
	css     string
	deleted bool
}

func (f *fakeCSSTemplate) serve() {
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/csrf_token/",
		httpmock.NewStringResponder(200, `{"result": "fake-csrf-token"}`))

	store := func(req *http.Request) {
		var payload struct {
			TemplateName string `json:"template_name"`
			CSS          string `json:"css"`
		}
		if err := json.NewDecoder(req.Body).Decode(&payload); err == nil {
			f.mu.Lock()
			f.name, f.css, f.deleted = payload.TemplateName, payload.CSS, false
			f.mu.Unlock()
		}
	}
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/css_template/",
		func(req *http.Request) (*http.Response, error) {
			store(req)
			return httpmock.NewStringResponse(201, `{"id": 7}`), nil
		})
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/css_template/7",
		func(req *http.Request) (*http.Response, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.deleted {
				return httpmock.NewStringResponse(404, `{"message": "Not found"}`), nil
			}
			return httpmock.NewJsonResponse(200, map[string]any{"id": 7, "result": map[string]any{"template_name": f.name, "css": f.css}})
		})
	httpmock.RegisterResponder("PUT", "http://superset-host/api/v1/css_template/7",
		func(req *http.Request) (*http.Response, error) {
			store(req)
			return httpmock.NewStringResponse(200, `{"id": 7}`), nil
		})
	httpmock.RegisterResponder("DELETE", "http://superset-host/api/v1/css_template/7",
		func(req *http.Request) (*http.Response, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.deleted {
				return httpmock.NewStringResponse(404, `{"message": "Not found"}`), nil
			}
			f.deleted = true
			return httpmock.NewStringResponse(200, `{"message": "OK"}`), nil
		})
}

func cssTemplateConfig(name, css string) string {
	return providerConfig + fmt.Sprintf(`
resource "superset_css_template" "test" {
  template_name = %q
  css           = %q
}
`, name, css)
}

func TestAccCSSTemplateResource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeCSSTemplate{}
	fake.serve()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: cssTemplateConfig("Dark header", ".dashboard-header { color: #fff; }"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_css_template.test", "id", "7"),
					resource.TestCheckResourceAttr("superset_css_template.test", "template_name", "Dark header"),
					resource.TestCheckResourceAttr("superset_css_template.test", "css", ".dashboard-header { color: #fff; }"),
				),
			},
			{
				ResourceName:      "superset_css_template.test",
				ImportState:       true,
				ImportStateVerify: true,
			},
			{
				Config: cssTemplateConfig("Darker header", ".dashboard-header { color: #eee; }"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_css_template.test", "id", "7"),
					resource.TestCheckResourceAttr("superset_css_template.test", "template_name", "Darker header"),
					resource.TestCheckResourceAttr("superset_css_template.test", "css", ".dashboard-header { color: #eee; }"),
				),
			},
		},
	})
}

func TestAccCSSTemplateResourceRecreatedWhenDeletedOutsideTerraform(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeCSSTemplate{}
	fake.serve()
	config := cssTemplateConfig("Dark header", ".dashboard-header { color: #fff; }")

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: config,
			},
			{
				PreConfig: func() {
					fake.mu.Lock()
					fake.deleted = true
					fake.mu.Unlock()
				},
				Config:             config,
				PlanOnly:           true,
				ExpectNonEmptyPlan: true,
			},
		},
	})
}
