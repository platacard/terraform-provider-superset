package provider

import (
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/plancheck"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
	"github.com/jarcoal/httpmock"
)

// fakeDashboardEmbedding serves the embedding endpoints of the fake Superset host for any
// dashboard. Like Superset, enabling the embedding of a dashboard twice keeps its uuid, and the
// domains are reported in reverse order to prove that the resource treats them as a set.
type fakeDashboardEmbedding struct {
	mu      sync.Mutex
	domains map[int64][]string
	uuids   int
}

func (f *fakeDashboardEmbedding) serve() {
	f.domains = map[int64][]string{}
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/csrf_token/",
		httpmock.NewStringResponder(200, `{"result": "fake-csrf-token"}`))

	const endpoint = `=~^http://superset-host/api/v1/dashboard/(\d+)/embedded$`
	result := func(dashboard int64) map[string]any {
		domains := f.domains[dashboard]
		reversed := make([]string, 0, len(domains))
		for i := len(domains) - 1; i >= 0; i-- {
			reversed = append(reversed, domains[i])
		}
		return map[string]any{"result": map[string]any{
			"uuid": "uuid-" + string(rune('0'+f.uuids)), "dashboard_id": dashboard, "allowed_domains": reversed,
		}}
	}
	set := func(req *http.Request) (*http.Response, error) {
		dashboard := httpmock.MustGetSubmatchAsInt(req, 1)
		var payload struct {
			AllowedDomains []string `json:"allowed_domains"`
		}
		if err := json.NewDecoder(req.Body).Decode(&payload); err != nil || payload.AllowedDomains == nil {
			return httpmock.NewStringResponse(400, `{"message": {"allowed_domains": ["Not a valid list."]}}`), nil
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		if _, enabled := f.domains[dashboard]; !enabled {
			f.uuids++
		}
		f.domains[dashboard] = payload.AllowedDomains
		return httpmock.NewJsonResponse(200, result(dashboard))
	}
	httpmock.RegisterResponder("POST", endpoint, set)
	httpmock.RegisterResponder("PUT", endpoint, set)
	httpmock.RegisterResponder("GET", endpoint,
		func(req *http.Request) (*http.Response, error) {
			dashboard := httpmock.MustGetSubmatchAsInt(req, 1)
			f.mu.Lock()
			defer f.mu.Unlock()
			if _, enabled := f.domains[dashboard]; !enabled {
				return httpmock.NewStringResponse(404, `{}`), nil
			}
			return httpmock.NewJsonResponse(200, result(dashboard))
		})
	httpmock.RegisterResponder("DELETE", endpoint,
		func(req *http.Request) (*http.Response, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			delete(f.domains, httpmock.MustGetSubmatchAsInt(req, 1))
			return httpmock.NewStringResponse(200, `{"message": "OK"}`), nil
		})
}

// disable drops the embedding of a dashboard, as a user disabling it in the UI would.
func (f *fakeDashboardEmbedding) disable(dashboard int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.domains, dashboard)
}

// enabled reports whether the embedding of a dashboard is enabled.
func (f *fakeDashboardEmbedding) enabled(dashboard int64) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, enabled := f.domains[dashboard]
	return enabled
}

func TestAccDashboardEmbeddingResource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeDashboardEmbedding{}
	fake.serve()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
resource "superset_dashboard_embedding" "test" {
  dashboard_id    = 3
  allowed_domains = ["https://app.example.com", "https://portal.example.com"]
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_dashboard_embedding.test", "id", "3"),
					resource.TestCheckResourceAttr("superset_dashboard_embedding.test", "dashboard_id", "3"),
					resource.TestCheckResourceAttr("superset_dashboard_embedding.test", "uuid", "uuid-1"),
					resource.TestCheckResourceAttr("superset_dashboard_embedding.test", "allowed_domains.#", "2"),
					resource.TestCheckTypeSetElemAttr("superset_dashboard_embedding.test", "allowed_domains.*", "https://app.example.com"),
					resource.TestCheckTypeSetElemAttr("superset_dashboard_embedding.test", "allowed_domains.*", "https://portal.example.com"),
				),
			},
			{
				ResourceName:      "superset_dashboard_embedding.test",
				ImportState:       true,
				ImportStateVerify: true,
			},
			{
				// Updating the domains keeps the uuid.
				Config: providerConfig + `
resource "superset_dashboard_embedding" "test" {
  dashboard_id    = 3
  allowed_domains = []
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_dashboard_embedding.test", "uuid", "uuid-1"),
					resource.TestCheckResourceAttr("superset_dashboard_embedding.test", "allowed_domains.#", "0"),
				),
			},
			{
				// Embedding disabled outside Terraform is enabled again, under a new uuid.
				PreConfig: func() { fake.disable(3) },
				Config: providerConfig + `
resource "superset_dashboard_embedding" "test" {
  dashboard_id    = 3
  allowed_domains = []
}
`,
				Check: resource.TestCheckResourceAttr("superset_dashboard_embedding.test", "uuid", "uuid-2"),
			},
			{
				// Another dashboard means a replacement, which disables the embedding of the old one.
				Config: providerConfig + `
resource "superset_dashboard_embedding" "test" {
  dashboard_id    = 4
  allowed_domains = ["https://app.example.com"]
}
`,
				ConfigPlanChecks: resource.ConfigPlanChecks{
					PreApply: []plancheck.PlanCheck{
						plancheck.ExpectResourceAction("superset_dashboard_embedding.test", plancheck.ResourceActionReplace),
					},
				},
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_dashboard_embedding.test", "id", "4"),
					resource.TestCheckResourceAttr("superset_dashboard_embedding.test", "uuid", "uuid-3"),
					func(*terraform.State) error {
						if fake.enabled(3) {
							return errors.New("the embedding of the replaced dashboard must be disabled")
						}
						return nil
					},
				),
			},
		},
	})
}
