package provider

import (
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"sync"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
	"github.com/jarcoal/httpmock"
)

// fakeDashboard serves the dashboard endpoints of the fake Superset host for one dashboard with
// id 3. Like Superset, a create stores the documents verbatim without linking any chart, while an
// update links the charts found in the positions of the metadata, re-encodes the layout with the
// uuid of each chart, and completes the metadata with defaults.
type fakeDashboard struct {
	mu        sync.Mutex
	dashboard map[string]any
	charts    []float64
	deleted   bool
}

func (f *fakeDashboard) serve() {
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/csrf_token/",
		httpmock.NewStringResponder(200, `{"result": "fake-csrf-token"}`))

	decode := func(req *http.Request) map[string]any {
		var payload map[string]any
		_ = json.NewDecoder(req.Body).Decode(&payload)
		return payload
	}
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/dashboard/",
		func(req *http.Request) (*http.Response, error) {
			payload := decode(req)
			// Unlike the update endpoint, the create endpoint rejects null documents.
			for _, key := range []string{"css", "json_metadata", "position_json"} {
				if value, present := payload[key]; present && value == nil {
					return httpmock.NewStringResponse(400, `{"message": {"`+key+`": ["Field may not be null."]}}`), nil
				}
			}
			f.mu.Lock()
			f.dashboard, f.charts, f.deleted = payload, nil, false
			f.mu.Unlock()
			return httpmock.NewStringResponse(201, `{"id": 3}`), nil
		})
	httpmock.RegisterResponder("PUT", "http://superset-host/api/v1/dashboard/3",
		func(req *http.Request) (*http.Response, error) {
			payload := decode(req)
			f.mu.Lock()
			defer f.mu.Unlock()
			f.dashboard = payload
			metadata := map[string]any{}
			if text, _ := payload["json_metadata"].(string); text != "" {
				if err := json.Unmarshal([]byte(text), &metadata); err != nil {
					return httpmock.NewStringResponse(400, `{"message": {"json_metadata": ["Not a valid JSON document"]}}`), nil
				}
			}
			if positions, ok := metadata["positions"].(map[string]any); ok {
				f.charts = nil
				for _, element := range positions {
					object, _ := element.(map[string]any)
					if meta, ok := object["meta"].(map[string]any); ok {
						if id, ok := meta["chartId"].(float64); ok {
							f.charts = append(f.charts, id)
							meta["uuid"] = "chart-uuid"
						}
					}
				}
				delete(metadata, "positions")
				layout, _ := json.Marshal(positions)
				f.dashboard["position_json"] = string(layout)
			}
			if _, ok := metadata["refresh_frequency"]; !ok {
				metadata["refresh_frequency"] = 0
			}
			metadata["expanded_slices"] = map[string]any{}
			metadata["label_colors"] = map[string]any{}
			completed, _ := json.Marshal(metadata)
			f.dashboard["json_metadata"] = string(completed)
			return httpmock.NewStringResponse(200, `{"id": 3}`), nil
		})
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/dashboard/3",
		func(req *http.Request) (*http.Response, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.deleted || f.dashboard == nil {
				return httpmock.NewStringResponse(404, `{"message": "Not found"}`), nil
			}
			owners := []map[string]any{{"id": 1, "first_name": "Admin"}}
			if configured, ok := f.dashboard["owners"].([]any); ok {
				owners = nil
				for _, id := range configured {
					owners = append(owners, map[string]any{"id": id})
				}
			}
			roles := []map[string]any{}
			if configured, ok := f.dashboard["roles"].([]any); ok {
				for _, id := range configured {
					roles = append(roles, map[string]any{"id": id, "name": "role"})
				}
			}
			result := map[string]any{
				"id":                    3,
				"dashboard_title":       f.dashboard["dashboard_title"],
				"slug":                  f.dashboard["slug"],
				"css":                   f.dashboard["css"],
				"json_metadata":         f.dashboard["json_metadata"],
				"position_json":         f.dashboard["position_json"],
				"published":             f.dashboard["published"],
				"certified_by":          f.dashboard["certified_by"],
				"certification_details": f.dashboard["certification_details"],
				"owners":                owners,
				"roles":                 roles,
			}
			return httpmock.NewJsonResponse(200, map[string]any{"id": 3, "result": result})
		})
	httpmock.RegisterResponder("DELETE", "http://superset-host/api/v1/dashboard/3",
		func(req *http.Request) (*http.Response, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.deleted = true
			return httpmock.NewStringResponse(200, `{"message": "OK"}`), nil
		})
}

// linked returns the ids of the charts linked to the dashboard by the last update.
func (f *fakeDashboard) linked() []float64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]float64(nil), f.charts...)
}

// metadata returns the stored metadata document.
func (f *fakeDashboard) metadata() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	text, _ := f.dashboard["json_metadata"].(string)
	return text
}

// setMetadata changes one key of the stored metadata, as a user editing the dashboard would.
func (f *fakeDashboard) setMetadata(key string, value any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	metadata := map[string]any{}
	text, _ := f.dashboard["json_metadata"].(string)
	_ = json.Unmarshal([]byte(text), &metadata)
	metadata[key] = value
	encoded, _ := json.Marshal(metadata)
	f.dashboard["json_metadata"] = string(encoded)
}

const dashboardLayout = `
  position_json = jsonencode({
    DASHBOARD_VERSION_KEY = "v2"
    ROOT_ID = { type = "ROOT", id = "ROOT_ID", children = ["GRID_ID"] }
    GRID_ID = { type = "GRID", id = "GRID_ID", parents = ["ROOT_ID"], children = ["ROW-1"] }
    "ROW-1" = { type = "ROW", id = "ROW-1", parents = ["ROOT_ID", "GRID_ID"], children = ["CHART-1"], meta = { background = "BACKGROUND_TRANSPARENT" } }
    "CHART-1" = { type = "CHART", id = "CHART-1", parents = ["ROOT_ID", "GRID_ID", "ROW-1"], children = [], meta = { chartId = 7, width = 4, height = 50 } }
  })
`

func TestAccDashboardResource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeDashboard{}
	fake.serve()

	fullConfig := providerConfig + `
resource "superset_dashboard" "test" {
  title         = "Signups"
  slug          = "signups"
  css           = ".dashboard { color: red; }"
  json_metadata = jsonencode({ refresh_frequency = 30, color_scheme = "supersetColors" })
  roles         = [4]
` + dashboardLayout + `
}
`
	updatedConfig := providerConfig + `
resource "superset_dashboard" "test" {
  title         = "Signups per day"
  json_metadata = jsonencode({ refresh_frequency = 60 })
  published     = true
  owners        = [1, 2]
` + dashboardLayout + `
}
`

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: fullConfig,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_dashboard.test", "id", "3"),
					resource.TestCheckResourceAttr("superset_dashboard.test", "title", "Signups"),
					resource.TestCheckResourceAttr("superset_dashboard.test", "slug", "signups"),
					resource.TestCheckResourceAttr("superset_dashboard.test", "published", "false"),
					resource.TestCheckResourceAttr("superset_dashboard.test", "owners.#", "1"),
					resource.TestCheckResourceAttr("superset_dashboard.test", "roles.#", "1"),
					resource.TestCheckResourceAttr("superset_dashboard.test", "roles.0", "4"),
					// The state keeps the configured documents although Superset completed them.
					resource.TestCheckResourceAttr("superset_dashboard.test", "json_metadata", `{"color_scheme":"supersetColors","refresh_frequency":30}`),
					resource.TestCheckResourceAttrWith("superset_dashboard.test", "position_json", func(value string) error {
						if regexp.MustCompile(`uuid`).MatchString(value) {
							return errors.New("the state must keep the configured layout, not the one completed by Superset")
						}
						if linked := fake.linked(); len(linked) != 1 || linked[0] != 7 {
							return errors.New("the chart of the layout must be linked to the dashboard")
						}
						return nil
					}),
				),
			},
			{
				ResourceName:            "superset_dashboard.test",
				ImportState:             true,
				ImportStateVerify:       true,
				ImportStateVerifyIgnore: []string{"json_metadata", "position_json"},
			},
			{
				// A key Superset added on its own is not drift.
				PreConfig: func() { fake.setMetadata("color_scheme_domain", []string{"#1FA8C9"}) },
				Config:    fullConfig,
				PlanOnly:  true,
			},
			{
				// A managed key changed outside Terraform is drift.
				PreConfig:          func() { fake.setMetadata("refresh_frequency", 90) },
				Config:             fullConfig,
				PlanOnly:           true,
				ExpectNonEmptyPlan: true,
			},
			{
				// A key set in the UI survives an update that does not mention it.
				PreConfig: func() { fake.setMetadata("native_filter_configuration", []any{"filter"}) },
				Config:    updatedConfig,
				Check: resource.ComposeAggregateTestCheckFunc(
					func(*terraform.State) error {
						if !regexp.MustCompile(`"native_filter_configuration":\["filter"\]`).MatchString(fake.metadata()) {
							return errors.New("a key the configuration does not set must survive the update")
						}
						return nil
					},
					resource.TestCheckResourceAttr("superset_dashboard.test", "title", "Signups per day"),
					resource.TestCheckResourceAttr("superset_dashboard.test", "published", "true"),
					resource.TestCheckResourceAttr("superset_dashboard.test", "owners.#", "2"),
					resource.TestCheckResourceAttr("superset_dashboard.test", "json_metadata", `{"refresh_frequency":60}`),
					resource.TestCheckNoResourceAttr("superset_dashboard.test", "slug"),
					resource.TestCheckNoResourceAttr("superset_dashboard.test", "css"),
					resource.TestCheckNoResourceAttr("superset_dashboard.test", "roles"),
				),
			},
			{
				PreConfig: func() {
					fake.mu.Lock()
					fake.deleted = true
					fake.mu.Unlock()
				},
				Config: updatedConfig,
				Check:  resource.TestCheckResourceAttr("superset_dashboard.test", "id", "3"),
			},
		},
	})
}

func TestAccDashboardResourceWithoutDocuments(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeDashboard{}
	fake.serve()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
resource "superset_dashboard" "test" {
  title = "Empty"
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_dashboard.test", "title", "Empty"),
					resource.TestCheckNoResourceAttr("superset_dashboard.test", "json_metadata"),
					resource.TestCheckNoResourceAttr("superset_dashboard.test", "position_json"),
					resource.TestCheckNoResourceAttr("superset_dashboard.test", "roles"),
				),
			},
			{
				// Documents written in the UI are left alone when they are not configured.
				PreConfig: func() {
					fake.mu.Lock()
					fake.dashboard["json_metadata"] = `{"refresh_frequency": 10}`
					fake.dashboard["position_json"] = `{"DASHBOARD_VERSION_KEY": "v2"}`
					fake.mu.Unlock()
				},
				Config: providerConfig + `
resource "superset_dashboard" "test" {
  title = "Empty"
}
`,
				PlanOnly: true,
			},
		},
	})
}
