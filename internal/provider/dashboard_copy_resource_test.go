package provider

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/plancheck"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
	"github.com/jarcoal/httpmock"
)

const copySourceLayout = `{"DASHBOARD_VERSION_KEY": "v2", "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]}, "CHART-1": {"type": "CHART", "id": "CHART-1", "meta": {"chartId": 20, "width": 4, "height": 50}}}`

// copyRecord is a copy the fake Superset made.
type copyRecord struct {
	title, css, metadata string
	duplicate, deleted   bool
}

// fakeDashboardCopy serves the source dashboard 30, the copy endpoint and the copies it made.
// A copy shows the chart of the source, 20, or its duplicate, 21.
type fakeDashboardCopy struct {
	mu       sync.Mutex
	nextID   int64
	copies   map[int64]*copyRecord
	copyBody map[string]any
	putBody  map[string]any
}

func (f *fakeDashboardCopy) serve() {
	f.nextID = 40
	f.copies = map[int64]*copyRecord{}
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/csrf_token/",
		httpmock.NewStringResponder(200, `{"result": "fake-csrf-token"}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/dashboard/30",
		httpmock.NewStringResponder(200, `{"id": 30, "result": {"id": 30, "dashboard_title": "Template", "slug": "template", "css": ".src {}", "json_metadata": "{\"color_scheme\": \"blue\", \"refresh_frequency\": 10}", "position_json": `+jsonString(copySourceLayout)+`, "published": true, "owners": [{"id": 1}], "roles": [], "tags": [], "uuid": "3f0b6a5e-0000-4000-8000-000000000030"}}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/dashboard/99",
		httpmock.NewStringResponder(404, `{"message": "Not found"}`))
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/dashboard/30/copy/", func(req *http.Request) (*http.Response, error) {
		body, _ := io.ReadAll(req.Body)
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			return httpmock.NewStringResponse(400, "bad json"), nil
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		f.copyBody = payload
		id := f.nextID
		f.nextID++
		// Superset keeps the metadata it was given and adds keys of its own.
		var metadata map[string]any
		_ = json.Unmarshal([]byte(text(payload, "json_metadata")), &metadata)
		metadata["default_filters"] = "{}"
		delete(metadata, "positions")
		encoded, _ := json.Marshal(metadata)
		duplicate, _ := payload["duplicate_slices"].(bool)
		f.copies[id] = &copyRecord{title: text(payload, "dashboard_title"), css: text(payload, "css"), metadata: string(encoded), duplicate: duplicate}
		return httpmock.NewStringResponse(200, fmt.Sprintf(`{"result": {"id": %d, "last_modified_time": 1794652013.0}}`, id)), nil
	})
	httpmock.RegisterResponder("GET", `=~^http://superset-host/api/v1/dashboard/(\d+)$`, func(req *http.Request) (*http.Response, error) {
		id := httpmock.MustGetSubmatchAsInt(req, 1)
		f.mu.Lock()
		defer f.mu.Unlock()
		record, ok := f.copies[id]
		if !ok || record.deleted {
			return httpmock.NewStringResponse(404, `{"message": "Not found"}`), nil
		}
		result := map[string]any{"id": id, "dashboard_title": record.title, "slug": nil, "css": record.css, "json_metadata": record.metadata, "position_json": copySourceLayout, "published": false, "owners": []any{map[string]any{"id": 1}}, "roles": []any{}, "tags": []any{}, "uuid": fmt.Sprintf("3f0b6a5e-0000-4000-8000-0000000000%d", id)}
		body, _ := json.Marshal(map[string]any{"id": id, "result": result})
		return httpmock.NewStringResponse(200, string(body)), nil
	})
	httpmock.RegisterResponder("GET", `=~^http://superset-host/api/v1/dashboard/(\d+)/charts$`, func(req *http.Request) (*http.Response, error) {
		id := httpmock.MustGetSubmatchAsInt(req, 1)
		f.mu.Lock()
		defer f.mu.Unlock()
		record, ok := f.copies[id]
		if !ok || record.deleted {
			return httpmock.NewStringResponse(404, `{"message": "Not found"}`), nil
		}
		chart := 20
		if record.duplicate {
			chart = 21
		}
		return httpmock.NewStringResponse(200, fmt.Sprintf(`{"result": [{"id": %d, "slice_name": "Sales by day"}]}`, chart)), nil
	})
	httpmock.RegisterResponder("PUT", `=~^http://superset-host/api/v1/dashboard/(\d+)$`, func(req *http.Request) (*http.Response, error) {
		id := httpmock.MustGetSubmatchAsInt(req, 1)
		body, _ := io.ReadAll(req.Body)
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			return httpmock.NewStringResponse(400, "bad json"), nil
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		record, ok := f.copies[id]
		if !ok || record.deleted {
			return httpmock.NewStringResponse(404, `{"message": "Not found"}`), nil
		}
		f.putBody = payload
		record.title = text(payload, "dashboard_title")
		record.css = text(payload, "css")
		record.metadata = text(payload, "json_metadata")
		return httpmock.NewStringResponse(200, `{"id": `+fmt.Sprint(id)+`, "result": {}}`), nil
	})
	httpmock.RegisterResponder("DELETE", `=~^http://superset-host/api/v1/dashboard/(\d+)$`, func(req *http.Request) (*http.Response, error) {
		id := httpmock.MustGetSubmatchAsInt(req, 1)
		f.mu.Lock()
		defer f.mu.Unlock()
		record, ok := f.copies[id]
		if !ok || record.deleted {
			return httpmock.NewStringResponse(404, `{"message": "Not found"}`), nil
		}
		record.deleted = true
		return httpmock.NewStringResponse(200, `{"message": "OK"}`), nil
	})
}

// jsonString quotes a JSON document as a JSON string.
func jsonString(document string) string {
	quoted, _ := json.Marshal(document)
	return string(quoted)
}

// sentMetadata returns the metadata document of the last copy request.
func (f *fakeDashboardCopy) sentMetadata() map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	var metadata map[string]any
	if f.copyBody != nil {
		_ = json.Unmarshal([]byte(text(f.copyBody, "json_metadata")), &metadata)
	}
	return metadata
}

// text returns the string under the key of a decoded request body.
func text(payload map[string]any, key string) string {
	value, _ := payload[key].(string)
	return value
}

func (f *fakeDashboardCopy) copySent(key string) any {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.copyBody[key]
}

func (f *fakeDashboardCopy) putSent(key string) any {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.putBody == nil {
		return nil
	}
	return f.putBody[key]
}

func dashboardCopyConfig(source int64, title, extra string) string {
	return providerConfig + fmt.Sprintf(`
resource "superset_dashboard_copy" "test" {
  source_dashboard_id = %d
  title               = %q
%s}
`, source, title, extra)
}

func TestAccDashboardCopyResource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	f := &fakeDashboardCopy{}
	f.serve()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: dashboardCopyConfig(30, "Sales EMEA", `  css           = ".copy {}"
  json_metadata = jsonencode({ refresh_frequency = 60 })
`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_dashboard_copy.test", "id", "40"),
					resource.TestCheckResourceAttr("superset_dashboard_copy.test", "title", "Sales EMEA"),
					resource.TestCheckResourceAttr("superset_dashboard_copy.test", "css", ".copy {}"),
					resource.TestCheckResourceAttr("superset_dashboard_copy.test", "duplicate_slices", "false"),
					resource.TestCheckResourceAttr("superset_dashboard_copy.test", "chart_ids.#", "1"),
					resource.TestCheckResourceAttr("superset_dashboard_copy.test", "chart_ids.0", "20"),
					resource.TestCheckResourceAttr("superset_dashboard_copy.test", "uuid", "3f0b6a5e-0000-4000-8000-000000000040"),
					func(*terraform.State) error {
						// The copy request carries the settings of the source under the configured
						// ones, and the layout of the source as positions, which links the charts.
						metadata := f.sentMetadata()
						positions, _ := metadata["positions"].(map[string]any)
						chart, _ := positions["CHART-1"].(map[string]any)
						meta, _ := chart["meta"].(map[string]any)
						if metadata["color_scheme"] != "blue" || metadata["refresh_frequency"] != float64(60) || meta["chartId"] != float64(20) {
							return fmt.Errorf("unexpected metadata of the copy: %v", metadata)
						}
						if f.copySent("css") != ".copy {}" || f.copySent("duplicate_slices") != false || f.copySent("dashboard_title") != "Sales EMEA" {
							return fmt.Errorf("unexpected copy request: %v", f.copyBody)
						}
						return nil
					},
				),
			},
			{
				// A new title and CSS update the copy in place; the settings keep their keys.
				Config: dashboardCopyConfig(30, "Sales EMEA (weekly)", `  css           = ".copy { color: red }"
  json_metadata = jsonencode({ refresh_frequency = 60 })
`),
				ConfigPlanChecks: resource.ConfigPlanChecks{
					PreApply: []plancheck.PlanCheck{plancheck.ExpectResourceAction("superset_dashboard_copy.test", plancheck.ResourceActionUpdate)},
				},
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_dashboard_copy.test", "id", "40"),
					resource.TestCheckResourceAttr("superset_dashboard_copy.test", "title", "Sales EMEA (weekly)"),
					resource.TestCheckResourceAttr("superset_dashboard_copy.test", "css", ".copy { color: red }"),
					func(*terraform.State) error {
						if f.putSent("dashboard_title") != "Sales EMEA (weekly)" || f.putSent("published") != false || f.putSent("slug") != nil {
							return fmt.Errorf("unexpected update request: %v", f.putBody)
						}
						metadata, _ := f.putSent("json_metadata").(string)
						if !strings.Contains(metadata, `"refresh_frequency":60`) || !strings.Contains(metadata, `"default_filters"`) {
							return fmt.Errorf("the update does not keep the settings of the copy: %s", metadata)
						}
						return nil
					},
				),
			},
			{
				// Duplicating the charts needs a new copy, which shows the duplicate.
				Config: dashboardCopyConfig(30, "Sales EMEA (weekly)", `  duplicate_slices = true
`),
				ConfigPlanChecks: resource.ConfigPlanChecks{
					PreApply: []plancheck.PlanCheck{plancheck.ExpectResourceAction("superset_dashboard_copy.test", plancheck.ResourceActionReplace)},
				},
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_dashboard_copy.test", "id", "41"),
					resource.TestCheckResourceAttr("superset_dashboard_copy.test", "duplicate_slices", "true"),
					resource.TestCheckResourceAttr("superset_dashboard_copy.test", "chart_ids.0", "21"),
					resource.TestCheckNoResourceAttr("superset_dashboard_copy.test", "css"),
					func(*terraform.State) error {
						if f.copySent("duplicate_slices") != true || f.copySent("css") != "" {
							return fmt.Errorf("unexpected copy request: %v", f.copyBody)
						}
						if record := f.copies[40]; !record.deleted {
							return fmt.Errorf("the first copy was not deleted")
						}
						return nil
					},
				),
			},
			{
				// A copy deleted outside Terraform is made again.
				PreConfig: func() {
					f.mu.Lock()
					f.copies[41].deleted = true
					f.mu.Unlock()
				},
				Config: dashboardCopyConfig(30, "Sales EMEA (weekly)", `  duplicate_slices = true
`),
				ConfigPlanChecks: resource.ConfigPlanChecks{
					PreApply: []plancheck.PlanCheck{plancheck.ExpectResourceAction("superset_dashboard_copy.test", plancheck.ResourceActionCreate)},
				},
				Check: resource.TestCheckResourceAttr("superset_dashboard_copy.test", "id", "42"),
			},
		},
	})
}

func TestAccDashboardCopyResourceMissingSource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	f := &fakeDashboardCopy{}
	f.serve()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config:      dashboardCopyConfig(99, "Nowhere", ""),
				ExpectError: regexp.MustCompile(`(?s)Could not copy dashboard 99: source dashboard: .*not\s+found`),
			},
		},
	})
}
