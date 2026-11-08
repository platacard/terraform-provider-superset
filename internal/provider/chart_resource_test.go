package provider

import (
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"strconv"
	"sync"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
	"github.com/jarcoal/httpmock"
)

// fakeChart serves the chart endpoints of the fake Superset host for one chart with id 7. Like
// Superset 3.1 it leaves the datasource out of the single-chart response and only reports it in
// the list, and like recent versions it returns the cache timeout as a string. Params are
// returned re-encoded, so the resource must compare them as documents.
type fakeChart struct {
	mu      sync.Mutex
	chart   map[string]any
	deleted bool
}

func (f *fakeChart) serve() {
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/csrf_token/",
		httpmock.NewStringResponder(200, `{"result": "fake-csrf-token"}`))

	store := func(req *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(req.Body).Decode(&payload); err == nil {
			f.mu.Lock()
			f.chart, f.deleted = payload, false
			f.mu.Unlock()
		}
	}
	reencode := func(document any) any {
		text, _ := document.(string)
		if text == "" {
			return nil
		}
		var parsed any
		if err := json.Unmarshal([]byte(text), &parsed); err != nil {
			return text
		}
		encoded, _ := json.Marshal(parsed)
		return string(encoded)
	}
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/chart/",
		func(req *http.Request) (*http.Response, error) {
			store(req)
			return httpmock.NewStringResponse(201, `{"id": 7}`), nil
		})
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/chart/7",
		func(req *http.Request) (*http.Response, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.deleted || f.chart == nil {
				return httpmock.NewStringResponse(404, `{"message": "Not found"}`), nil
			}
			var timeout any
			if v, ok := f.chart["cache_timeout"].(float64); ok {
				timeout = strconv.FormatInt(int64(v), 10)
			}
			owners := []map[string]any{{"id": 1, "first_name": "Admin"}}
			if configured, ok := f.chart["owners"].([]any); ok {
				owners = nil
				for _, id := range configured {
					owners = append(owners, map[string]any{"id": id})
				}
			}
			result := map[string]any{
				"id":                    7,
				"slice_name":            f.chart["slice_name"],
				"viz_type":              f.chart["viz_type"],
				"params":                reencode(f.chart["params"]),
				"query_context":         reencode(f.chart["query_context"]),
				"description":           f.chart["description"],
				"cache_timeout":         timeout,
				"certified_by":          f.chart["certified_by"],
				"certification_details": f.chart["certification_details"],
				"owners":                owners,
			}
			return httpmock.NewJsonResponse(200, map[string]any{"id": 7, "result": result})
		})
	httpmock.RegisterResponder("GET", listEndpoint("/api/v1/chart/"),
		func(req *http.Request) (*http.Response, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.deleted || f.chart == nil {
				return httpmock.NewJsonResponse(200, map[string]any{"count": 0, "result": []any{}})
			}
			row := map[string]any{"id": 7, "datasource_id": f.chart["datasource_id"], "datasource_type": f.chart["datasource_type"]}
			return httpmock.NewJsonResponse(200, map[string]any{"count": 1, "result": []any{row}})
		})
	httpmock.RegisterResponder("PUT", "http://superset-host/api/v1/chart/7",
		func(req *http.Request) (*http.Response, error) {
			store(req)
			return httpmock.NewStringResponse(200, `{"id": 7}`), nil
		})
	httpmock.RegisterResponder("DELETE", "http://superset-host/api/v1/chart/7",
		func(req *http.Request) (*http.Response, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.deleted = true
			return httpmock.NewStringResponse(200, `{"message": "OK"}`), nil
		})
}

// sent reports whether the last create or update request carried the key with a non-null value.
func (f *fakeChart) sent(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.chart[key] != nil
}

func TestAccChartResource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeChart{}
	fake.serve()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
resource "superset_chart" "test" {
  name          = "Daily signups"
  datasource_id = 3
  viz_type      = "table"
  params = jsonencode({
    viz_type  = "table"
    metrics   = ["count"]
    row_limit = 100
  })
  description   = "Signups per day"
  cache_timeout = 60
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_chart.test", "id", "7"),
					resource.TestCheckResourceAttr("superset_chart.test", "name", "Daily signups"),
					resource.TestCheckResourceAttr("superset_chart.test", "datasource_id", "3"),
					resource.TestCheckResourceAttr("superset_chart.test", "datasource_type", "table"),
					resource.TestCheckResourceAttr("superset_chart.test", "description", "Signups per day"),
					resource.TestCheckResourceAttr("superset_chart.test", "cache_timeout", "60"),
					resource.TestCheckResourceAttr("superset_chart.test", "owners.#", "1"),
					resource.TestCheckResourceAttr("superset_chart.test", "owners.0", "1"),
					resource.TestCheckNoResourceAttr("superset_chart.test", "query_context"),
				),
			},
			{
				ResourceName:            "superset_chart.test",
				ImportState:             true,
				ImportStateVerify:       true,
				ImportStateVerifyIgnore: []string{"params"},
			},
			{
				Config: providerConfig + `
resource "superset_chart" "test" {
  name            = "Signups per day"
  datasource_id   = 3
  datasource_type = "table"
  viz_type        = "echarts_timeseries_line"
  params          = jsonencode({ viz_type = "echarts_timeseries_line", metrics = ["count"], x_axis = "day" })
  query_context   = jsonencode({ queries = [{ metrics = ["count"] }] })
  certified_by    = "QA"
  owners          = [1, 2]
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_chart.test", "name", "Signups per day"),
					resource.TestCheckResourceAttr("superset_chart.test", "viz_type", "echarts_timeseries_line"),
					resource.TestCheckResourceAttr("superset_chart.test", "certified_by", "QA"),
					resource.TestCheckResourceAttr("superset_chart.test", "owners.#", "2"),
					resource.TestCheckNoResourceAttr("superset_chart.test", "description"),
					resource.TestCheckNoResourceAttr("superset_chart.test", "cache_timeout"),
					func(*terraform.State) error {
						if fake.sent("description") || fake.sent("cache_timeout") {
							return errors.New("cleared attributes must be sent as null")
						}
						return nil
					},
				),
			},
			{
				// A query context Superset generated on its own is left alone when it is not configured.
				PreConfig: func() {
					fake.mu.Lock()
					delete(fake.chart, "query_context")
					fake.chart["query_context"] = `{"queries":[{"metrics":["count"]}],"form_data":{"slice_id":7}}`
					fake.mu.Unlock()
				},
				Config: providerConfig + `
resource "superset_chart" "test" {
  name            = "Signups per day"
  datasource_id   = 3
  datasource_type = "table"
  viz_type        = "echarts_timeseries_line"
  params          = jsonencode({ viz_type = "echarts_timeseries_line", metrics = ["count"], x_axis = "day" })
  certified_by    = "QA"
  owners          = [1, 2]
}
`,
				Check: resource.TestCheckResourceAttr("superset_chart.test", "query_context", `{"form_data":{"slice_id":7},"queries":[{"metrics":["count"]}]}`),
			},
			{
				PreConfig: func() {
					fake.mu.Lock()
					fake.deleted = true
					fake.mu.Unlock()
				},
				Config: providerConfig + `
resource "superset_chart" "test" {
  name            = "Signups per day"
  datasource_id   = 3
  datasource_type = "table"
  viz_type        = "echarts_timeseries_line"
  params          = jsonencode({ viz_type = "echarts_timeseries_line", metrics = ["count"], x_axis = "day" })
  query_context   = jsonencode({ queries = [{ metrics = ["count"] }] })
  certified_by    = "QA"
  owners          = [1, 2]
}
`,
				Check: resource.TestCheckResourceAttr("superset_chart.test", "id", "7"),
			},
		},
	})
}

func TestAccChartResourceInvalidParams(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeChart{}
	fake.serve()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
resource "superset_chart" "test" {
  name          = "Broken"
  datasource_id = 3
  viz_type      = "table"
  params        = "{not json"
}
`,
				ExpectError: regexp.MustCompile("Invalid JSON Document"),
			},
		},
	})
}
