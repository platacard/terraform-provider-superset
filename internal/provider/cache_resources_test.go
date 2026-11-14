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

// fakeCache serves the warm-up and invalidation endpoints and records what they were given.
// Chart 20 computes, chart 21 has no query context and chart 99 does not exist.
type fakeCache struct {
	mu        sync.Mutex
	warmups   []string
	drops     []string
	chartGone bool
}

func (f *fakeCache) serve() {
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/csrf_token/",
		httpmock.NewStringResponder(200, `{"result": "fake-csrf-token"}`))
	httpmock.RegisterResponder("GET", `=~^http://superset-host/api/v1/chart/(20|21)$`, func(req *http.Request) (*http.Response, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.chartGone {
			return httpmock.NewStringResponse(404, `{"message": "Not found"}`), nil
		}
		id := httpmock.MustGetSubmatchAsInt(req, 1)
		return httpmock.NewStringResponse(200, fmt.Sprintf(`{"id": %d, "result": {"slice_name": "Sales", "viz_type": "table", "datasource_id": 10, "datasource_type": "table", "params": "{}", "owners": [], "dashboards": [], "tags": []}}`, id)), nil
	})
	httpmock.RegisterResponder("PUT", "http://superset-host/api/v1/chart/warm_up_cache", func(req *http.Request) (*http.Response, error) {
		body, _ := io.ReadAll(req.Body)
		var payload map[string]int64
		_ = json.Unmarshal(body, &payload)
		f.mu.Lock()
		defer f.mu.Unlock()
		f.warmups = append(f.warmups, strings.TrimSpace(string(body)))
		switch payload["chart_id"] {
		case 20:
			return httpmock.NewStringResponse(200, `{"result": [{"chart_id": 20, "viz_error": null, "viz_status": "success"}]}`), nil
		case 21:
			return httpmock.NewStringResponse(200, `{"result": [{"chart_id": 21, "viz_error": "Chart's query context does not exist", "viz_status": null}]}`), nil
		default:
			return httpmock.NewStringResponse(404, `{"message": "Chart not found"}`), nil
		}
	})
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/cachekey/invalidate", func(req *http.Request) (*http.Response, error) {
		body, _ := io.ReadAll(req.Body)
		f.mu.Lock()
		defer f.mu.Unlock()
		f.drops = append(f.drops, strings.TrimSpace(string(body)))
		return httpmock.NewStringResponse(201, `{}`), nil
	})
}

func (f *fakeCache) sentWarmups() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.warmups...)
}

func (f *fakeCache) sentDrops() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.drops...)
}

func requestsCheck(name string, got func() []string, want ...string) resource.TestCheckFunc {
	return func(*terraform.State) error {
		if g := got(); strings.Join(g, " ") != strings.Join(want, " ") {
			return fmt.Errorf("%s requests: %q, want %q", name, g, want)
		}
		return nil
	}
}

func TestAccChartCacheWarmupResource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	f := &fakeCache{}
	f.serve()
	warmups := func(want ...string) resource.TestCheckFunc { return requestsCheck("warm-up", f.sentWarmups, want...) }

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
resource "superset_chart_cache_warmup" "test" {
  chart_id     = 20
  dashboard_id = 30
  triggers     = { version = "1" }
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_chart_cache_warmup.test", "id", "20"),
					resource.TestCheckResourceAttr("superset_chart_cache_warmup.test", "viz_status", "success"),
					resource.TestCheckNoResourceAttr("superset_chart_cache_warmup.test", "viz_error"),
					warmups(`{"chart_id":20,"dashboard_id":30}`),
				),
			},
			{
				// A new trigger runs the warm-up again; nothing else changes.
				Config: providerConfig + `
resource "superset_chart_cache_warmup" "test" {
  chart_id     = 20
  dashboard_id = 30
  triggers     = { version = "2" }
}
`,
				ConfigPlanChecks: resource.ConfigPlanChecks{
					PreApply: []plancheck.PlanCheck{plancheck.ExpectResourceAction("superset_chart_cache_warmup.test", plancheck.ResourceActionUpdate)},
				},
				Check: warmups(`{"chart_id":20,"dashboard_id":30}`, `{"chart_id":20,"dashboard_id":30}`),
			},
			{
				// Another chart is another resource; a chart that cannot be computed is
				// reported, not failed.
				Config: providerConfig + `
resource "superset_chart_cache_warmup" "test" {
  chart_id = 21
  triggers = { version = "2" }
}
`,
				ConfigPlanChecks: resource.ConfigPlanChecks{
					PreApply: []plancheck.PlanCheck{plancheck.ExpectResourceAction("superset_chart_cache_warmup.test", plancheck.ResourceActionReplace)},
				},
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_chart_cache_warmup.test", "id", "21"),
					resource.TestCheckNoResourceAttr("superset_chart_cache_warmup.test", "viz_status"),
					resource.TestCheckResourceAttr("superset_chart_cache_warmup.test", "viz_error", "Chart's query context does not exist"),
					warmups(`{"chart_id":20,"dashboard_id":30}`, `{"chart_id":20,"dashboard_id":30}`, `{"chart_id":21}`),
				),
			},
			{
				// A chart deleted outside Terraform removes the resource from the state, so the
				// warm-up is planned again.
				PreConfig: func() {
					f.mu.Lock()
					f.chartGone = true
					f.mu.Unlock()
				},
				RefreshState:       true,
				ExpectNonEmptyPlan: true,
			},
		},
	})
}

func TestAccChartCacheWarmupResourceMissingChart(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	f := &fakeCache{}
	f.serve()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
resource "superset_chart_cache_warmup" "test" {
  chart_id = 99
}
`,
				ExpectError: regexp.MustCompile(`(?s)Could not warm up the cache of chart 99: .*not\s+found`),
			},
		},
	})
}

func TestAccCacheInvalidationResource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	f := &fakeCache{}
	f.serve()
	drops := func(want ...string) resource.TestCheckFunc {
		return requestsCheck("invalidation", f.sentDrops, want...)
	}

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
resource "superset_cache_invalidation" "test" {
  dataset_ids = [12, 10]
  triggers    = { loaded_at = "2026-11-14" }
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_cache_invalidation.test", "id", "10__table,12__table"),
					drops(`{"datasource_uids":["10__table","12__table"]}`),
				),
			},
			{
				// The same configuration does not run again.
				Config: providerConfig + `
resource "superset_cache_invalidation" "test" {
  dataset_ids = [10, 12]
  triggers    = { loaded_at = "2026-11-14" }
}
`,
				ConfigPlanChecks: resource.ConfigPlanChecks{
					PreApply: []plancheck.PlanCheck{plancheck.ExpectEmptyPlan()},
				},
				Check: drops(`{"datasource_uids":["10__table","12__table"]}`),
			},
			{
				// A new trigger runs it again; other datasets are another resource.
				Config: providerConfig + `
resource "superset_cache_invalidation" "test" {
  dataset_ids = [10, 12]
  triggers    = { loaded_at = "2026-11-15" }
}
`,
				Check: drops(`{"datasource_uids":["10__table","12__table"]}`, `{"datasource_uids":["10__table","12__table"]}`),
			},
			{
				Config: providerConfig + `
resource "superset_cache_invalidation" "test" {
  dataset_ids = [10]
  triggers    = { loaded_at = "2026-11-15" }
}
`,
				ConfigPlanChecks: resource.ConfigPlanChecks{
					PreApply: []plancheck.PlanCheck{plancheck.ExpectResourceAction("superset_cache_invalidation.test", plancheck.ResourceActionReplace)},
				},
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_cache_invalidation.test", "id", "10__table"),
					drops(`{"datasource_uids":["10__table","12__table"]}`, `{"datasource_uids":["10__table","12__table"]}`, `{"datasource_uids":["10__table"]}`),
				),
			},
		},
	})
}
