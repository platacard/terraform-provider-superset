package provider

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
	"github.com/jarcoal/httpmock"
)

type idRef struct {
	ID int64 `json:"id"`
}

type nameRef struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

type fakeChartRow struct {
	ID             int64     `json:"id"`
	Name           string    `json:"slice_name"`
	VizType        string    `json:"viz_type"`
	DatasourceID   int64     `json:"datasource_id"`
	DatasourceType string    `json:"datasource_type"`
	Description    *string   `json:"description"`
	URL            string    `json:"url"`
	Owners         []idRef   `json:"owners"`
	Tags           []nameRef `json:"tags"`
	Dashboards     []idRef   `json:"dashboards"`
}

type fakeDashboardRow struct {
	ID        int64     `json:"id"`
	Title     string    `json:"dashboard_title"`
	Slug      *string   `json:"slug"`
	Published bool      `json:"published"`
	Status    string    `json:"status"`
	URL       string    `json:"url"`
	Owners    []idRef   `json:"owners"`
	Tags      []nameRef `json:"tags"`
}

// fakeListing serves the chart and dashboard list endpoints the way Superset filters them:
// equality filters are exact, the tag filters match a case-insensitive substring of a tag
// name, and a many-to-many filter whose id matches no row is ignored, which returns every
// row. It records the last query of each endpoint so a test can check the filters sent.
type fakeListing struct {
	mu         sync.Mutex
	charts     []fakeChartRow
	dashboards []fakeDashboardRow
	lastQuery  map[string]string
}

var filterPattern = regexp.MustCompile(`\(col:(\w+),opr:(\w+),value:([^)]*)\)`)

func (f *fakeListing) serve() {
	f.lastQuery = map[string]string{}
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))
	httpmock.RegisterResponder("GET", listEndpoint("/api/v1/chart/"), func(req *http.Request) (*http.Response, error) {
		q := req.URL.Query().Get("q")
		f.mu.Lock()
		f.lastQuery["/api/v1/chart/"] = q
		f.mu.Unlock()
		var rows []any
		for _, chart := range f.charts {
			if f.matches(q, chart.VizType, chart.DatasourceID, nil, chart.Owners, chart.Tags, chart.Dashboards, "chart_tags") {
				rows = append(rows, chart)
			}
		}
		return listResponse(rows)
	})
	httpmock.RegisterResponder("GET", listEndpoint("/api/v1/dashboard/"), func(req *http.Request) (*http.Response, error) {
		q := req.URL.Query().Get("q")
		f.mu.Lock()
		f.lastQuery["/api/v1/dashboard/"] = q
		f.mu.Unlock()
		var rows []any
		for _, dashboard := range f.dashboards {
			published := dashboard.Published
			if f.matches(q, "", 0, &published, dashboard.Owners, dashboard.Tags, nil, "dashboard_tags") {
				rows = append(rows, dashboard)
			}
		}
		return listResponse(rows)
	})
}

// matches applies the filters of the query to one row the way Superset does.
func (f *fakeListing) matches(q, vizType string, datasourceID int64, published *bool, owners []idRef, tags []nameRef, dashboards []idRef, tagOperator string) bool {
	for _, m := range filterPattern.FindAllStringSubmatch(q, -1) {
		column, operator, value := m[1], m[2], strings.Trim(m[3], "'")
		switch {
		case column == "viz_type" && operator == "eq":
			if vizType != value {
				return false
			}
		case column == "datasource_id" && operator == "eq":
			if strconv.FormatInt(datasourceID, 10) != value {
				return false
			}
		case column == "published" && operator == "eq":
			if published == nil || *published != (value == "!t") {
				return false
			}
		case column == "tags" && operator == tagOperator:
			found := false
			for _, tag := range tags {
				if strings.Contains(strings.ToLower(tag.Name), strings.ToLower(value)) {
					found = true
				}
			}
			if !found {
				return false
			}
		case (column == "owners" || column == "dashboards") && operator == "rel_m_m":
			id, _ := strconv.ParseInt(value, 10, 64)
			related := owners
			known := f.knownOwner(id)
			if column == "dashboards" {
				related = dashboards
				known = f.knownDashboard(id)
			}
			if !known {
				continue // Flask-AppBuilder ignores a relation filter with an unknown id.
			}
			found := false
			for _, ref := range related {
				if ref.ID == id {
					found = true
				}
			}
			if !found {
				return false
			}
		default:
			panic("unexpected filter " + m[0])
		}
	}
	return true
}

func (f *fakeListing) knownOwner(id int64) bool {
	for _, chart := range f.charts {
		for _, owner := range chart.Owners {
			if owner.ID == id {
				return true
			}
		}
	}
	for _, dashboard := range f.dashboards {
		for _, owner := range dashboard.Owners {
			if owner.ID == id {
				return true
			}
		}
	}
	return false
}

func (f *fakeListing) knownDashboard(id int64) bool {
	for _, dashboard := range f.dashboards {
		if dashboard.ID == id {
			return true
		}
	}
	return false
}

// sentFilters returns the filters of the last query of the endpoint.
func (f *fakeListing) sentFilters(path string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return strings.Join(filterPattern.FindAllString(f.lastQuery[path], -1), ",")
}

func listResponse(rows []any) (*http.Response, error) {
	if rows == nil {
		rows = []any{}
	}
	body, err := json.Marshal(map[string]any{"count": len(rows), "result": rows})
	if err != nil {
		return nil, err
	}
	return httpmock.NewStringResponse(200, string(body)), nil
}

func newFakeListing() *fakeListing {
	daily := "Daily"
	sales := "sales"
	return &fakeListing{
		charts: []fakeChartRow{
			{ID: 20, Name: "Sales by day", VizType: "table", DatasourceID: 10, DatasourceType: "table", Description: &daily, URL: "/explore/?slice_id=20", Owners: []idRef{{1}}, Tags: []nameRef{{60, "finance"}, {1, "owner:1"}}, Dashboards: []idRef{{30}}},
			{ID: 21, Name: "Sales by region", VizType: "pie", DatasourceID: 10, DatasourceType: "table", URL: "/explore/?slice_id=21", Owners: []idRef{{2}}, Tags: []nameRef{{61, "finance-archive"}}, Dashboards: []idRef{{30}, {31}}},
			{ID: 22, Name: "Signups", VizType: "table", DatasourceID: 12, DatasourceType: "table", URL: "/explore/?slice_id=22", Owners: []idRef{{1}}},
		},
		dashboards: []fakeDashboardRow{
			{ID: 30, Title: "Sales", Slug: &sales, Published: true, Status: "published", URL: "/superset/dashboard/sales/", Owners: []idRef{{1}}, Tags: []nameRef{{62, "public"}}},
			{ID: 31, Title: "Sales draft", Published: false, Status: "draft", URL: "/superset/dashboard/31/", Owners: []idRef{{2}}, Tags: []nameRef{{63, "public-old"}}},
			{ID: 32, Title: "Ops", Published: true, Status: "published", URL: "/superset/dashboard/32/", Owners: []idRef{{1}}},
		},
	}
}

// checkSentFilters checks the filters the last request of the endpoint carried.
func checkSentFilters(f *fakeListing, path, want string) resource.TestCheckFunc {
	return func(*terraform.State) error {
		if got := f.sentFilters(path); got != want {
			return fmt.Errorf("filters sent to %s: got %q, want %q", path, got, want)
		}
		return nil
	}
}

func TestAccChartsDataSource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	f := newFakeListing()
	f.serve()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
data "superset_charts" "all" {}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.superset_charts.all", "ids.#", "3"),
					resource.TestCheckResourceAttr("data.superset_charts.all", "ids.0", "20"),
					resource.TestCheckResourceAttr("data.superset_charts.all", "ids.2", "22"),
					resource.TestCheckResourceAttr("data.superset_charts.all", "charts.#", "3"),
					resource.TestCheckResourceAttr("data.superset_charts.all", "charts.0.name", "Sales by day"),
					resource.TestCheckResourceAttr("data.superset_charts.all", "charts.0.viz_type", "table"),
					resource.TestCheckResourceAttr("data.superset_charts.all", "charts.0.datasource_id", "10"),
					resource.TestCheckResourceAttr("data.superset_charts.all", "charts.0.datasource_type", "table"),
					resource.TestCheckResourceAttr("data.superset_charts.all", "charts.0.description", "Daily"),
					resource.TestCheckResourceAttr("data.superset_charts.all", "charts.0.url", "/explore/?slice_id=20"),
					resource.TestCheckNoResourceAttr("data.superset_charts.all", "charts.1.description"),
					checkSentFilters(f, "/api/v1/chart/", ""),
				),
			},
			{
				Config: providerConfig + `
data "superset_charts" "tables" {
  viz_type      = "table"
  datasource_id = 10
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.superset_charts.tables", "ids.#", "1"),
					resource.TestCheckResourceAttr("data.superset_charts.tables", "ids.0", "20"),
					checkSentFilters(f, "/api/v1/chart/", "(col:viz_type,opr:eq,value:'table'),(col:datasource_id,opr:eq,value:10)"),
				),
			},
			{
				// The server matches "finance" loosely (it also returns "finance-archive"); the
				// data source keeps the exact tag only.
				Config: providerConfig + `
data "superset_charts" "finance" {
  tag = "finance"
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.superset_charts.finance", "ids.#", "1"),
					resource.TestCheckResourceAttr("data.superset_charts.finance", "ids.0", "20"),
					checkSentFilters(f, "/api/v1/chart/", "(col:tags,opr:chart_tags,value:'finance')"),
				),
			},
			{
				Config: providerConfig + `
data "superset_charts" "on_draft" {
  dashboard_id = 31
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.superset_charts.on_draft", "ids.#", "1"),
					resource.TestCheckResourceAttr("data.superset_charts.on_draft", "ids.0", "21"),
					checkSentFilters(f, "/api/v1/chart/", "(col:dashboards,opr:rel_m_m,value:31)"),
				),
			},
			{
				// The server ignores an owner that does not exist and returns every chart; the
				// data source returns none.
				Config: providerConfig + `
data "superset_charts" "of_nobody" {
  owner_id = 9
}

data "superset_charts" "of_two" {
  owner_id = 2
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.superset_charts.of_nobody", "ids.#", "0"),
					resource.TestCheckResourceAttr("data.superset_charts.of_nobody", "charts.#", "0"),
					resource.TestCheckResourceAttr("data.superset_charts.of_two", "ids.#", "1"),
					resource.TestCheckResourceAttr("data.superset_charts.of_two", "ids.0", "21"),
				),
			},
		},
	})
}

func TestAccDashboardsDataSource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	f := newFakeListing()
	f.serve()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
data "superset_dashboards" "all" {}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.superset_dashboards.all", "ids.#", "3"),
					resource.TestCheckResourceAttr("data.superset_dashboards.all", "dashboards.0.id", "30"),
					resource.TestCheckResourceAttr("data.superset_dashboards.all", "dashboards.0.title", "Sales"),
					resource.TestCheckResourceAttr("data.superset_dashboards.all", "dashboards.0.slug", "sales"),
					resource.TestCheckResourceAttr("data.superset_dashboards.all", "dashboards.0.published", "true"),
					resource.TestCheckResourceAttr("data.superset_dashboards.all", "dashboards.0.status", "published"),
					resource.TestCheckResourceAttr("data.superset_dashboards.all", "dashboards.0.url", "/superset/dashboard/sales/"),
					resource.TestCheckNoResourceAttr("data.superset_dashboards.all", "dashboards.1.slug"),
					checkSentFilters(f, "/api/v1/dashboard/", ""),
				),
			},
			{
				Config: providerConfig + `
data "superset_dashboards" "published" {
  published = true
}

data "superset_dashboards" "drafts" {
  published = false
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.superset_dashboards.published", "ids.#", "2"),
					resource.TestCheckResourceAttr("data.superset_dashboards.published", "ids.0", "30"),
					resource.TestCheckResourceAttr("data.superset_dashboards.published", "ids.1", "32"),
					resource.TestCheckResourceAttr("data.superset_dashboards.drafts", "ids.#", "1"),
					resource.TestCheckResourceAttr("data.superset_dashboards.drafts", "ids.0", "31"),
				),
			},
			{
				Config: providerConfig + `
data "superset_dashboards" "public" {
  tag      = "public"
  owner_id = 1
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.superset_dashboards.public", "ids.#", "1"),
					resource.TestCheckResourceAttr("data.superset_dashboards.public", "ids.0", "30"),
					checkSentFilters(f, "/api/v1/dashboard/", "(col:owners,opr:rel_m_m,value:1),(col:tags,opr:dashboard_tags,value:'public')"),
				),
			},
			{
				Config: providerConfig + `
data "superset_dashboards" "of_nobody" {
  owner_id = 9
}
`,
				Check: resource.TestCheckResourceAttr("data.superset_dashboards.of_nobody", "ids.#", "0"),
			},
		},
	})
}
