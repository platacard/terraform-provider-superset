package provider

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"sync"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/knownvalue"
	"github.com/hashicorp/terraform-plugin-testing/plancheck"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
	"github.com/hashicorp/terraform-plugin-testing/tfjsonpath"
	"github.com/jarcoal/httpmock"
)

// fakeDataset serves the dataset endpoints of the fake Superset host for one dataset with id
// 123. Like Superset, the create request only stores the identifying fields and an update merges
// the fields it carries into the dataset; the owners default to the current user and the main
// datetime column is detected when none is set.
type fakeDataset struct {
	mu      sync.Mutex
	dataset map[string]any
	metrics []map[string]any
	nextID  float64
	deleted bool
	updates int
	// legacy makes the create endpoint reject template parameters, as Superset 3.1 does.
	legacy  bool
	creates int
}

const fakeDatabases = `{"count": 2, "result": [
  {"id": 1, "database_name": "PostgreSQL Database", "backend": "postgresql"},
  {"id": 2, "database_name": "MySQL Database", "backend": "mysql"}
]}`

func (f *fakeDataset) serve() {
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/csrf_token/",
		httpmock.NewStringResponder(200, `{"result": "fake-csrf-token"}`))
	httpmock.RegisterResponder("GET", listEndpoint("/api/v1/database/"),
		httpmock.NewStringResponder(200, fakeDatabases))

	decode := func(req *http.Request) map[string]any {
		var payload map[string]any
		_ = json.NewDecoder(req.Body).Decode(&payload)
		return payload
	}
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/dataset/",
		func(req *http.Request) (*http.Response, error) {
			payload := decode(req)
			f.mu.Lock()
			defer f.mu.Unlock()
			f.creates++
			if _, present := payload["template_params"]; present && f.legacy {
				return httpmock.NewStringResponse(400, `{"message": {"template_params": ["Unknown field."]}}`), nil
			}
			f.dataset, f.deleted = payload, false
			// Superset creates the count metric with every dataset.
			f.metrics = []map[string]any{{"id": float64(1), "metric_name": "count", "expression": "COUNT(*)", "verbose_name": "COUNT(*)", "metric_type": "count"}}
			f.nextID = 2
			return httpmock.NewStringResponse(201, `{"id": 123}`), nil
		})
	httpmock.RegisterResponder("PUT", "http://superset-host/api/v1/dataset/123",
		func(req *http.Request) (*http.Response, error) {
			payload := decode(req)
			f.mu.Lock()
			defer f.mu.Unlock()
			for key, value := range payload {
				if key == "metrics" {
					continue
				}
				f.dataset[key] = value
			}
			if entries, ok := payload["metrics"].([]any); ok {
				// Like Superset: an entry with an id updates the metric and keeps the fields it
				// does not carry, one without an id creates a metric, and the rest are deleted.
				existing := map[float64]map[string]any{}
				for _, metric := range f.metrics {
					id, _ := metric["id"].(float64)
					existing[id] = metric
				}
				var kept []map[string]any
				for _, entry := range entries {
					fields, _ := entry.(map[string]any)
					metric := map[string]any{}
					if id, ok := fields["id"].(float64); ok {
						if _, known := existing[id]; !known {
							return httpmock.NewStringResponse(422, `{"message": {"metrics": ["One or more metrics do not exist"]}}`), nil
						}
						for k, v := range existing[id] {
							metric[k] = v
						}
					} else {
						metric["id"] = f.nextID
						f.nextID++
					}
					for k, v := range fields {
						metric[k] = v
					}
					kept = append(kept, metric)
				}
				f.metrics = kept
			}
			f.updates++
			return httpmock.NewStringResponse(200, `{"id": 123}`), nil
		})
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/dataset/123",
		func(req *http.Request) (*http.Response, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.deleted || f.dataset == nil {
				return httpmock.NewStringResponse(404, `{"message": "Not found"}`), nil
			}
			owners := []map[string]any{{"id": 1, "first_name": "Admin"}}
			if configured, ok := f.dataset["owners"].([]any); ok {
				owners = nil
				for _, id := range configured {
					owners = append(owners, map[string]any{"id": id})
				}
			}
			databaseName := map[any]string{float64(1): "PostgreSQL Database", float64(2): "MySQL Database"}[f.dataset["database"]]
			result := map[string]any{
				"id":                      123,
				"database":                map[string]any{"id": f.dataset["database"], "database_name": databaseName},
				"table_name":              f.dataset["table_name"],
				"schema":                  f.dataset["schema"],
				"sql":                     f.dataset["sql"],
				"description":             f.dataset["description"],
				"owners":                  owners,
				"cache_timeout":           f.dataset["cache_timeout"],
				"main_dttm_col":           "created_at",
				"template_params":         f.dataset["template_params"],
				"extra":                   f.dataset["extra"],
				"fetch_values_predicate":  f.dataset["fetch_values_predicate"],
				"filter_select_enabled":   true,
				"normalize_columns":       f.dataset["normalize_columns"],
				"always_filter_main_dttm": f.dataset["always_filter_main_dttm"],
				"default_endpoint":        f.dataset["default_endpoint"],
				"metrics":                 f.metrics,
			}
			if v, ok := f.dataset["main_dttm_col"]; ok && v != nil {
				result["main_dttm_col"] = v
			}
			if v, ok := f.dataset["filter_select_enabled"]; ok {
				result["filter_select_enabled"] = v
			}
			return httpmock.NewJsonResponse(200, map[string]any{"id": 123, "result": result})
		})
	httpmock.RegisterResponder("DELETE", "http://superset-host/api/v1/dataset/123",
		func(req *http.Request) (*http.Response, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.deleted = true
			return httpmock.NewStringResponse(200, `{"message": "OK"}`), nil
		})
}

// metric returns the stored metric with the given name, or nil.
func (f *fakeDataset) metric(name string) map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, metric := range f.metrics {
		if metric["metric_name"] == name {
			return metric
		}
	}
	return nil
}

// sent returns the value of a key of the dataset as the last request left it.
func (f *fakeDataset) sent(key string) any {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.dataset[key]
}

const physicalDataset = providerConfig + `
resource "superset_dataset" "test" {
  table_name    = "sales"
  database_name = "PostgreSQL Database"
  schema        = "public"
}
`

const configuredDataset = providerConfig + `
resource "superset_dataset" "test" {
  table_name             = "sales"
  database_name          = "PostgreSQL Database"
  schema                 = "public"
  description            = "Sales per day"
  owners                 = [1, 2]
  cache_timeout          = 600
  main_dttm_col          = "day"
  template_params        = jsonencode({ region = "eu" })
  extra                  = jsonencode({ certification = { certified_by = "QA" } })
  fetch_values_predicate = "day > '2026-01-01'"
  filter_select_enabled  = false
  normalize_columns      = true
  always_filter_main_dttm = true
  default_endpoint       = "https://example.com/sales"
}
`

func TestAccDatasetResource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeDataset{}
	fake.serve()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: physicalDataset,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_dataset.test", "id", "123"),
					resource.TestCheckResourceAttr("superset_dataset.test", "table_name", "sales"),
					resource.TestCheckResourceAttr("superset_dataset.test", "database_name", "PostgreSQL Database"),
					resource.TestCheckResourceAttr("superset_dataset.test", "schema", "public"),
					resource.TestCheckNoResourceAttr("superset_dataset.test", "sql"),
					resource.TestCheckResourceAttr("superset_dataset.test", "owners.#", "1"),
					resource.TestCheckResourceAttr("superset_dataset.test", "main_dttm_col", "created_at"),
					resource.TestCheckResourceAttr("superset_dataset.test", "filter_select_enabled", "true"),
					resource.TestCheckResourceAttr("superset_dataset.test", "normalize_columns", "false"),
					resource.TestCheckResourceAttr("superset_dataset.test", "always_filter_main_dttm", "false"),
					resource.TestCheckResourceAttr("superset_dataset.test", "perm", "[PostgreSQL Database].[sales](id:123)"),
					resource.TestCheckResourceAttr("superset_dataset.test", "schema_perm", "[PostgreSQL Database].[public]"),
					func(*terraform.State) error {
						if fake.sent("database") != float64(1) {
							return errors.New("the database must be looked up by its name")
						}
						if _, present := fake.dataset["main_dttm_col"]; present {
							return errors.New("the main datetime column must not be sent when it is not configured")
						}
						return nil
					},
				),
			},
			{
				ResourceName:      "superset_dataset.test",
				ImportState:       true,
				ImportStateVerify: true,
			},
			{
				Config: configuredDataset,
				// The permissions of an existing dataset are known during planning.
				ConfigPlanChecks: resource.ConfigPlanChecks{
					PreApply: []plancheck.PlanCheck{
						plancheck.ExpectKnownValue("superset_dataset.test", tfjsonpath.New("perm"), knownvalue.StringExact("[PostgreSQL Database].[sales](id:123)")),
						plancheck.ExpectKnownValue("superset_dataset.test", tfjsonpath.New("schema_perm"), knownvalue.StringExact("[PostgreSQL Database].[public]")),
					},
				},
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_dataset.test", "description", "Sales per day"),
					resource.TestCheckResourceAttr("superset_dataset.test", "owners.#", "2"),
					resource.TestCheckResourceAttr("superset_dataset.test", "cache_timeout", "600"),
					resource.TestCheckResourceAttr("superset_dataset.test", "main_dttm_col", "day"),
					resource.TestCheckResourceAttr("superset_dataset.test", "template_params", `{"region":"eu"}`),
					resource.TestCheckResourceAttr("superset_dataset.test", "fetch_values_predicate", "day > '2026-01-01'"),
					resource.TestCheckResourceAttr("superset_dataset.test", "filter_select_enabled", "false"),
					resource.TestCheckResourceAttr("superset_dataset.test", "normalize_columns", "true"),
					resource.TestCheckResourceAttr("superset_dataset.test", "always_filter_main_dttm", "true"),
					resource.TestCheckResourceAttr("superset_dataset.test", "default_endpoint", "https://example.com/sales"),
					func(*terraform.State) error {
						if fake.sent("extra") != `{"certification":{"certified_by":"QA"}}` {
							return fmt.Errorf("unexpected extra sent: %v", fake.sent("extra"))
						}
						return nil
					},
				),
			},
			{
				// Removing the optional attributes clears them with null; the computed ones keep their value.
				Config: physicalDataset,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckNoResourceAttr("superset_dataset.test", "description"),
					resource.TestCheckNoResourceAttr("superset_dataset.test", "cache_timeout"),
					resource.TestCheckNoResourceAttr("superset_dataset.test", "template_params"),
					resource.TestCheckNoResourceAttr("superset_dataset.test", "extra"),
					resource.TestCheckResourceAttr("superset_dataset.test", "main_dttm_col", "day"),
					resource.TestCheckResourceAttr("superset_dataset.test", "owners.#", "2"),
					func(*terraform.State) error {
						if fake.sent("description") != nil || fake.sent("cache_timeout") != nil {
							return errors.New("cleared attributes must be sent as null")
						}
						return nil
					},
				),
			},
			{
				// Another database means a replacement.
				Config: providerConfig + `
resource "superset_dataset" "test" {
  table_name    = "sales"
  database_name = "MySQL Database"
  schema        = "public"
}
`,
				PlanOnly:           true,
				ExpectNonEmptyPlan: true,
			},
			{
				PreConfig: func() {
					fake.mu.Lock()
					fake.deleted = true
					fake.mu.Unlock()
				},
				Config: physicalDataset,
				Check:  resource.TestCheckResourceAttr("superset_dataset.test", "id", "123"),
			},
		},
	})
}

func TestAccDatasetResourceVirtual(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeDataset{}
	fake.serve()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
resource "superset_dataset" "test" {
  table_name    = "signups_per_day"
  database_name = "PostgreSQL Database"
  sql           = "SELECT day, count(*) AS signups FROM signups GROUP BY day"
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_dataset.test", "sql", "SELECT day, count(*) AS signups FROM signups GROUP BY day"),
					resource.TestCheckNoResourceAttr("superset_dataset.test", "schema"),
					resource.TestCheckResourceAttr("superset_dataset.test", "perm", "[PostgreSQL Database].[signups_per_day](id:123)"),
					resource.TestCheckNoResourceAttr("superset_dataset.test", "schema_perm"),
				),
			},
		},
	})
}

// Superset 3.1 rejects template parameters in the create request; they are then written by the
// update that follows the creation.
func TestAccDatasetResourceTemplateParamsOnLegacySuperset(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeDataset{legacy: true}
	fake.serve()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
resource "superset_dataset" "test" {
  table_name      = "signups_per_day"
  database_name   = "PostgreSQL Database"
  sql             = "SELECT day FROM signups WHERE region = '{{ region | default('eu') }}'"
  template_params = jsonencode({ region = "eu" })
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_dataset.test", "template_params", `{"region":"eu"}`),
					func(*terraform.State) error {
						if fake.creates != 2 {
							return fmt.Errorf("expected the creation to be retried without template parameters, got %d create requests", fake.creates)
						}
						if fake.sent("template_params") != `{"region":"eu"}` {
							return errors.New("the template parameters must be written by the update")
						}
						return nil
					},
				),
			},
		},
	})
}

func TestAccDatasetResourceReplacedOnDatabaseChange(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeDataset{}
	fake.serve()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{Config: physicalDataset},
			{
				Config: providerConfig + `
resource "superset_dataset" "test" {
  table_name    = "sales"
  database_name = "MySQL Database"
  schema        = "public"
}
`,
				ConfigPlanChecks: resource.ConfigPlanChecks{
					PreApply: []plancheck.PlanCheck{
						plancheck.ExpectResourceAction("superset_dataset.test", plancheck.ResourceActionReplace),
					},
				},
				Check: resource.TestCheckResourceAttr("superset_dataset.test", "database_name", "MySQL Database"),
			},
		},
	})
}

func TestAccDatasetResourceUnknownDatabase(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeDataset{}
	fake.serve()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
resource "superset_dataset" "test" {
  table_name    = "sales"
  database_name = "Missing Database"
}
`,
				ExpectError: regexp.MustCompile(`database "Missing Database": not found`),
			},
		},
	})
}

func TestAccDatasetResourceMetrics(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeDataset{}
	fake.serve()

	withMetrics := providerConfig + `
resource "superset_dataset" "test" {
  table_name    = "sales"
  database_name = "PostgreSQL Database"
  schema        = "public"
  metrics = [
    { metric_name = "count", expression = "COUNT(*)" },
    { metric_name = "revenue", expression = "SUM(amount)", verbose_name = "Revenue", d3format = ",.2f", description = "Sum of amount", warning_text = "beta", metric_type = "sum" },
  ]
}
`
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				// Without metrics in the configuration the dataset keeps the count metric of Superset.
				Config: physicalDataset,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_dataset.test", "metrics.#", "1"),
					resource.TestCheckTypeSetElemNestedAttrs("superset_dataset.test", "metrics.*", map[string]string{"metric_name": "count", "expression": "COUNT(*)", "verbose_name": "COUNT(*)", "metric_type": "count"}),
					func(*terraform.State) error {
						if _, present := fake.dataset["metrics"]; present {
							return errors.New("metrics must not be sent when they are not configured")
						}
						return nil
					},
				),
			},
			{
				Config: withMetrics,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_dataset.test", "metrics.#", "2"),
					resource.TestCheckTypeSetElemNestedAttrs("superset_dataset.test", "metrics.*", map[string]string{"metric_name": "revenue", "expression": "SUM(amount)", "verbose_name": "Revenue", "d3format": ",.2f", "description": "Sum of amount", "warning_text": "beta", "metric_type": "sum"}),
					func(*terraform.State) error {
						count := fake.metric("count")
						if count == nil || count["id"] != float64(1) {
							return errors.New("the existing count metric must be updated in place, not recreated")
						}
						if count["verbose_name"] != nil || count["metric_type"] != nil {
							return errors.New("fields missing from the configuration must be cleared with null")
						}
						if revenue := fake.metric("revenue"); revenue == nil || revenue["id"] != float64(2) {
							return errors.New("the new metric must be created")
						}
						return nil
					},
				),
			},
			{
				ResourceName:      "superset_dataset.test",
				ImportState:       true,
				ImportStateVerify: true,
			},
			{
				// Removing a metric from the configuration deletes it.
				Config: providerConfig + `
resource "superset_dataset" "test" {
  table_name    = "sales"
  database_name = "PostgreSQL Database"
  schema        = "public"
  metrics = [
    { metric_name = "revenue", expression = "SUM(amount) / 100" },
  ]
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_dataset.test", "metrics.#", "1"),
					func(*terraform.State) error {
						if fake.metric("count") != nil {
							return errors.New("a metric removed from the configuration must be deleted")
						}
						if revenue := fake.metric("revenue"); revenue["id"] != float64(2) || revenue["expression"] != "SUM(amount) / 100" {
							return fmt.Errorf("the changed metric must keep its id, got %v", revenue)
						}
						return nil
					},
				),
			},
			{
				// Dropping the metrics attribute leaves the metrics alone.
				Config:   physicalDataset,
				PlanOnly: true,
			},
		},
	})
}
