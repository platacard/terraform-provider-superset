package provider

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
	"github.com/jarcoal/httpmock"
)

// fakeSavedQuery serves the saved query endpoints of the fake Superset host for one query
// with id 9, remembering the raw payload of the last create or update request.
type fakeSavedQuery struct {
	mu      sync.Mutex
	payload map[string]any
	deleted bool
}

func (f *fakeSavedQuery) serve() {
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/csrf_token/",
		httpmock.NewStringResponder(200, `{"result": "fake-csrf-token"}`))

	store := func(req *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(req.Body).Decode(&payload); err == nil {
			f.mu.Lock()
			if f.payload == nil {
				f.payload = map[string]any{}
			}
			for k, v := range payload {
				f.payload[k] = v
			}
			f.deleted = false
			f.mu.Unlock()
		}
	}
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/saved_query/",
		func(req *http.Request) (*http.Response, error) {
			store(req)
			return httpmock.NewStringResponse(201, `{"id": 9}`), nil
		})
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/saved_query/9",
		func(req *http.Request) (*http.Response, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.deleted || f.payload == nil {
				return httpmock.NewStringResponse(404, `{"message": "Not found"}`), nil
			}
			result := map[string]any{"database": map[string]any{"id": f.payload["db_id"], "database_name": "analytics"}}
			for _, k := range []string{"label", "sql", "description", "schema", "catalog", "template_parameters"} {
				result[k] = f.payload[k]
			}
			return httpmock.NewJsonResponse(200, map[string]any{"id": 9, "result": result})
		})
	httpmock.RegisterResponder("PUT", "http://superset-host/api/v1/saved_query/9",
		func(req *http.Request) (*http.Response, error) {
			store(req)
			return httpmock.NewStringResponse(200, `{"id": 9}`), nil
		})
	httpmock.RegisterResponder("DELETE", "http://superset-host/api/v1/saved_query/9",
		func(req *http.Request) (*http.Response, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.deleted = true
			return httpmock.NewStringResponse(200, `{"message": "OK"}`), nil
		})
}

func (f *fakeSavedQuery) has(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.payload[key]
	return ok
}

func TestAccSavedQueryResource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeSavedQuery{}
	fake.serve()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
resource "superset_saved_query" "test" {
  database_id = 3
  label       = "Daily signups"
  sql         = "SELECT count(*) FROM signups WHERE day = '{{ day }}'"
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_saved_query.test", "id", "9"),
					resource.TestCheckResourceAttr("superset_saved_query.test", "database_id", "3"),
					resource.TestCheckResourceAttr("superset_saved_query.test", "label", "Daily signups"),
					resource.TestCheckNoResourceAttr("superset_saved_query.test", "description"),
					resource.TestCheckNoResourceAttr("superset_saved_query.test", "catalog"),
					func(*terraform.State) error {
						if fake.has("catalog") {
							return fmt.Errorf("catalog must not be sent when it is not set")
						}
						return nil
					},
				),
			},
			{
				ResourceName:      "superset_saved_query.test",
				ImportState:       true,
				ImportStateVerify: true,
			},
			{
				Config: providerConfig + `
resource "superset_saved_query" "test" {
  database_id         = 4
  label               = "Daily signups (EU)"
  sql                 = "SELECT count(*) FROM signups WHERE region = 'EU'"
  description         = "Signups per day in the EU"
  schema              = "public"
  catalog             = "main"
  template_parameters = "{\"day\": \"2026-01-01\"}"
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_saved_query.test", "database_id", "4"),
					resource.TestCheckResourceAttr("superset_saved_query.test", "description", "Signups per day in the EU"),
					resource.TestCheckResourceAttr("superset_saved_query.test", "schema", "public"),
					resource.TestCheckResourceAttr("superset_saved_query.test", "catalog", "main"),
					resource.TestCheckResourceAttr("superset_saved_query.test", "template_parameters", `{"day": "2026-01-01"}`),
				),
			},
			{
				Config: providerConfig + `
resource "superset_saved_query" "test" {
  database_id = 4
  label       = "Daily signups (EU)"
  sql         = "SELECT count(*) FROM signups WHERE region = 'EU'"
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckNoResourceAttr("superset_saved_query.test", "description"),
					resource.TestCheckNoResourceAttr("superset_saved_query.test", "catalog"),
					func(*terraform.State) error {
						if !fake.has("catalog") {
							return fmt.Errorf("clearing the catalog must send an explicit null")
						}
						return nil
					},
				),
			},
		},
	})
}

func TestAccSavedQueryResourceRecreatedWhenDeletedOutsideTerraform(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeSavedQuery{}
	fake.serve()
	config := providerConfig + `
resource "superset_saved_query" "test" {
  database_id = 3
  label       = "Daily signups"
  sql         = "SELECT 1"
}
`

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{Config: config},
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
