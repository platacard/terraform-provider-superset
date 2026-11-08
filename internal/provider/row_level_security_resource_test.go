package provider

import (
	"encoding/json"
	"net/http"
	"regexp"
	"sync"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/jarcoal/httpmock"
)

// fakeRLS serves the row level security endpoints of the fake Superset host for one rule with
// id 5, remembering the values written by the create and update requests. Tables and roles are
// reported in reverse order to prove that the resource treats them as sets.
type fakeRLS struct {
	mu      sync.Mutex
	rule    map[string]any
	deleted bool
}

func (f *fakeRLS) serve() {
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/csrf_token/",
		httpmock.NewStringResponder(200, `{"result": "fake-csrf-token"}`))

	store := func(req *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(req.Body).Decode(&payload); err == nil {
			f.mu.Lock()
			f.rule, f.deleted = payload, false
			f.mu.Unlock()
		}
	}
	reversed := func(values any, label string) []map[string]any {
		items, _ := values.([]any)
		out := make([]map[string]any, 0, len(items))
		for i := len(items) - 1; i >= 0; i-- {
			out = append(out, map[string]any{"id": items[i], "name": label, "table_name": label})
		}
		return out
	}
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/rowlevelsecurity/",
		func(req *http.Request) (*http.Response, error) {
			store(req)
			return httpmock.NewStringResponse(201, `{"id": 5}`), nil
		})
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/rowlevelsecurity/5",
		func(req *http.Request) (*http.Response, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.deleted || f.rule == nil {
				return httpmock.NewStringResponse(404, `{"message": "Not found"}`), nil
			}
			result := map[string]any{}
			for k, v := range f.rule {
				result[k] = v
			}
			result["tables"] = reversed(f.rule["tables"], "table")
			result["roles"] = reversed(f.rule["roles"], "role")
			return httpmock.NewJsonResponse(200, map[string]any{"id": 5, "result": result})
		})
	httpmock.RegisterResponder("PUT", "http://superset-host/api/v1/rowlevelsecurity/5",
		func(req *http.Request) (*http.Response, error) {
			store(req)
			return httpmock.NewStringResponse(200, `{"id": 5}`), nil
		})
	httpmock.RegisterResponder("DELETE", "http://superset-host/api/v1/rowlevelsecurity/5",
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

const rlsCreateConfig = providerConfig + `
resource "superset_row_level_security" "test" {
  name   = "Region filter"
  clause = "region = 'EMEA'"
  tables = [12, 11]
  roles  = [4]
}
`

const rlsUpdateConfig = providerConfig + `
resource "superset_row_level_security" "test" {
  name        = "Region filter"
  description = "Everyone but admins"
  filter_type = "Base"
  group_key   = "region"
  clause      = "region = 'EMEA'"
  tables      = [11, 12, 13]
  roles       = [4, 3]
}
`

const rlsUpdateReorderedConfig = providerConfig + `
resource "superset_row_level_security" "test" {
  name        = "Region filter"
  description = "Everyone but admins"
  filter_type = "Base"
  group_key   = "region"
  clause      = "region = 'EMEA'"
  tables      = [13, 12, 11]
  roles       = [3, 4]
}
`

func TestAccRowLevelSecurityResource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeRLS{}
	fake.serve()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: rlsCreateConfig,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_row_level_security.test", "id", "5"),
					resource.TestCheckResourceAttr("superset_row_level_security.test", "filter_type", "Regular"),
					resource.TestCheckNoResourceAttr("superset_row_level_security.test", "description"),
					resource.TestCheckNoResourceAttr("superset_row_level_security.test", "group_key"),
					resource.TestCheckResourceAttr("superset_row_level_security.test", "tables.#", "2"),
					resource.TestCheckTypeSetElemAttr("superset_row_level_security.test", "tables.*", "11"),
					resource.TestCheckTypeSetElemAttr("superset_row_level_security.test", "tables.*", "12"),
					resource.TestCheckResourceAttr("superset_row_level_security.test", "roles.#", "1"),
				),
			},
			{
				ResourceName:      "superset_row_level_security.test",
				ImportState:       true,
				ImportStateVerify: true,
			},
			{
				Config: rlsUpdateConfig,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_row_level_security.test", "filter_type", "Base"),
					resource.TestCheckResourceAttr("superset_row_level_security.test", "description", "Everyone but admins"),
					resource.TestCheckResourceAttr("superset_row_level_security.test", "group_key", "region"),
					resource.TestCheckResourceAttr("superset_row_level_security.test", "tables.#", "3"),
					resource.TestCheckResourceAttr("superset_row_level_security.test", "roles.#", "2"),
				),
			},
			{
				Config:   rlsUpdateReorderedConfig,
				PlanOnly: true,
			},
		},
	})
}

func TestAccRowLevelSecurityResourceRejectsUnknownFilterType(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeRLS{}
	fake.serve()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
resource "superset_row_level_security" "test" {
  name        = "Region filter"
  filter_type = "Strict"
  clause      = "region = 'EMEA'"
  tables      = [12]
}
`,
				ExpectError: regexp.MustCompile(`value must be one of: Regular, Base, got "Strict"`),
			},
		},
	})
}

func TestAccRowLevelSecurityResourceRecreatedWhenDeletedOutsideTerraform(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeRLS{}
	fake.serve()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: rlsCreateConfig,
			},
			{
				PreConfig: func() {
					fake.mu.Lock()
					fake.deleted = true
					fake.mu.Unlock()
				},
				Config:             rlsCreateConfig,
				PlanOnly:           true,
				ExpectNonEmptyPlan: true,
			},
		},
	})
}
