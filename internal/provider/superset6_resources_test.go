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
	"github.com/hashicorp/terraform-plugin-testing/plancheck"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
	"github.com/jarcoal/httpmock"
)

// fakeTheme serves the theme endpoints of the fake Superset host for one theme with id 9,
// with the places of the default and the dark theme of the instance. Like Superset, it refuses
// to delete the theme while it holds a place.
type fakeTheme struct {
	mu            sync.Mutex
	theme         map[string]any
	deleted       bool
	systemDefault int64
	systemDark    int64
}

func (f *fakeTheme) serve() {
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/csrf_token/",
		httpmock.NewStringResponder(200, `{"result": "fake-csrf-token"}`))
	decode := func(req *http.Request) map[string]any {
		var payload map[string]any
		_ = json.NewDecoder(req.Body).Decode(&payload)
		return payload
	}
	valid := func(payload map[string]any) *http.Response {
		name, _ := payload["theme_name"].(string)
		data, _ := payload["json_data"].(string)
		if name == "" || data == "" {
			return httpmock.NewStringResponse(400, `{"message": {"json_data": ["Missing data for required field."]}}`)
		}
		if !json.Valid([]byte(data)) {
			return httpmock.NewStringResponse(400, `{"message": {"json_data": ["Invalid JSON configuration"]}}`)
		}
		return nil
	}
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/theme/",
		func(req *http.Request) (*http.Response, error) {
			payload := decode(req)
			if resp := valid(payload); resp != nil {
				return resp, nil
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			f.theme, f.deleted = payload, false
			return httpmock.NewStringResponse(201, `{"id": 9}`), nil
		})
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/theme/9",
		func(req *http.Request) (*http.Response, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.deleted || f.theme == nil {
				return httpmock.NewStringResponse(404, `{"message": "Not found"}`), nil
			}
			result := map[string]any{"id": 9, "theme_name": f.theme["theme_name"], "json_data": f.theme["json_data"], "uuid": "theme-uuid", "is_system": false, "is_system_default": f.systemDefault == 9, "is_system_dark": f.systemDark == 9}
			return httpmock.NewJsonResponse(200, map[string]any{"id": 9, "result": result})
		})
	httpmock.RegisterResponder("PUT", "http://superset-host/api/v1/theme/9",
		func(req *http.Request) (*http.Response, error) {
			payload := decode(req)
			if resp := valid(payload); resp != nil {
				return resp, nil
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			f.theme = payload
			return httpmock.NewStringResponse(200, `{"id": 9}`), nil
		})
	httpmock.RegisterResponder("PUT", "http://superset-host/api/v1/theme/9/set_system_default",
		func(req *http.Request) (*http.Response, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.systemDefault = 9
			return httpmock.NewStringResponse(200, `{"id": 9, "result": "success"}`), nil
		})
	httpmock.RegisterResponder("PUT", "http://superset-host/api/v1/theme/9/set_system_dark",
		func(req *http.Request) (*http.Response, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.systemDark = 9
			return httpmock.NewStringResponse(200, `{"id": 9, "result": "success"}`), nil
		})
	httpmock.RegisterResponder("DELETE", "http://superset-host/api/v1/theme/unset_system_default",
		func(req *http.Request) (*http.Response, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.systemDefault = 0
			return httpmock.NewStringResponse(200, `{"result": "success"}`), nil
		})
	httpmock.RegisterResponder("DELETE", "http://superset-host/api/v1/theme/unset_system_dark",
		func(req *http.Request) (*http.Response, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.systemDark = 0
			return httpmock.NewStringResponse(200, `{"result": "success"}`), nil
		})
	httpmock.RegisterResponder("DELETE", "http://superset-host/api/v1/theme/9",
		func(req *http.Request) (*http.Response, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.systemDefault == 9 || f.systemDark == 9 {
				return httpmock.NewStringResponse(422, `{"message": "Cannot delete theme that is set as system default or dark theme."}`), nil
			}
			f.deleted = true
			return httpmock.NewStringResponse(200, `{"message": "Deleted 1 theme"}`), nil
		})
}

func themeConfig(extra string) string {
	return providerConfig + `
resource "superset_theme" "test" {
  name      = "Corporate"
  json_data = jsonencode({ token = { colorPrimary = "#1f77b4" } })
` + extra + `
}
`
}

func TestAccThemeResource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeTheme{}
	fake.serve()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: themeConfig(""),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_theme.test", "id", "9"),
					resource.TestCheckResourceAttr("superset_theme.test", "name", "Corporate"),
					resource.TestCheckResourceAttr("superset_theme.test", "json_data", `{"token":{"colorPrimary":"#1f77b4"}}`),
					resource.TestCheckResourceAttr("superset_theme.test", "system_default", "false"),
					resource.TestCheckResourceAttr("superset_theme.test", "system_dark", "false"),
					resource.TestCheckResourceAttr("superset_theme.test", "uuid", "theme-uuid"),
				),
			},
			{
				ResourceName:      "superset_theme.test",
				ImportState:       true,
				ImportStateVerify: true,
			},
			{
				Config: themeConfig(`  system_default = true
  system_dark    = true`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_theme.test", "system_default", "true"),
					resource.TestCheckResourceAttr("superset_theme.test", "system_dark", "true"),
					func(*terraform.State) error {
						fake.mu.Lock()
						defer fake.mu.Unlock()
						if fake.systemDefault != 9 || fake.systemDark != 9 {
							return errors.New("the theme must hold both system places")
						}
						return nil
					},
				),
			},
			{
				// Another theme took the default place: giving it up must not unset that theme.
				PreConfig: func() {
					fake.mu.Lock()
					fake.systemDefault = 2
					fake.mu.Unlock()
				},
				Config: themeConfig(`  system_dark = true`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_theme.test", "system_default", "false"),
					func(*terraform.State) error {
						fake.mu.Lock()
						defer fake.mu.Unlock()
						if fake.systemDefault != 2 {
							return fmt.Errorf("the default place of theme 2 must be left alone, got %d", fake.systemDefault)
						}
						return nil
					},
				),
			},
			{
				// The destroy gives up the dark place first: the fake refuses to delete a system theme.
				Config:  providerConfig,
				Destroy: true,
				Check: func(*terraform.State) error {
					fake.mu.Lock()
					defer fake.mu.Unlock()
					if !fake.deleted || fake.systemDark != 0 {
						return errors.New("the theme must give up the dark place and be deleted")
					}
					return nil
				},
			},
			{
				Config: providerConfig + `
resource "superset_theme" "test" {
  name      = "Corporate"
  json_data = "{not json"
}
`,
				ExpectError: regexp.MustCompile(`(?s)json_data.*JSON`),
			},
		},
	})
}

// fakeGroup serves the group endpoints of the fake Superset host for one group with id 4.
type fakeGroup struct {
	mu      sync.Mutex
	group   map[string]any
	deleted bool
}

func (f *fakeGroup) serve() {
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/csrf_token/",
		httpmock.NewStringResponder(200, `{"result": "fake-csrf-token"}`))
	decode := func(req *http.Request) map[string]any {
		var payload map[string]any
		_ = json.NewDecoder(req.Body).Decode(&payload)
		return payload
	}
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/groups/",
		func(req *http.Request) (*http.Response, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.group, f.deleted = decode(req), false
			return httpmock.NewStringResponse(201, `{"id": 4}`), nil
		})
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/groups/4",
		func(req *http.Request) (*http.Response, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.deleted || f.group == nil {
				return httpmock.NewStringResponse(404, `{"message": "Not found"}`), nil
			}
			items := func(key string) []map[string]any {
				var result []map[string]any
				ids, _ := f.group[key].([]any)
				for _, id := range ids {
					result = append(result, map[string]any{"id": id, "name": "x"})
				}
				return result
			}
			result := map[string]any{"id": 4, "name": f.group["name"], "label": f.group["label"], "description": f.group["description"], "roles": items("roles"), "users": items("users")}
			return httpmock.NewJsonResponse(200, map[string]any{"id": 4, "result": result})
		})
	httpmock.RegisterResponder("PUT", "http://superset-host/api/v1/security/groups/4",
		func(req *http.Request) (*http.Response, error) {
			payload := decode(req)
			f.mu.Lock()
			defer f.mu.Unlock()
			for key, value := range payload {
				f.group[key] = value
			}
			return httpmock.NewStringResponse(200, `{"result": {}}`), nil
		})
	httpmock.RegisterResponder("DELETE", "http://superset-host/api/v1/security/groups/4",
		func(req *http.Request) (*http.Response, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.deleted = true
			return httpmock.NewStringResponse(200, `{"message": "OK"}`), nil
		})
}

func TestAccGroupResource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeGroup{}
	fake.serve()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
resource "superset_group" "test" {
  name        = "finance"
  label       = "Finance"
  description = "The finance team"
  roles       = [3, 5]
  users       = [1]
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_group.test", "id", "4"),
					resource.TestCheckResourceAttr("superset_group.test", "label", "Finance"),
					resource.TestCheckResourceAttr("superset_group.test", "roles.#", "2"),
					resource.TestCheckResourceAttr("superset_group.test", "users.#", "1"),
				),
			},
			{
				ResourceName:      "superset_group.test",
				ImportState:       true,
				ImportStateVerify: true,
			},
			{
				// Removing the optional attributes clears them and revokes the roles and members.
				Config: providerConfig + `
resource "superset_group" "test" {
  name = "finance-team"
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_group.test", "name", "finance-team"),
					resource.TestCheckNoResourceAttr("superset_group.test", "label"),
					resource.TestCheckNoResourceAttr("superset_group.test", "roles"),
					func(*terraform.State) error {
						fake.mu.Lock()
						defer fake.mu.Unlock()
						if fake.group["label"] != nil || fake.group["description"] != nil {
							return errors.New("cleared attributes must be sent as null")
						}
						if roles, _ := fake.group["roles"].([]any); len(roles) != 0 {
							return errors.New("the roles must be revoked with an empty list")
						}
						return nil
					},
				),
			},
			{
				PreConfig: func() {
					fake.mu.Lock()
					fake.deleted = true
					fake.mu.Unlock()
				},
				Config: providerConfig + `
resource "superset_group" "test" {
  name = "finance-team"
}
`,
				Check: resource.TestCheckResourceAttr("superset_group.test", "id", "4"),
			},
		},
	})
}

func TestAccDatabasePermissionsSyncResource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/csrf_token/",
		httpmock.NewStringResponder(200, `{"result": "fake-csrf-token"}`))
	var mu sync.Mutex
	syncs, exists := 0, true
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/database/6/sync_permissions/",
		func(req *http.Request) (*http.Response, error) {
			mu.Lock()
			defer mu.Unlock()
			if !exists {
				return httpmock.NewStringResponse(404, `{"message": "Database not found."}`), nil
			}
			syncs++
			return httpmock.NewStringResponse(200, `{"message": "Permissions successfully synced"}`), nil
		})
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/database/6/connection",
		func(req *http.Request) (*http.Response, error) {
			mu.Lock()
			defer mu.Unlock()
			if !exists {
				return httpmock.NewStringResponse(200, `{"result": {}}`), nil
			}
			return httpmock.NewStringResponse(200, `{"id": 6, "result": {"id": 6, "database_name": "Warehouse"}}`), nil
		})
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/database/7/sync_permissions/",
		httpmock.NewStringResponder(404, `{"errors": [{"message": "404 Not Found: The requested URL was not found on the server."}]}`))

	count := func(want int) resource.TestCheckFunc {
		return func(*terraform.State) error {
			mu.Lock()
			defer mu.Unlock()
			if syncs != want {
				return fmt.Errorf("the permissions were synced %d times, want %d", syncs, want)
			}
			return nil
		}
	}
	config := func(version string) string {
		return providerConfig + fmt.Sprintf(`
resource "superset_database_permissions_sync" "test" {
  database_id = 6
  triggers = {
    version = %q
  }
}
`, version)
	}

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: config("1"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_database_permissions_sync.test", "id", "6"),
					count(1),
				),
			},
			{
				// A changed trigger updates the resource in place and syncs again.
				Config: config("2"),
				ConfigPlanChecks: resource.ConfigPlanChecks{
					PreApply: []plancheck.PlanCheck{
						plancheck.ExpectResourceAction("superset_database_permissions_sync.test", plancheck.ResourceActionUpdate),
					},
				},
				Check: count(2),
			},
			{
				// A connection that was recreated is synced again.
				PreConfig: func() {
					mu.Lock()
					exists = false
					mu.Unlock()
				},
				Config:      config("2"),
				ExpectError: regexp.MustCompile(`sync the permissions of the database: not found`),
			},
			{
				Config: providerConfig + `
resource "superset_database_permissions_sync" "legacy" {
  database_id = 7
}
`,
				ExpectError: regexp.MustCompile(`need Superset 6`),
			},
		},
	})
}
