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
	"github.com/hashicorp/terraform-plugin-testing/terraform"
	"github.com/jarcoal/httpmock"
)

// fakePermissionViews serves the permissions (can_read is 1), the view menus and the pairs of
// a Superset: the view menu Existing (50) exists and carries can_write (pair 300); new view
// menus and pairs get ids from 60 and 301 on.
type fakePermissionViews struct {
	mu        sync.Mutex
	viewMenus map[int64]string
	pairs     map[int64][2]int64 // pair id -> permission id, view menu id
	nextMenu  int64
	nextPair  int64
	log       []string
}

func newFakePermissionViews() *fakePermissionViews {
	return &fakePermissionViews{viewMenus: map[int64]string{50: "Existing"}, pairs: map[int64][2]int64{300: {2, 50}}, nextMenu: 60, nextPair: 301}
}

func (f *fakePermissionViews) permissionName(id int64) string {
	return map[int64]string{1: "can_read", 2: "can_write"}[id]
}

func (f *fakePermissionViews) serve() {
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/csrf_token/",
		httpmock.NewStringResponder(200, `{"result": "fake-csrf-token"}`))
	httpmock.RegisterResponder("GET", listEndpoint("/api/v1/security/permissions/"), func(req *http.Request) (*http.Response, error) {
		q := req.URL.Query().Get("q")
		for id, name := range map[int64]string{1: "can_read", 2: "can_write"} {
			if strings.Contains(q, fmt.Sprintf("(col:name,opr:eq,value:'%s')", name)) {
				return httpmock.NewStringResponse(200, fmt.Sprintf(`{"count": 1, "result": [{"id": %d, "name": %q}]}`, id, name)), nil
			}
		}
		return httpmock.NewStringResponse(200, `{"count": 0, "result": []}`), nil
	})
	httpmock.RegisterResponder("GET", listEndpoint("/api/v1/security/resources/"), func(req *http.Request) (*http.Response, error) {
		q := req.URL.Query().Get("q")
		f.mu.Lock()
		defer f.mu.Unlock()
		for id, name := range f.viewMenus {
			if strings.Contains(q, fmt.Sprintf("(col:name,opr:eq,value:'%s')", name)) {
				return httpmock.NewStringResponse(200, fmt.Sprintf(`{"count": 1, "result": [{"id": %d, "name": %q}]}`, id, name)), nil
			}
		}
		return httpmock.NewStringResponse(200, `{"count": 0, "result": []}`), nil
	})
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/resources/", func(req *http.Request) (*http.Response, error) {
		body, _ := io.ReadAll(req.Body)
		var payload map[string]string
		_ = json.Unmarshal(body, &payload)
		f.mu.Lock()
		defer f.mu.Unlock()
		id := f.nextMenu
		f.nextMenu++
		f.viewMenus[id] = payload["name"]
		f.log = append(f.log, "create menu "+payload["name"])
		return httpmock.NewStringResponse(201, fmt.Sprintf(`{"id": %d, "result": {"name": %q}}`, id, payload["name"])), nil
	})
	httpmock.RegisterResponder("DELETE", `=~^http://superset-host/api/v1/security/resources/(\d+)$`, func(req *http.Request) (*http.Response, error) {
		id := httpmock.MustGetSubmatchAsInt(req, 1)
		f.mu.Lock()
		defer f.mu.Unlock()
		name, ok := f.viewMenus[id]
		if !ok {
			return httpmock.NewStringResponse(404, `{"message": "Not found"}`), nil
		}
		delete(f.viewMenus, id)
		f.log = append(f.log, "delete menu "+name)
		return httpmock.NewStringResponse(200, `{"message": "OK"}`), nil
	})
	httpmock.RegisterResponder("GET", listEndpoint("/api/v1/security/permissions-resources/"), func(req *http.Request) (*http.Response, error) {
		q := req.URL.Query().Get("q")
		f.mu.Lock()
		defer f.mu.Unlock()
		rows := []string{}
		for id, pair := range f.pairs {
			if strings.Contains(q, fmt.Sprintf("(col:view_menu,opr:rel_o_m,value:%d)", pair[1])) {
				rows = append(rows, fmt.Sprintf(`{"id": %d, "permission": {"id": %d, "name": %q}, "view_menu": {"id": %d, "name": %q}}`, id, pair[0], f.permissionName(pair[0]), pair[1], f.viewMenus[pair[1]]))
			}
		}
		return httpmock.NewStringResponse(200, fmt.Sprintf(`{"count": %d, "result": [%s]}`, len(rows), strings.Join(rows, ","))), nil
	})
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/permissions-resources/", func(req *http.Request) (*http.Response, error) {
		body, _ := io.ReadAll(req.Body)
		var payload map[string]int64
		_ = json.Unmarshal(body, &payload)
		f.mu.Lock()
		defer f.mu.Unlock()
		id := f.nextPair
		f.nextPair++
		f.pairs[id] = [2]int64{payload["permission_id"], payload["view_menu_id"]}
		f.log = append(f.log, fmt.Sprintf("create pair %s on %s", f.permissionName(payload["permission_id"]), f.viewMenus[payload["view_menu_id"]]))
		return httpmock.NewStringResponse(201, fmt.Sprintf(`{"id": %d, "result": {"permission_id": %d, "view_menu_id": %d}}`, id, payload["permission_id"], payload["view_menu_id"])), nil
	})
	httpmock.RegisterResponder("GET", `=~^http://superset-host/api/v1/security/permissions-resources/(\d+)$`, func(req *http.Request) (*http.Response, error) {
		id := httpmock.MustGetSubmatchAsInt(req, 1)
		f.mu.Lock()
		defer f.mu.Unlock()
		pair, ok := f.pairs[id]
		if !ok {
			return httpmock.NewStringResponse(404, `{"message": "Not found"}`), nil
		}
		return httpmock.NewStringResponse(200, fmt.Sprintf(`{"id": %d, "result": {"id": %d, "permission": {"name": %q}, "view_menu": {"name": %q}}}`, id, id, f.permissionName(pair[0]), f.viewMenus[pair[1]])), nil
	})
	httpmock.RegisterResponder("DELETE", `=~^http://superset-host/api/v1/security/permissions-resources/(\d+)$`, func(req *http.Request) (*http.Response, error) {
		id := httpmock.MustGetSubmatchAsInt(req, 1)
		f.mu.Lock()
		defer f.mu.Unlock()
		pair, ok := f.pairs[id]
		if !ok {
			return httpmock.NewStringResponse(404, `{"message": "Not found"}`), nil
		}
		delete(f.pairs, id)
		f.log = append(f.log, fmt.Sprintf("delete pair %s on %s", f.permissionName(pair[0]), f.viewMenus[pair[1]]))
		return httpmock.NewStringResponse(200, `{"message": "OK"}`), nil
	})
}

func (f *fakePermissionViews) actions() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return strings.Join(f.log, "; ")
}

func permissionViewConfig(permission, viewMenu string) string {
	return providerConfig + fmt.Sprintf(`
resource "superset_permission_view" "test" {
  permission_name = %q
  view_menu_name  = %q
}
`, permission, viewMenu)
}

func TestAccPermissionViewResource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	f := newFakePermissionViews()
	f.serve()
	actions := func(want string) resource.TestCheckFunc {
		return func(*terraform.State) error {
			if got := f.actions(); got != want {
				return fmt.Errorf("actions: %q, want %q", got, want)
			}
			return nil
		}
	}

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				// A view menu that does not exist is created with the pair.
				Config: permissionViewConfig("can_read", "CustomReports"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_permission_view.test", "id", "301"),
					resource.TestCheckResourceAttr("superset_permission_view.test", "permission_id", "1"),
					resource.TestCheckResourceAttr("superset_permission_view.test", "view_menu_id", "60"),
					resource.TestCheckResourceAttr("superset_permission_view.test", "created_view_menu", "true"),
					actions("create menu CustomReports; create pair can_read on CustomReports"),
				),
			},
			{
				ResourceName:      "superset_permission_view.test",
				ImportState:       true,
				ImportStateVerify: true,
				// An imported pair does not know that the view menu was created by Terraform.
				ImportStateVerifyIgnore: []string{"created_view_menu"},
			},
			{
				// Another permission on the same view menu replaces the pair: the old pair goes
				// with the view menu it had created, and the new pair creates the view menu
				// again.
				Config: permissionViewConfig("can_write", "CustomReports"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_permission_view.test", "id", "302"),
					resource.TestCheckResourceAttr("superset_permission_view.test", "view_menu_id", "61"),
					resource.TestCheckResourceAttr("superset_permission_view.test", "created_view_menu", "true"),
					actions("create menu CustomReports; create pair can_read on CustomReports; delete pair can_read on CustomReports; delete menu CustomReports; create menu CustomReports; create pair can_write on CustomReports"),
				),
			},
		},
	})
	if got := f.actions(); !strings.HasSuffix(got, "create pair can_write on CustomReports; delete pair can_write on CustomReports; delete menu CustomReports") {
		t.Fatalf("actions: %q, want the destroy to delete the pair and the view menu it created", got)
	}
}

func TestAccPermissionViewResourceExistingViewMenu(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	f := newFakePermissionViews()
	f.serve()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: permissionViewConfig("can_read", "Existing"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_permission_view.test", "view_menu_id", "50"),
					resource.TestCheckResourceAttr("superset_permission_view.test", "created_view_menu", "false"),
				),
			},
			{
				// A pair that exists already is not taken over.
				Config:      permissionViewConfig("can_write", "Existing"),
				ExpectError: regexp.MustCompile(`(?s)Permission "can_write" on view menu "Existing" already exists with id\s+300:\s+import it instead`),
			},
			{
				Config:      permissionViewConfig("can_fly", "Existing"),
				ExpectError: regexp.MustCompile(`(?s)Permission "can_fly" does not exist`),
			},
		},
	})
	if got := f.actions(); got != "create pair can_read on Existing; delete pair can_read on Existing" {
		t.Fatalf("actions: %q, want the existing view menu to be left alone", got)
	}
}
