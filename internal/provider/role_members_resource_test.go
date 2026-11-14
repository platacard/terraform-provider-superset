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

// fakeRoleMembers serves role 5, its member endpoints and the user and group lists that
// reflect what the member endpoints were last given. Users 7 and 8 and groups 2 and 3 exist.
type fakeRoleMembers struct {
	mu     sync.Mutex
	users  []int64
	groups []int64
	puts   []string
}

func (f *fakeRoleMembers) serve() {
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/csrf_token/",
		httpmock.NewStringResponder(200, `{"result": "fake-csrf-token"}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/roles/5",
		httpmock.NewStringResponder(200, `{"id": 5, "result": {"id": 5, "name": "Analysts"}}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/roles/99",
		httpmock.NewStringResponder(404, `{"message": "Not found"}`))
	for _, kind := range []string{"users", "groups"} {
		httpmock.RegisterResponder("PUT", "http://superset-host/api/v1/security/roles/5/"+kind, func(req *http.Request) (*http.Response, error) {
			body, _ := io.ReadAll(req.Body)
			var payload map[string][]int64
			if err := json.Unmarshal(body, &payload); err != nil {
				return httpmock.NewStringResponse(400, "bad json"), nil
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			ids := payload["user_ids"]
			if kind == "groups" {
				ids = payload["group_ids"]
			}
			for _, id := range ids {
				if id > 10 {
					return httpmock.NewStringResponse(404, `{"message": "Not found"}`), nil
				}
			}
			if kind == "users" {
				f.users = ids
			} else {
				f.groups = ids
			}
			f.puts = append(f.puts, fmt.Sprintf("%s=%v", kind, ids))
			return httpmock.NewStringResponse(200, string(body)), nil
		})
		httpmock.RegisterResponder("PUT", "http://superset-host/api/v1/security/roles/99/"+kind,
			httpmock.NewStringResponder(404, `{"message": "Not found"}`))
	}
	httpmock.RegisterResponder("GET", listEndpoint("/api/v1/security/users/"), func(req *http.Request) (*http.Response, error) {
		if !strings.Contains(req.URL.Query().Get("q"), "(col:roles,opr:rel_m_m,value:5)") {
			return httpmock.NewStringResponse(400, "the users must be filtered on the role"), nil
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		rows := []string{}
		for _, id := range f.users {
			rows = append(rows, fmt.Sprintf(`{"id": %d, "username": "user%d", "roles": [{"id": 5, "name": "Analysts"}, {"id": 4, "name": "Gamma"}]}`, id, id))
		}
		return httpmock.NewStringResponse(200, fmt.Sprintf(`{"count": %d, "result": [%s]}`, len(rows), strings.Join(rows, ","))), nil
	})
	httpmock.RegisterResponder("GET", listEndpoint("/api/v1/security/groups/"), func(*http.Request) (*http.Response, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		rows := []string{}
		for _, id := range []int64{2, 3} {
			roles := ""
			for _, member := range f.groups {
				if member == id {
					roles = `{"id": 5, "name": "Analysts"}`
				}
			}
			rows = append(rows, fmt.Sprintf(`{"id": %d, "name": "group%d", "label": null, "description": null, "roles": [%s], "users": []}`, id, id, roles))
		}
		return httpmock.NewStringResponse(200, fmt.Sprintf(`{"count": %d, "result": [%s]}`, len(rows), strings.Join(rows, ","))), nil
	})
}

func (f *fakeRoleMembers) sent() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return strings.Join(f.puts, " ")
}

func roleMembersConfig(role int64, lists string) string {
	return providerConfig + fmt.Sprintf(`
resource "superset_role_members" "test" {
  role_id = %d
%s}
`, role, lists)
}

func TestAccRoleMembersResource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	f := &fakeRoleMembers{}
	f.serve()
	sent := func(want string) resource.TestCheckFunc {
		return func(*terraform.State) error {
			if got := f.sent(); got != want {
				return fmt.Errorf("member requests sent: %q, want %q", got, want)
			}
			return nil
		}
	}

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: roleMembersConfig(5, `  user_ids  = [8, 7]
  group_ids = [2]
`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_role_members.test", "id", "5"),
					resource.TestCheckResourceAttr("superset_role_members.test", "user_ids.#", "2"),
					resource.TestCheckTypeSetElemAttr("superset_role_members.test", "user_ids.*", "7"),
					resource.TestCheckTypeSetElemAttr("superset_role_members.test", "user_ids.*", "8"),
					resource.TestCheckResourceAttr("superset_role_members.test", "group_ids.#", "1"),
					resource.TestCheckTypeSetElemAttr("superset_role_members.test", "group_ids.*", "2"),
					sent("users=[7 8] groups=[2]"),
				),
			},
			{
				// A member added outside Terraform is removed on the next apply.
				PreConfig: func() {
					f.mu.Lock()
					f.users = append(f.users, 9)
					f.mu.Unlock()
				},
				Config: roleMembersConfig(5, `  user_ids  = [8, 7]
  group_ids = [2]
`),
				ConfigPlanChecks: resource.ConfigPlanChecks{
					PreApply: []plancheck.PlanCheck{plancheck.ExpectResourceAction("superset_role_members.test", plancheck.ResourceActionUpdate)},
				},
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_role_members.test", "user_ids.#", "2"),
					sent("users=[7 8] groups=[2] users=[7 8]"),
				),
			},
			{
				// Only the list that changed is sent; an empty list removes every member.
				Config: roleMembersConfig(5, `  user_ids  = []
  group_ids = [2]
`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_role_members.test", "user_ids.#", "0"),
					resource.TestCheckResourceAttr("superset_role_members.test", "group_ids.#", "1"),
					sent("users=[7 8] groups=[2] users=[7 8] users=[]"),
				),
			},
			{
				// A list that is no longer set is left alone and becomes null.
				PreConfig: func() {
					f.mu.Lock()
					f.groups = []int64{2, 3}
					f.mu.Unlock()
				},
				Config: roleMembersConfig(5, `  user_ids = [7]
`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_role_members.test", "user_ids.#", "1"),
					resource.TestCheckNoResourceAttr("superset_role_members.test", "group_ids"),
					sent("users=[7 8] groups=[2] users=[7 8] users=[] users=[7]"),
					func(*terraform.State) error {
						f.mu.Lock()
						defer f.mu.Unlock()
						if len(f.groups) != 2 {
							return fmt.Errorf("the groups were changed although they are no longer managed: %v", f.groups)
						}
						return nil
					},
				),
			},
			{
				// An unknown user fails the apply.
				Config: roleMembersConfig(5, `  user_ids = [7, 42]
`),
				ExpectError: regexp.MustCompile(`(?s)Could not set the users of role 5: .*not\s+found`),
			},
		},
	})
	// The destroy of the last step removed the users the resource managed, not the groups.
	if got := f.sent(); !strings.HasSuffix(got, " users=[]") || strings.HasSuffix(got, "groups=[]") {
		t.Fatalf("member requests sent: %q, want the destroy to remove the managed users only", got)
	}
}

func TestAccRoleMembersResourceMissingRole(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	f := &fakeRoleMembers{}
	f.serve()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config:      roleMembersConfig(99, "  user_ids = [7]\n"),
				ExpectError: regexp.MustCompile(`(?s)Could not set the users of role 99: .*not\s+found`),
			},
		},
	})
}

// On a version without the member endpoints the apply explains that they need Superset 6.
func TestAccRoleMembersResourceOnLegacySuperset(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/csrf_token/",
		httpmock.NewStringResponder(200, `{"result": "fake-csrf-token"}`))
	httpmock.RegisterResponder("PUT", "http://superset-host/api/v1/security/roles/5/users", httpmock.NewStringResponder(404, missingRoute))

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config:      roleMembersConfig(5, "  user_ids = [7]\n"),
				ExpectError: regexp.MustCompile(`need Superset 6`),
			},
		},
	})
}
