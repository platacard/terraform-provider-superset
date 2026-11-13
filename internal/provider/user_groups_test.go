package provider

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
	"github.com/jarcoal/httpmock"
)

// userConfig is a user with the given extra attributes.
func userConfig(attributes string) string {
	return providerConfig + `
resource "superset_user" "test" {
  username   = "member.user"
  first_name = "Member"
  last_name  = "User"
  email      = "member.user@example.com"
  password   = "MemberPass123!"
  roles      = [4]
` + attributes + `
}
`
}

// fakeUser serves the user endpoints of the fake Superset host for one user with id 7 and
// keeps its groups across requests, as Superset 6 does. A legacy fake has no groups: it does
// not return them and rejects them in a write, as Superset 3.1 does.
type fakeUser struct {
	legacy     bool
	inactive   bool
	groups     []int64
	groupsSent bool
	sent       []int64
}

func (f *fakeUser) serve() {
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/csrf_token/",
		httpmock.NewStringResponder(200, `{"result": "fake-csrf-token"}`))
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/users/", f.write(201, `{"id": 7}`))
	httpmock.RegisterResponder("PUT", "http://superset-host/api/v1/security/users/7", f.write(200, `{}`))
	httpmock.RegisterResponder("DELETE", "http://superset-host/api/v1/security/users/7",
		httpmock.NewStringResponder(204, ""))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/users/7",
		func(*http.Request) (*http.Response, error) {
			groups := ""
			if !f.legacy {
				items := make([]string, 0, len(f.groups))
				for _, id := range f.groups {
					items = append(items, fmt.Sprintf(`{"id": %d, "name": "group-%d"}`, id, id))
				}
				groups = fmt.Sprintf(`, "groups": [%s]`, joinStrings(items))
			}
			return httpmock.NewJsonResponse(200, json.RawMessage(fmt.Sprintf(`{"result": {
				"id": 7, "username": "member.user", "first_name": "Member", "last_name": "User",
				"email": "member.user@example.com", "active": %t, "roles": [{"id": 4, "name": "Gamma"}]%s}}`, !f.inactive, groups)))
		})
}

// write records whether a create or update carried the groups and applies them.
func (f *fakeUser) write(status int, body string) httpmock.Responder {
	return func(req *http.Request) (*http.Response, error) {
		var payload struct {
			Active bool     `json:"active"`
			Groups *[]int64 `json:"groups"`
		}
		if err := json.NewDecoder(req.Body).Decode(&payload); err != nil {
			return httpmock.NewStringResponse(400, err.Error()), nil
		}
		f.inactive = !payload.Active
		f.groupsSent = payload.Groups != nil
		f.sent = nil
		if payload.Groups == nil {
			return httpmock.NewStringResponse(status, body), nil
		}
		if f.legacy {
			return httpmock.NewStringResponse(400, `{"message": {"groups": ["Unknown field."]}}`), nil
		}
		f.sent = *payload.Groups
		f.groups = *payload.Groups
		return httpmock.NewStringResponse(status, body), nil
	}
}

// join makes the user a member of exactly the given groups, as the groups would.
func (f *fakeUser) join(groups ...int64) {
	f.groups = groups
}

func joinStrings(items []string) string {
	result := ""
	for i, item := range items {
		if i > 0 {
			result += ", "
		}
		result += item
	}
	return result
}

func TestAccUserResourceGroups(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeUser{}
	fake.serve()
	// groupsSent checks whether the last write carried the groups, and how many.
	groupsSent := func(want bool, count int) resource.TestCheckFunc {
		return func(*terraform.State) error {
			if fake.groupsSent != want || (want && len(fake.sent) != count) {
				return fmt.Errorf("groups sent: %v %v, want %v with %d", fake.groupsSent, fake.sent, want, count)
			}
			return nil
		}
	}

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: userConfig(`  groups = [1, 2]`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_user.test", "groups.#", "2"),
					resource.TestCheckTypeSetElemAttr("superset_user.test", "groups.*", "1"),
					resource.TestCheckTypeSetElemAttr("superset_user.test", "groups.*", "2"),
					groupsSent(true, 2),
				),
			},
			{
				ResourceName:            "superset_user.test",
				ImportState:             true,
				ImportStateVerify:       true,
				ImportStateVerifyIgnore: []string{"last_updated", "password"},
			},
			{
				// Memberships made by the groups are left alone when the groups are not
				// configured, and an update does not send them.
				PreConfig: func() { fake.join(3) },
				Config:    userConfig(`  active = false`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_user.test", "active", "false"),
					resource.TestCheckResourceAttr("superset_user.test", "groups.#", "1"),
					resource.TestCheckTypeSetElemAttr("superset_user.test", "groups.*", "3"),
					groupsSent(false, 0),
				),
			},
			{
				PreConfig: func() { fake.join(3, 4) },
				Config:    userConfig(`  active = false`),
				PlanOnly:  true,
			},
			{
				// An empty set removes the user from every group.
				Config: userConfig(`  groups = []`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_user.test", "groups.#", "0"),
					groupsSent(true, 0),
				),
			},
		},
	})
}

func TestAccUserResourceGroupsOnLegacySuperset(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeUser{legacy: true}
	fake.serve()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config:      userConfig(`  groups = [1]`),
				ExpectError: regexp.MustCompile("Unknown field"),
			},
			{
				// Without the attribute nothing is sent, and the state holds no groups.
				Config: userConfig(``),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttrSet("superset_user.test", "id"),
					attributeAbsent("superset_user.test", "groups.#"),
				),
			},
			{
				Config: userConfig(`  active = false`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_user.test", "active", "false"),
					attributeAbsent("superset_user.test", "groups.#"),
				),
			},
		},
	})
}

// attributeAbsent checks that the state has no value at all for the attribute, which for a
// collection means null rather than empty.
func attributeAbsent(name, key string) resource.TestCheckFunc {
	return func(state *terraform.State) error {
		rs, ok := state.RootModule().Resources[name]
		if !ok {
			return fmt.Errorf("%s not found in the state", name)
		}
		if value, ok := rs.Primary.Attributes[key]; ok {
			return fmt.Errorf("%s: %s is %q, want it absent", name, key, value)
		}
		return nil
	}
}
