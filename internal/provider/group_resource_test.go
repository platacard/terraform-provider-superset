package provider

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/jarcoal/httpmock"
)

func TestAccGroupResource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	// Track group state for mocking
	var groupName = "Data Team"
	var groupRoles = []map[string]interface{}{{"id": 1, "name": "Alpha"}}
	var groupUsers = []map[string]interface{}{{"id": 10, "username": "user1"}}

	// Mock login
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))

	// Mock create group
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/group/",
		httpmock.NewStringResponder(201, `{"id": 50}`))

	// Mock get group (dynamic)
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/group/50",
		func(req *http.Request) (*http.Response, error) {
			rolesJSON, _ := json.Marshal(groupRoles)
			usersJSON, _ := json.Marshal(groupUsers)
			resp := httpmock.NewStringResponse(200, fmt.Sprintf(`{
				"result": {
					"id": 50,
					"name": "%s",
					"roles": %s,
					"users": %s
				}
			}`, groupName, string(rolesJSON), string(usersJSON)))
			resp.Header.Set("Content-Type", "application/json")
			return resp, nil
		})

	// Mock update group (updates state)
	httpmock.RegisterResponder("PUT", "http://superset-host/api/v1/security/group/50",
		func(req *http.Request) (*http.Response, error) {
			var updateData map[string]interface{}
			if err := json.NewDecoder(req.Body).Decode(&updateData); err == nil {
				if n, ok := updateData["name"].(string); ok {
					groupName = n
				}
				if r, ok := updateData["roles"].([]interface{}); ok {
					groupRoles = nil
					for _, v := range r {
						if id, ok := v.(float64); ok {
							groupRoles = append(groupRoles, map[string]interface{}{"id": int(id), "name": "role"})
						}
					}
				}
				if u, ok := updateData["users"].([]interface{}); ok {
					groupUsers = nil
					for _, v := range u {
						if id, ok := v.(float64); ok {
							groupUsers = append(groupUsers, map[string]interface{}{"id": int(id), "username": "user"})
						}
					}
				}
			}
			return httpmock.NewStringResponse(200, `{}`), nil
		})

	// Mock delete group
	httpmock.RegisterResponder("DELETE", "http://superset-host/api/v1/security/group/50",
		httpmock.NewStringResponder(204, ""))

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			// Create and Read testing
			{
				Config: providerConfig + testAccGroupResourceConfig,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_group.test_group", "name", "Data Team"),
					resource.TestCheckResourceAttr("superset_group.test_group", "roles.#", "1"),
					resource.TestCheckResourceAttr("superset_group.test_group", "roles.0", "1"),
					resource.TestCheckResourceAttr("superset_group.test_group", "users.#", "1"),
					resource.TestCheckResourceAttr("superset_group.test_group", "users.0", "10"),
					resource.TestCheckResourceAttrSet("superset_group.test_group", "id"),
					resource.TestCheckResourceAttrSet("superset_group.test_group", "last_updated"),
				),
			},
			// ImportState testing
			{
				ResourceName:            "superset_group.test_group",
				ImportState:             true,
				ImportStateVerify:       true,
				ImportStateVerifyIgnore: []string{"last_updated"},
			},
			// Update and Read testing
			{
				Config: providerConfig + testAccGroupResourceConfigUpdated,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_group.test_group", "name", "Updated Team"),
					resource.TestCheckResourceAttr("superset_group.test_group", "roles.#", "2"),
					resource.TestCheckResourceAttr("superset_group.test_group", "roles.0", "1"),
					resource.TestCheckResourceAttr("superset_group.test_group", "roles.1", "2"),
					resource.TestCheckResourceAttr("superset_group.test_group", "users.#", "1"),
					resource.TestCheckResourceAttr("superset_group.test_group", "users.0", "10"),
				),
			},
		},
	})
}

const testAccGroupResourceConfig = `
resource "superset_group" "test_group" {
  name  = "Data Team"
  roles = [1]
  users = [10]
}
`

const testAccGroupResourceConfigUpdated = `
resource "superset_group" "test_group" {
  name  = "Updated Team"
  roles = [1, 2]
  users = [10]
}
`
