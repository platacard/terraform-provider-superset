package provider

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/jarcoal/httpmock"
)

func TestAccUserResource(t *testing.T) {
	// Activate httpmock
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	// Track user state for mocking
	var userLastName = "User"
	var userEmail = "test.user@example.com"

	// Mock the Superset API login response
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))

	// Mock the CSRF token endpoint (required for all write operations)
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/csrf_token/",
		httpmock.NewStringResponder(200, `{"result": "fake-csrf-token"}`))

	// Mock the Superset API response for creating users
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/users/",
		httpmock.NewStringResponder(201, `{"id": 100}`))

	// Mock the Superset API response for reading users by ID (dynamic based on state)
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/users/100",
		func(req *http.Request) (*http.Response, error) {
			resp := httpmock.NewStringResponse(200, fmt.Sprintf(`{
				"result": {
					"id": 100,
					"username": "test.user",
					"first_name": "Test",
					"last_name": "%s",
					"email": "%s",
					"active": true,
					"roles": [
						{"id": 4, "name": "Gamma"}
					]
				}
			}`, userLastName, userEmail))
			resp.Header.Set("Content-Type", "application/json")
			return resp, nil
		})

	// Mock the Superset API response for updating users (updates state)
	httpmock.RegisterResponder("PUT", "http://superset-host/api/v1/security/users/100",
		func(req *http.Request) (*http.Response, error) {
			// Parse the request body to update our mock state
			var updateData map[string]interface{}
			if err := json.NewDecoder(req.Body).Decode(&updateData); err == nil {
				if ln, ok := updateData["last_name"].(string); ok {
					userLastName = ln
				}
				if em, ok := updateData["email"].(string); ok {
					userEmail = em
				}
			}
			return httpmock.NewStringResponse(200, `{}`), nil
		})

	// Mock the Superset API response for deleting users
	httpmock.RegisterResponder("DELETE", "http://superset-host/api/v1/security/users/100",
		httpmock.NewStringResponder(204, ""))

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			// Create and Read testing
			{
				Config: providerConfig + testAccUserResourceConfig,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_user.test_user", "username", "test.user"),
					resource.TestCheckResourceAttr("superset_user.test_user", "first_name", "Test"),
					resource.TestCheckResourceAttr("superset_user.test_user", "last_name", "User"),
					resource.TestCheckResourceAttr("superset_user.test_user", "email", "test.user@example.com"),
					resource.TestCheckResourceAttr("superset_user.test_user", "active", "true"),
					resource.TestCheckResourceAttr("superset_user.test_user", "roles.#", "1"),
					resource.TestCheckResourceAttr("superset_user.test_user", "roles.0", "4"),
					resource.TestCheckResourceAttrSet("superset_user.test_user", "id"),
					resource.TestCheckResourceAttrSet("superset_user.test_user", "last_updated"),
				),
			},
			// ImportState testing
			{
				ResourceName:            "superset_user.test_user",
				ImportState:             true,
				ImportStateVerify:       true,
				ImportStateVerifyIgnore: []string{"last_updated", "password"},
			},
			// Update and Read testing
			{
				Config: providerConfig + testAccUserResourceConfigUpdated,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_user.test_user", "username", "test.user"),
					resource.TestCheckResourceAttr("superset_user.test_user", "first_name", "Test"),
					resource.TestCheckResourceAttr("superset_user.test_user", "last_name", "UpdatedUser"),
					resource.TestCheckResourceAttr("superset_user.test_user", "email", "test.user.updated@example.com"),
					resource.TestCheckResourceAttr("superset_user.test_user", "active", "true"),
					resource.TestCheckResourceAttr("superset_user.test_user", "roles.#", "1"),
					resource.TestCheckResourceAttr("superset_user.test_user", "roles.0", "4"),
				),
			},
			// Delete testing automatically occurs in TestCase
		},
	})
}

const testAccUserResourceConfig = `
resource "superset_user" "test_user" {
  username   = "test.user"
  first_name = "Test"
  last_name  = "User"
  email      = "test.user@example.com"
  password   = "S0meStr0ngPass!"
  active     = true
  roles      = [4]
}
`

const testAccUserResourceConfigUpdated = `
resource "superset_user" "test_user" {
  username   = "test.user"
  first_name = "Test"
  last_name  = "UpdatedUser"
  email      = "test.user.updated@example.com"
  active     = true
  roles      = [4]
}
`
