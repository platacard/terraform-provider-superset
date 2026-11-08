package provider

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/jarcoal/httpmock"
)

// loginRecorder serves the login endpoint of the fake Superset host and remembers the
// authentication provider of the last login request.
type loginRecorder struct {
	mu       sync.Mutex
	provider string
}

func (l *loginRecorder) serve() {
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		func(req *http.Request) (*http.Response, error) {
			var payload struct {
				Provider string `json:"provider"`
			}
			_ = json.NewDecoder(req.Body).Decode(&payload)
			l.mu.Lock()
			l.provider = payload.Provider
			l.mu.Unlock()
			return httpmock.NewStringResponse(200, `{"access_token": "fake-token"}`), nil
		})
	httpmock.RegisterResponder("GET", listEndpoint("/api/v1/security/roles/"),
		httpmock.NewStringResponder(200, `{"count": 1, "result": [{"id": 1, "name": "Admin"}]}`))
}

func (l *loginRecorder) last() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.provider
}

const roleLookup = `
data "superset_role" "admin" {
  name = "Admin"
}
`

func TestAccProviderAuthProvider(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	recorder := &loginRecorder{}
	recorder.serve()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + roleLookup,
				Check: resource.TestCheckResourceAttrWith("data.superset_role.admin", "id", func(string) error {
					if recorder.last() != "db" {
						return fmt.Errorf("the login must name the db provider by default, got %q", recorder.last())
					}
					return nil
				}),
			},
			{
				Config: `
provider "superset" {
  host          = "http://superset-host"
  username      = "fake-username"
  password      = "fake-password"
  auth_provider = "ldap"
}
` + roleLookup,
				Check: resource.TestCheckResourceAttrWith("data.superset_role.admin", "id", func(string) error {
					if recorder.last() != "ldap" {
						return fmt.Errorf("the login must name the ldap provider, got %q", recorder.last())
					}
					return nil
				}),
			},
		},
	})
}

func TestAccProviderAuthProviderFromEnvironment(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	recorder := &loginRecorder{}
	recorder.serve()
	t.Setenv("SUPERSET_AUTH_PROVIDER", "ldap")

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + roleLookup,
				Check: resource.TestCheckResourceAttrWith("data.superset_role.admin", "id", func(string) error {
					if recorder.last() != "ldap" {
						return fmt.Errorf("the environment must select the ldap provider, got %q", recorder.last())
					}
					return nil
				}),
			},
		},
	})
}
