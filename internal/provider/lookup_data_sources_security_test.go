package provider

import (
	"regexp"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/jarcoal/httpmock"
)

// missingRoute is the 404 Superset answers for a route that does not exist on the instance.
const missingRoute = `{"errors": [{"message": "404 Not Found: The requested URL was not found on the server. If you entered the URL manually please check your spelling and try again.", "error_type": "GENERIC_BACKEND_ERROR", "level": "error"}]}`

// serveMissingRoute answers the login and a list endpoint that does not exist on the instance.
func serveMissingRoute(path string) {
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))
	httpmock.RegisterResponder("GET", listEndpoint(path), httpmock.NewStringResponder(404, missingRoute))
}

func TestAccUserDataSource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	serveLookups()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
data "superset_user" "test" {
  username = "analyst"
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.superset_user.test", "id", "7"),
					resource.TestCheckResourceAttr("data.superset_user.test", "first_name", "Ana"),
					resource.TestCheckResourceAttr("data.superset_user.test", "last_name", "Lyst"),
					resource.TestCheckResourceAttr("data.superset_user.test", "email", "analyst@example.com"),
					resource.TestCheckResourceAttr("data.superset_user.test", "active", "true"),
					resource.TestCheckResourceAttr("data.superset_user.test", "roles.#", "2"),
					resource.TestCheckResourceAttr("data.superset_user.test", "roles.0", "4"),
					resource.TestCheckResourceAttr("data.superset_user.test", "roles.1", "5"),
					resource.TestCheckResourceAttr("data.superset_user.test", "groups.#", "1"),
					resource.TestCheckTypeSetElemAttr("data.superset_user.test", "groups.*", "2"),
				),
			},
			{
				// A version without groups does not return them: the attribute is null.
				Config: providerConfig + `
data "superset_user" "legacy" {
  username = "legacy"
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.superset_user.legacy", "id", "8"),
					resource.TestCheckResourceAttr("data.superset_user.legacy", "active", "false"),
					resource.TestCheckResourceAttr("data.superset_user.legacy", "roles.#", "0"),
					resource.TestCheckNoResourceAttr("data.superset_user.legacy", "groups"),
				),
			},
			{
				Config: providerConfig + `
data "superset_user" "missing" {
  username = "Analyst"
}
`,
				ExpectError: regexp.MustCompile(`Could not look up user "Analyst": not found`),
			},
			{
				Config: providerConfig + `
data "superset_user" "empty" {
  username = ""
}
`,
				ExpectError: regexp.MustCompile(`The username must not be empty`),
			},
		},
	})
}

// Without FAB_ADD_SECURITY_API the users endpoint does not exist; the lookup explains it.
func TestAccUserDataSourceSecurityAPIDisabled(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	serveMissingRoute("/api/v1/security/users/")

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
data "superset_user" "test" {
  username = "analyst"
}
`,
				ExpectError: regexp.MustCompile("FAB_ADD_SECURITY_API"),
			},
		},
	})
}

func TestAccGroupDataSource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	serveLookups()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
data "superset_group" "test" {
  name = "analysts"
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.superset_group.test", "id", "2"),
					resource.TestCheckResourceAttr("data.superset_group.test", "label", "Analysts"),
					resource.TestCheckNoResourceAttr("data.superset_group.test", "description"),
					resource.TestCheckResourceAttr("data.superset_group.test", "roles.#", "1"),
					resource.TestCheckTypeSetElemAttr("data.superset_group.test", "roles.*", "4"),
					resource.TestCheckResourceAttr("data.superset_group.test", "users.#", "1"),
					resource.TestCheckTypeSetElemAttr("data.superset_group.test", "users.*", "7"),
				),
			},
			{
				Config: providerConfig + `
data "superset_group" "missing" {
  name = "Analysts"
}
`,
				ExpectError: regexp.MustCompile(`Could not look up group "Analysts": not found`),
			},
		},
	})
}

// On a version without groups the endpoint does not exist; the lookup explains it.
func TestAccGroupDataSourceOnLegacySuperset(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	serveMissingRoute("/api/v1/security/groups/")

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
data "superset_group" "test" {
  name = "analysts"
}
`,
				ExpectError: regexp.MustCompile(`need Superset 6`),
			},
		},
	})
}

func TestAccRowLevelSecurityDataSource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	serveLookups()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
data "superset_row_level_security" "test" {
  name = "Region"
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.superset_row_level_security.test", "id", "5"),
					resource.TestCheckResourceAttr("data.superset_row_level_security.test", "description", "Rows of the region of the user"),
					resource.TestCheckResourceAttr("data.superset_row_level_security.test", "filter_type", "Regular"),
					resource.TestCheckResourceAttr("data.superset_row_level_security.test", "group_key", "region"),
					resource.TestCheckResourceAttr("data.superset_row_level_security.test", "clause", "region = '{{ current_username() }}'"),
					resource.TestCheckResourceAttr("data.superset_row_level_security.test", "tables.#", "1"),
					resource.TestCheckTypeSetElemAttr("data.superset_row_level_security.test", "tables.*", "10"),
					resource.TestCheckResourceAttr("data.superset_row_level_security.test", "roles.#", "1"),
					resource.TestCheckTypeSetElemAttr("data.superset_row_level_security.test", "roles.*", "4"),
				),
			},
			{
				Config: providerConfig + `
data "superset_row_level_security" "missing" {
  name = "region"
}
`,
				ExpectError: regexp.MustCompile(`Could not look up row level security rule "region": not found`),
			},
		},
	})
}
