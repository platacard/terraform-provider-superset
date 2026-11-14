package provider

import (
	"net/http"
	"regexp"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/jarcoal/httpmock"
)

func TestAccDatabaseCatalogsDataSource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	serveLookups()
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/database/3/catalogs/",
		func(req *http.Request) (*http.Response, error) {
			if strings.Contains(req.URL.Query().Get("q"), "force:!t") {
				return httpmock.NewStringResponse(200, `{"result": ["pgdb", "postgres", "fresh"]}`), nil
			}
			return httpmock.NewStringResponse(200, `{"result": ["postgres", "pgdb"]}`), nil
		})

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
data "superset_database_catalogs" "cached" {
  database_name = "Warehouse"
}

data "superset_database_catalogs" "fresh" {
  database_name = "Warehouse"
  force         = true
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.superset_database_catalogs.cached", "database_id", "3"),
					resource.TestCheckResourceAttr("data.superset_database_catalogs.cached", "catalogs.#", "2"),
					resource.TestCheckResourceAttr("data.superset_database_catalogs.cached", "catalogs.0", "pgdb"),
					resource.TestCheckResourceAttr("data.superset_database_catalogs.cached", "catalogs.1", "postgres"),
					resource.TestCheckResourceAttr("data.superset_database_catalogs.fresh", "catalogs.#", "3"),
					resource.TestCheckResourceAttr("data.superset_database_catalogs.fresh", "catalogs.0", "fresh"),
				),
			},
			{
				Config: providerConfig + `
data "superset_database_catalogs" "missing" {
  database_name = "Nowhere"
}
`,
				ExpectError: regexp.MustCompile(`Nowhere`),
			},
		},
	})
}

// On a version without catalogs the endpoint does not exist; the lookup explains it.
func TestAccDatabaseCatalogsDataSourceOnLegacySuperset(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	serveLookups()
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/database/3/catalogs/", httpmock.NewStringResponder(404, missingRoute))

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
data "superset_database_catalogs" "test" {
  database_name = "Warehouse"
}
`,
				ExpectError: regexp.MustCompile(`(?s)need\s+Superset\s+6`),
			},
		},
	})
}
