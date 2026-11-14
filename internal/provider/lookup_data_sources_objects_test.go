package provider

import (
	"regexp"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/jarcoal/httpmock"
)

func TestAccSavedQueryDataSource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	serveLookups()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
data "superset_saved_query" "test" {
  label       = "Top customers"
  database_id = 4
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.superset_saved_query.test", "id", "41"),
					resource.TestCheckResourceAttr("data.superset_saved_query.test", "database_id", "4"),
					resource.TestCheckResourceAttr("data.superset_saved_query.test", "database_name", "Staging"),
					resource.TestCheckNoResourceAttr("data.superset_saved_query.test", "schema"),
					resource.TestCheckResourceAttr("data.superset_saved_query.test", "catalog", "pgdb"),
					resource.TestCheckResourceAttr("data.superset_saved_query.test", "sql", "SELECT 1"),
					resource.TestCheckNoResourceAttr("data.superset_saved_query.test", "description"),
				),
			},
			{
				Config: providerConfig + `
data "superset_saved_query" "ambiguous" {
  label = "Top customers"
}
`,
				ExpectError: regexp.MustCompile(`2 saved queries are named "Top customers" \(ids 40, 41\); set database_id`),
			},
			{
				Config: providerConfig + `
data "superset_saved_query" "missing" {
  label = "top customers"
}
`,
				ExpectError: regexp.MustCompile(`Could not look up saved query "top customers": not found`),
			},
		},
	})
}

func TestAccAnnotationLayerDataSource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	serveLookups()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
data "superset_annotation_layer" "test" {
  name = "Releases"
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.superset_annotation_layer.test", "id", "50"),
					resource.TestCheckResourceAttr("data.superset_annotation_layer.test", "description", "Production releases"),
				),
			},
			{
				Config: providerConfig + `
data "superset_annotation_layer" "ambiguous" {
  name = "Twice"
}
`,
				ExpectError: regexp.MustCompile(`2 annotation layers are named "Twice" \(ids 51, 52\)`),
			},
			{
				Config: providerConfig + `
data "superset_annotation_layer" "missing" {
  name = "releases"
}
`,
				ExpectError: regexp.MustCompile(`Could not look up annotation layer "releases": not found`),
			},
		},
	})
}

func TestAccTagDataSource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	serveLookups()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
data "superset_tag" "test" {
  name = "finance"
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.superset_tag.test", "id", "60"),
					resource.TestCheckResourceAttr("data.superset_tag.test", "description", "Finance team"),
				),
			},
			{
				Config: providerConfig + `
data "superset_tag" "missing" {
  name = "Finance"
}
`,
				ExpectError: regexp.MustCompile(`Could not look up tag "Finance": tag "Finance": not found`),
			},
		},
	})
}

// Without the TAGGING_SYSTEM feature flag the tag endpoint does not exist; the lookup explains it.
func TestAccTagDataSourceTaggingSystemDisabled(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	serveMissingRoute("/api/v1/tag/")

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
data "superset_tag" "test" {
  name = "finance"
}
`,
				ExpectError: regexp.MustCompile("TAGGING_SYSTEM"),
			},
		},
	})
}

func TestAccReportDataSource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	serveLookups()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
data "superset_report" "test" {
  name = "Weekly sales"
  type = "Alert"
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.superset_report.test", "id", "71"),
					resource.TestCheckResourceAttr("data.superset_report.test", "type", "Alert"),
					resource.TestCheckNoResourceAttr("data.superset_report.test", "description"),
					resource.TestCheckResourceAttr("data.superset_report.test", "crontab", "0 * * * *"),
					resource.TestCheckResourceAttr("data.superset_report.test", "timezone", "Europe/Berlin"),
					resource.TestCheckResourceAttr("data.superset_report.test", "active", "false"),
					resource.TestCheckResourceAttr("data.superset_report.test", "chart_id", "20"),
					resource.TestCheckNoResourceAttr("data.superset_report.test", "dashboard_id"),
					resource.TestCheckResourceAttr("data.superset_report.test", "last_state", "Not triggered"),
				),
			},
			{
				Config: providerConfig + `
data "superset_report" "ambiguous" {
  name = "Weekly sales"
}
`,
				ExpectError: regexp.MustCompile(`2 reports are named "Weekly sales" \(ids 70, 71\); set type`),
			},
			{
				Config: providerConfig + `
data "superset_report" "invalid" {
  name = "Weekly sales"
  type = "Dashboard"
}
`,
				ExpectError: regexp.MustCompile(`The type must be Report or Alert`),
			},
			{
				Config: providerConfig + `
data "superset_report" "missing" {
  name = "weekly sales"
}
`,
				ExpectError: regexp.MustCompile(`Could not look up report "weekly sales": not found`),
			},
		},
	})
}

// Without the ALERT_REPORTS feature flag the report endpoint does not exist; the lookup explains it.
func TestAccReportDataSourceAlertReportsDisabled(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	serveMissingRoute("/api/v1/report/")

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
data "superset_report" "test" {
  name = "Weekly sales"
}
`,
				ExpectError: regexp.MustCompile("ALERT_REPORTS"),
			},
		},
	})
}

func TestAccThemeDataSource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	serveLookups()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
data "superset_theme" "test" {
  name = "Corporate"
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.superset_theme.test", "id", "80"),
					resource.TestCheckResourceAttr("data.superset_theme.test", "uuid", "ad3b15d6-d271-410b-8781-c8e76073b594"),
					resource.TestCheckResourceAttr("data.superset_theme.test", "json_data", `{"token": {"colorPrimary": "#1f77b4"}}`),
					resource.TestCheckResourceAttr("data.superset_theme.test", "is_system", "false"),
					resource.TestCheckResourceAttr("data.superset_theme.test", "system_default", "true"),
					resource.TestCheckResourceAttr("data.superset_theme.test", "system_dark", "false"),
				),
			},
			{
				Config: providerConfig + `
data "superset_theme" "missing" {
  name = "corporate"
}
`,
				ExpectError: regexp.MustCompile(`Could not look up theme "corporate": not found`),
			},
		},
	})
}

// On a version without themes the endpoint does not exist; the lookup explains it.
func TestAccThemeDataSourceOnLegacySuperset(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	serveMissingRoute("/api/v1/theme/")

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
data "superset_theme" "test" {
  name = "Corporate"
}
`,
				ExpectError: regexp.MustCompile(`(?s)need\s+Superset\s+6`),
			},
		},
	})
}
