package provider

import (
	"errors"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
	"github.com/jarcoal/httpmock"
)

// The objects Superset can mark as managed externally: the flag is written and read back, the
// external url is written but never returned, so it is kept from the configuration, and the
// uuid is read.

const externalURL = "https://git.example.com/analytics/superset.tf"

const managedAttributes = `
  is_managed_externally = true
  external_url          = "` + externalURL + `"
`

// managedChecks checks the attributes of a managed object, and that the request carried the url.
func managedChecks(name, uuid string, sent func(string) any) resource.TestCheckFunc {
	return resource.ComposeAggregateTestCheckFunc(
		resource.TestCheckResourceAttr(name, "is_managed_externally", "true"),
		resource.TestCheckResourceAttr(name, "external_url", externalURL),
		resource.TestCheckResourceAttr(name, "uuid", uuid),
		func(*terraform.State) error {
			if sent("is_managed_externally") != true || sent("external_url") != externalURL {
				return errors.New("the flag and the external url must be sent")
			}
			return nil
		},
	)
}

// unmanagedChecks checks the attributes once the object is no longer managed, and that the
// cleared url was sent as null.
func unmanagedChecks(name string, sent func(string) any) resource.TestCheckFunc {
	return resource.ComposeAggregateTestCheckFunc(
		resource.TestCheckResourceAttr(name, "is_managed_externally", "false"),
		resource.TestCheckNoResourceAttr(name, "external_url"),
		func(*terraform.State) error {
			if sent("is_managed_externally") != false || sent("external_url") != nil {
				return errors.New("the cleared flag and url must be sent as false and null")
			}
			return nil
		},
	)
}

// importStep imports the object: the url is not part of what Superset returns.
func importStep(name string, ignore ...string) resource.TestStep {
	return resource.TestStep{
		ResourceName:            name,
		ImportState:             true,
		ImportStateVerify:       true,
		ImportStateVerifyIgnore: append([]string{"external_url"}, ignore...),
	}
}

func TestAccChartResourceManagedExternally(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeChart{}
	fake.serve()
	sent := func(key string) any {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		return fake.chart[key]
	}
	config := func(attributes string) string {
		return providerConfig + `
resource "superset_chart" "test" {
  name          = "Daily signups"
  datasource_id = 3
  viz_type      = "table"
  params        = jsonencode({ viz_type = "table" })
` + attributes + `
}
`
	}

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{Config: config(managedAttributes), Check: managedChecks("superset_chart.test", "chart-uuid-7", sent)},
			importStep("superset_chart.test", "params"),
			{Config: config(""), Check: unmanagedChecks("superset_chart.test", sent)},
		},
	})
}

func TestAccDashboardResourceManagedExternally(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeDashboard{}
	fake.serve()
	config := func(attributes string) string {
		return providerConfig + `
resource "superset_dashboard" "test" {
  title = "Signups"
` + attributes + `
}
`
	}

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{Config: config(managedAttributes), Check: managedChecks("superset_dashboard.test", "dashboard-uuid-3", fake.sent)},
			importStep("superset_dashboard.test"),
			{Config: config(""), Check: unmanagedChecks("superset_dashboard.test", fake.sent)},
		},
	})
}

func TestAccDatasetResourceManagedExternally(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeDataset{}
	fake.serve()
	config := func(attributes string) string {
		return providerConfig + `
resource "superset_dataset" "test" {
  table_name    = "sales"
  database_name = "PostgreSQL Database"
` + attributes + `
}
`
	}

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{Config: config(managedAttributes), Check: managedChecks("superset_dataset.test", "dataset-uuid-123", fake.sent)},
			importStep("superset_dataset.test"),
			{Config: config(""), Check: unmanagedChecks("superset_dataset.test", fake.sent)},
		},
	})
}

func TestAccDatabaseResourceManagedExternally(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeDatabase{}
	fake.serve()
	config := func(attributes string) string {
		return providerConfig + `
resource "superset_database" "test" {
  connection_name = "DWH"
  db_engine       = "postgresql"
  db_user         = "superset_user"
  db_pass         = "dbpassword"
  db_host         = "pg.internal"
  db_port         = 5432
  db_name         = "superset_db"
` + databaseFlags + attributes + `
}
`
	}

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{Config: config(managedAttributes), Check: managedChecks("superset_database.test", "database-uuid-208", fake.sent)},
			importStep("superset_database.test", "db_pass"),
			{Config: config(""), Check: unmanagedChecks("superset_database.test", fake.sent)},
		},
	})
}
