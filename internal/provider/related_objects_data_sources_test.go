package provider

import (
	"regexp"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/jarcoal/httpmock"
)

const relatedObjectsBody = `{
  "charts": {"count": 2, "result": [{"id": 20, "slice_name": "Sales by day", "viz_type": "table"}, {"id": 21, "slice_name": "Sales by region", "viz_type": "pie"}]},
  "dashboards": {"count": 1, "result": [{"id": 30, "json_metadata": null, "slug": "sales", "title": "Sales"}]},
  "sqllab_tab_states": {"count": 0, "result": []}
}`

func serveRelatedObjects() {
	serveLookups()
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/dataset/10/related_objects",
		httpmock.NewStringResponder(200, relatedObjectsBody))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/dataset/12/related_objects",
		httpmock.NewStringResponder(200, `{"charts": {"count": 0, "result": []}, "dashboards": {"count": 0, "result": []}}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/dataset/99/related_objects",
		httpmock.NewStringResponder(404, `{"message": "Not found"}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/database/3/related_objects",
		httpmock.NewStringResponder(200, relatedObjectsBody))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/database/99/related_objects",
		httpmock.NewStringResponder(404, `{"message": "Not found"}`))
}

func TestAccDatasetRelatedObjectsDataSource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	serveRelatedObjects()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
data "superset_dataset_related_objects" "sales" {
  dataset_id = 10
}

data "superset_dataset_related_objects" "unused" {
  dataset_id = 12
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.superset_dataset_related_objects.sales", "chart_ids.#", "2"),
					resource.TestCheckResourceAttr("data.superset_dataset_related_objects.sales", "chart_ids.0", "20"),
					resource.TestCheckResourceAttr("data.superset_dataset_related_objects.sales", "chart_ids.1", "21"),
					resource.TestCheckResourceAttr("data.superset_dataset_related_objects.sales", "charts.1.name", "Sales by region"),
					resource.TestCheckResourceAttr("data.superset_dataset_related_objects.sales", "charts.1.viz_type", "pie"),
					resource.TestCheckResourceAttr("data.superset_dataset_related_objects.sales", "dashboard_ids.#", "1"),
					resource.TestCheckResourceAttr("data.superset_dataset_related_objects.sales", "dashboard_ids.0", "30"),
					resource.TestCheckResourceAttr("data.superset_dataset_related_objects.sales", "dashboards.0.title", "Sales"),
					resource.TestCheckResourceAttr("data.superset_dataset_related_objects.sales", "dashboards.0.slug", "sales"),
					resource.TestCheckResourceAttr("data.superset_dataset_related_objects.unused", "chart_ids.#", "0"),
					resource.TestCheckResourceAttr("data.superset_dataset_related_objects.unused", "charts.#", "0"),
					resource.TestCheckResourceAttr("data.superset_dataset_related_objects.unused", "dashboards.#", "0"),
				),
			},
			{
				Config: providerConfig + `
data "superset_dataset_related_objects" "missing" {
  dataset_id = 99
}
`,
				ExpectError: regexp.MustCompile(`(?s)Could not read the related objects of dataset 99: .*not found`),
			},
		},
	})
}

func TestAccDatabaseRelatedObjectsDataSource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	serveRelatedObjects()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
data "superset_database_related_objects" "by_name" {
  database_name = "Warehouse"
}

data "superset_database_related_objects" "by_id" {
  database_id = 3
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.superset_database_related_objects.by_name", "database_id", "3"),
					resource.TestCheckResourceAttr("data.superset_database_related_objects.by_name", "chart_ids.#", "2"),
					resource.TestCheckResourceAttr("data.superset_database_related_objects.by_name", "dashboards.0.id", "30"),
					resource.TestCheckResourceAttr("data.superset_database_related_objects.by_id", "chart_ids.1", "21"),
					resource.TestCheckNoResourceAttr("data.superset_database_related_objects.by_id", "database_name"),
				),
			},
			{
				Config: providerConfig + `
data "superset_database_related_objects" "both" {
  database_name = "Warehouse"
  database_id   = 3
}
`,
				ExpectError: regexp.MustCompile(`Set exactly one of database_name and database_id`),
			},
			{
				Config: providerConfig + `
data "superset_database_related_objects" "neither" {}
`,
				ExpectError: regexp.MustCompile(`Set exactly one of database_name and database_id`),
			},
			{
				Config: providerConfig + `
data "superset_database_related_objects" "unknown_name" {
  database_name = "Nowhere"
}
`,
				ExpectError: regexp.MustCompile(`Could not look up database "Nowhere"`),
			},
			{
				Config: providerConfig + `
data "superset_database_related_objects" "missing" {
  database_id = 99
}
`,
				ExpectError: regexp.MustCompile(`(?s)Could not read the related objects of database 99: .*not found`),
			},
		},
	})
}
