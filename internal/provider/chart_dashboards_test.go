package provider

import (
	"fmt"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
	"github.com/jarcoal/httpmock"
)

// chartConfig is a chart with the given extra attributes.
func chartConfig(attributes string) string {
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

func TestAccChartResourceDashboards(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeChart{}
	fake.serve()
	// dashboardsSent checks whether the last request carried the dashboards, and how many.
	dashboardsSent := func(want bool, count int) resource.TestCheckFunc {
		return func(*terraform.State) error {
			sent, linked := fake.sentDashboards()
			if sent != want || (sent && len(linked) != count) {
				return fmt.Errorf("dashboards sent: %v %v, want %v with %d", sent, linked, want, count)
			}
			return nil
		}
	}

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: chartConfig(`  dashboards = [3, 4]`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_chart.test", "dashboards.#", "2"),
					resource.TestCheckTypeSetElemAttr("superset_chart.test", "dashboards.*", "3"),
					resource.TestCheckTypeSetElemAttr("superset_chart.test", "dashboards.*", "4"),
					dashboardsSent(true, 2),
				),
			},
			{
				ResourceName:            "superset_chart.test",
				ImportState:             true,
				ImportStateVerify:       true,
				ImportStateVerifyIgnore: []string{"params"},
			},
			{
				// Links made by the layout of a dashboard are left alone when the dashboards
				// are not configured, and an update does not send them.
				PreConfig: func() { fake.link(5) },
				Config:    chartConfig(`  description = "Signups per day"`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_chart.test", "description", "Signups per day"),
					resource.TestCheckResourceAttr("superset_chart.test", "dashboards.#", "1"),
					resource.TestCheckTypeSetElemAttr("superset_chart.test", "dashboards.*", "5"),
					dashboardsSent(false, 0),
				),
			},
			{
				PreConfig: func() { fake.link(5, 6) },
				Config:    chartConfig(`  description = "Signups per day"`),
				PlanOnly:  true,
			},
			{
				// An empty list unlinks the chart from every dashboard.
				Config: chartConfig(`  dashboards = []`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_chart.test", "dashboards.#", "0"),
					dashboardsSent(true, 0),
				),
			},
		},
	})
}
