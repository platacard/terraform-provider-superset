package provider

import (
	"errors"
	"fmt"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
	"github.com/jarcoal/httpmock"
)

func taggedChart(tags string) string {
	return taggedChartNamed("Revenue", tags)
}

func taggedChartNamed(name, tags string) string {
	return providerConfig + `
resource "superset_chart" "test" {
  name          = "` + name + `"
  datasource_id = 5
  viz_type      = "table"
  params        = jsonencode({ viz_type = "table" })
` + tags + `
}
`
}

func TestAccChartResourceTags(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	tags := newFakeTags()
	tags.serve()
	fake := &fakeChart{tags: tags}
	fake.serve()

	counts := func(attaches, detaches int) resource.TestCheckFunc {
		return func(*terraform.State) error {
			tags.mu.Lock()
			defer tags.mu.Unlock()
			if tags.attaches != attaches || tags.detaches != detaches {
				return fmt.Errorf("got %d attaches and %d detaches, want %d and %d", tags.attaches, tags.detaches, attaches, detaches)
			}
			return nil
		}
	}

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				// Without tags, the ones Superset has are recorded and left alone: the system tag is not one of them.
				Config: taggedChart(""),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_chart.test", "tags.#", "0"),
					counts(0, 0),
				),
			},
			{
				// Setting tags attaches them, creating the ones that do not exist.
				Config: taggedChart(`  tags = ["finance", "weekly-review"]`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_chart.test", "tags.#", "2"),
					resource.TestCheckTypeSetElemAttr("superset_chart.test", "tags.*", "finance"),
					resource.TestCheckTypeSetElemAttr("superset_chart.test", "tags.*", "weekly-review"),
					counts(2, 0),
					func(*terraform.State) error {
						if tag := tags.tag("finance"); tag == nil || !tag.objects["chart:7"] {
							return errors.New("the finance tag must be created and attached to the chart")
						}
						return nil
					},
				),
			},
			{
				ResourceName:      "superset_chart.test",
				ImportState:       true,
				ImportStateVerify: true,
			},
			{
				// Only the difference is sent: one tag detached, one attached, the kept one untouched.
				Config: taggedChart(`  tags = ["weekly-review", "sales"]`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_chart.test", "tags.#", "2"),
					resource.TestCheckTypeSetElemAttr("superset_chart.test", "tags.*", "sales"),
					counts(3, 1),
					func(*terraform.State) error {
						if tag := tags.tag("finance"); tag == nil || tag.objects["chart:7"] {
							return errors.New("the finance tag must be detached but kept")
						}
						return nil
					},
				),
			},
			{
				// An update of another attribute does not touch the tags.
				Config: taggedChartNamed("Revenue per day", `  tags = ["weekly-review", "sales"]`),
				Check:  counts(3, 1),
			},
			{
				// Dropping the attribute leaves the tags as they are.
				Config:   taggedChartNamed("Revenue per day", ""),
				PlanOnly: true,
			},
			{
				// An empty set detaches every tag.
				Config: taggedChartNamed("Revenue per day", `  tags = []`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_chart.test", "tags.#", "0"),
					counts(3, 3),
				),
			},
		},
	})
}

func TestAccDashboardResourceTags(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	tags := newFakeTags()
	tags.serve()
	fake := &fakeDashboard{tags: tags}
	fake.serve()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
resource "superset_dashboard" "test" {
  title         = "Sales"
  json_metadata = jsonencode({ refresh_frequency = 30 })
  tags          = ["finance"]
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_dashboard.test", "tags.#", "1"),
					resource.TestCheckTypeSetElemAttr("superset_dashboard.test", "tags.*", "finance"),
					func(*terraform.State) error {
						if tag := tags.tag("finance"); tag == nil || !tag.objects["dashboard:3"] {
							return errors.New("the finance tag must be attached to the dashboard")
						}
						return nil
					},
				),
			},
			{
				ResourceName:            "superset_dashboard.test",
				ImportState:             true,
				ImportStateVerify:       true,
				ImportStateVerifyIgnore: []string{"json_metadata", "position_json"},
			},
			{
				Config: providerConfig + `
resource "superset_dashboard" "test" {
  title         = "Sales"
  json_metadata = jsonencode({ refresh_frequency = 30 })
  tags          = []
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_dashboard.test", "tags.#", "0"),
					func(*terraform.State) error {
						if tag := tags.tag("finance"); tag == nil || tag.objects["dashboard:3"] {
							return errors.New("the finance tag must be detached from the dashboard")
						}
						return nil
					},
				),
			},
		},
	})
}
