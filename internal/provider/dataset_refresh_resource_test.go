package provider

import (
	"errors"
	"fmt"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/plancheck"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
	"github.com/jarcoal/httpmock"
)

func datasetRefreshConfig(version string) string {
	return physicalDataset + fmt.Sprintf(`
resource "superset_dataset_refresh" "test" {
  dataset_id = superset_dataset.test.id
  triggers = {
    version = %q
  }
}
`, version)
}

func TestAccDatasetRefreshResource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeDataset{}
	fake.serve()

	refreshes := func(want int) resource.TestCheckFunc {
		return func(*terraform.State) error {
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if fake.refreshes != want {
				return fmt.Errorf("the dataset was refreshed %d times, want %d", fake.refreshes, want)
			}
			return nil
		}
	}

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: datasetRefreshConfig("1"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_dataset_refresh.test", "id", "123"),
					resource.TestCheckResourceAttr("superset_dataset_refresh.test", "dataset_id", "123"),
					resource.TestCheckResourceAttr("superset_dataset_refresh.test", "triggers.version", "1"),
					refreshes(1),
				),
			},
			{
				// A changed trigger updates the resource in place, which syncs again: a column
				// dropped from the dataset in the meantime is detected again.
				PreConfig: func() {
					fake.mu.Lock()
					defer fake.mu.Unlock()
					fake.columns = fake.columns[:2]
				},
				Config: datasetRefreshConfig("2"),
				ConfigPlanChecks: resource.ConfigPlanChecks{
					PreApply: []plancheck.PlanCheck{
						plancheck.ExpectResourceAction("superset_dataset_refresh.test", plancheck.ResourceActionUpdate),
					},
				},
				Check: resource.ComposeAggregateTestCheckFunc(
					refreshes(2),
					func(*terraform.State) error {
						if fake.column("day") == nil {
							return errors.New("the sync must detect the day column again")
						}
						return nil
					},
				),
			},
			{
				// A dataset that was recreated is synced again.
				PreConfig: func() {
					fake.mu.Lock()
					fake.deleted = true
					fake.mu.Unlock()
				},
				Config: datasetRefreshConfig("2"),
				Check:  refreshes(3),
			},
		},
	})
}
