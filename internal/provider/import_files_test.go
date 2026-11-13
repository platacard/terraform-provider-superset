package provider

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
	"github.com/jarcoal/httpmock"
)

// filesConfig renders the export files as the files attribute of a dashboard import. The
// password is keyed by the file name alone, as a Kubernetes Secret key would be.
func filesConfig(files map[string]string) string {
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	var lines strings.Builder
	for _, name := range names {
		fmt.Fprintf(&lines, "    %q = %q\n", name, files[name])
	}
	return providerConfig + fmt.Sprintf(`
resource "superset_dashboard_import" "test" {
  files = {
%s  }
  database_passwords = {
    "warehouse.yaml" = "secret"
  }
}
`, lines.String())
}

func TestAccDashboardImportResourceFiles(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeImport{}
	fake.serve()

	changed := map[string]string{}
	for name, content := range exportFiles {
		changed[name] = content
	}
	changed["dashboards/signups.yaml"] = strings.Replace(exportFiles["dashboards/signups.yaml"], "Signups", "Signups v2", 1)

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: filesConfig(exportFiles),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_dashboard_import.test", "dashboard_ids."+exportDashboardUUID, "5"),
					resource.TestCheckResourceAttrSet("superset_dashboard_import.test", "content_hash"),
					func(*terraform.State) error {
						if fake.count() != 1 {
							return fmt.Errorf("expected one import, got %d", fake.count())
						}
						if fake.field("passwords") != `{"databases/warehouse.yaml":"secret"}` {
							return fmt.Errorf("a password keyed by the file name must be sent for databases/warehouse.yaml, got %s", fake.field("passwords"))
						}
						if fake.entry("bundle/metadata.yaml") != "version: 1.0.0\ntype: Dashboard\n" {
							return fmt.Errorf("unexpected metadata: %q", fake.entry("bundle/metadata.yaml"))
						}
						if !strings.Contains(fake.entry("bundle/dashboards/signups.yaml"), "slug: signups") {
							return errors.New("the dashboard file must be in the bundle")
						}
						return nil
					},
				),
			},
			{
				// Nothing changed: no import.
				Config:   filesConfig(exportFiles),
				PlanOnly: true,
			},
			{
				// A changed file plans and performs an import.
				Config: filesConfig(changed),
				Check: func(*terraform.State) error {
					if fake.count() != 2 {
						return fmt.Errorf("expected a second import, got %d", fake.count())
					}
					if !strings.Contains(fake.entry("bundle/dashboards/signups.yaml"), "Signups v2") {
						return errors.New("the changed file must be imported")
					}
					return nil
				},
			},
		},
	})
}

func TestAccImportResourceBundleAttributes(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeImport{}
	fake.serve()

	for _, typeName := range []string{"superset_dashboard_import", "superset_chart_import", "superset_dataset_import", "superset_assets_import"} {
		t.Run(typeName, func(t *testing.T) {
			resource.Test(t, resource.TestCase{
				PreCheck:                 func() { testAccPreCheck(t) },
				ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
				Steps: []resource.TestStep{
					{
						Config:      providerConfig + fmt.Sprintf("resource %q \"test\" {}\n", typeName),
						ExpectError: regexp.MustCompile("Set either source"),
					},
					{
						Config: providerConfig + fmt.Sprintf(`
resource %q "test" {
  source = "/tmp/export"
  files  = { "dashboards/a.yaml" = "uuid: x" }
}
`, typeName),
						ExpectError: regexp.MustCompile("not both"),
					},
					{
						Config: providerConfig + fmt.Sprintf(`
resource %q "test" {
  files = { "../dashboards/a.yaml" = "uuid: x" }
}
`, typeName),
						ExpectError: regexp.MustCompile("must be relative and clean"),
					},
				},
			})
		})
	}
}
