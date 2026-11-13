package provider

import (
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/plancheck"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
	"github.com/jarcoal/httpmock"
)

// serveDatabaseConnection serves the connection of database 3, which answers 404 as many
// times as gone says.
func serveDatabaseConnection(mu *sync.Mutex, gone *int) {
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/database/3/connection",
		func(*http.Request) (*http.Response, error) {
			mu.Lock()
			defer mu.Unlock()
			if *gone > 0 {
				*gone--
				return httpmock.NewStringResponse(404, `{"message": "Not found"}`), nil
			}
			return httpmock.NewStringResponse(200, `{"id": 3, "result": {"id": 3, "database_name": "Warehouse", "sqlalchemy_uri": "postgresql://x", "expose_in_sqllab": true, "allow_dml": true}}`), nil
		})
}

func sqlExecutionConfig(sql, destroySQL, version string) string {
	return providerConfig + fmt.Sprintf(`
resource "superset_sql_execution" "test" {
  database_name = "Warehouse"
  schema        = "public"
  sql           = %q
  destroy_sql   = %q
  triggers = {
    version = %q
  }
}
`, sql, destroySQL, version)
}

func TestAccSQLExecutionResource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeSQLLab{}
	fake.serve()
	var mu sync.Mutex
	gone := 0
	serveDatabaseConnection(&mu, &gone)
	// executed checks the statements run so far.
	executed := func(want ...string) resource.TestCheckFunc {
		return func(*terraform.State) error {
			if got := fake.executed(); strings.Join(got, "|") != strings.Join(want, "|") {
				return fmt.Errorf("statements run: %q, want %q", got, want)
			}
			return nil
		}
	}
	create, drop := "CREATE VIEW v AS SELECT 1", "DROP VIEW v"

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: sqlExecutionConfig(create, drop, "1"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_sql_execution.test", "id", "9"),
					resource.TestCheckResourceAttr("superset_sql_execution.test", "database_id", "3"),
					resource.TestCheckResourceAttr("superset_sql_execution.test", "triggers.version", "1"),
					executed(create),
					func(*terraform.State) error {
						if fake.sent("schema") != "public" || fake.sent("queryLimit") != float64(1) || fake.sent("runAsync") != false {
							return fmt.Errorf("unexpected request: schema %v, queryLimit %v, runAsync %v", fake.sent("schema"), fake.sent("queryLimit"), fake.sent("runAsync"))
						}
						return nil
					},
				),
			},
			{
				// A changed trigger runs the statement again in place.
				Config: sqlExecutionConfig(create, drop, "2"),
				ConfigPlanChecks: resource.ConfigPlanChecks{
					PreApply: []plancheck.PlanCheck{
						plancheck.ExpectResourceAction("superset_sql_execution.test", plancheck.ResourceActionUpdate),
					},
				},
				Check: executed(create, create),
			},
			{
				// A changed destroy statement is only recorded.
				Config: sqlExecutionConfig(create, "DROP VIEW IF EXISTS v", "2"),
				ConfigPlanChecks: resource.ConfigPlanChecks{
					PreApply: []plancheck.PlanCheck{
						plancheck.ExpectResourceAction("superset_sql_execution.test", plancheck.ResourceActionUpdate),
					},
				},
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_sql_execution.test", "destroy_sql", "DROP VIEW IF EXISTS v"),
					executed(create, create),
				),
			},
			{
				// A changed statement replaces the resource: the destroy statement runs, then the
				// new one.
				Config: sqlExecutionConfig("CREATE VIEW v AS SELECT 2", "DROP VIEW IF EXISTS v", "2"),
				ConfigPlanChecks: resource.ConfigPlanChecks{
					PreApply: []plancheck.PlanCheck{
						plancheck.ExpectResourceAction("superset_sql_execution.test", plancheck.ResourceActionReplace),
					},
				},
				Check: executed(create, create, "DROP VIEW IF EXISTS v", "CREATE VIEW v AS SELECT 2"),
			},
			{
				// A connection that was recreated gets the statement again, without a destroy.
				PreConfig: func() {
					mu.Lock()
					gone = 1
					mu.Unlock()
				},
				Config: sqlExecutionConfig("CREATE VIEW v AS SELECT 2", "DROP VIEW IF EXISTS v", "2"),
				ConfigPlanChecks: resource.ConfigPlanChecks{
					PreApply: []plancheck.PlanCheck{
						plancheck.ExpectResourceAction("superset_sql_execution.test", plancheck.ResourceActionCreate),
					},
				},
				Check: executed(create, create, "DROP VIEW IF EXISTS v", "CREATE VIEW v AS SELECT 2", "CREATE VIEW v AS SELECT 2"),
			},
			{
				// Destroying the resource runs the destroy statement.
				Config: providerConfig,
				Check:  executed(create, create, "DROP VIEW IF EXISTS v", "CREATE VIEW v AS SELECT 2", "CREATE VIEW v AS SELECT 2", "DROP VIEW IF EXISTS v"),
			},
			{
				Config: providerConfig + `
resource "superset_sql_execution" "test" {
  database_id = 3
  sql         = "SELECT * FROM nosuch"
}
`,
				ExpectError: regexp.MustCompile(`relation "nosuch" does not exist`),
			},
		},
	})
}
