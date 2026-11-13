package provider

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
	"github.com/jarcoal/httpmock"
)

// datasetSettings is a dataset with every setting of the later phases: the SQL Lab flag, the
// offset, the fields of Superset 6, a metric with a currency and a column with its extra fields.
const datasetSettings = providerConfig + `
resource "superset_dataset" "test" {
  table_name           = "sales"
  database_name        = "PostgreSQL Database"
  schema               = "public"
  is_sqllab_view       = true
  offset               = 3
  catalog              = "pgdb"
  currency_code_column = "amount"

  metrics = [
    { metric_name = "count", expression = "COUNT(*)", currency = jsonencode({ symbol = "USD", symbolPosition = "prefix" }) },
  ]
  columns = [
    { column_name = "amount", advanced_data_type = "internet_address", extra = "{\"warning_markdown\": \"careful\"}", is_active = false },
  ]
}
`

func TestAccDatasetResourceSettings(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeDataset{currencyObjects: true}
	fake.serve()
	// sentNull checks that the last update carried the key as null, which clears it.
	sentNull := func(key string) resource.TestCheckFunc {
		return func(*terraform.State) error {
			value, present := fake.sentKey(key)
			if !present || value != nil {
				return fmt.Errorf("%s sent as %v (present %v), want null", key, value, present)
			}
			return nil
		}
	}

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: datasetSettings,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_dataset.test", "is_sqllab_view", "true"),
					resource.TestCheckResourceAttr("superset_dataset.test", "offset", "3"),
					resource.TestCheckResourceAttr("superset_dataset.test", "catalog", "pgdb"),
					resource.TestCheckResourceAttr("superset_dataset.test", "currency_code_column", "amount"),
					resource.TestCheckResourceAttr("superset_dataset.test", "metrics.#", "1"),
					resource.TestCheckResourceAttr("superset_dataset.test", "metrics.0.currency", `{"symbol":"USD","symbolPosition":"prefix"}`),
					resource.TestCheckResourceAttr("superset_dataset.test", "columns.#", "1"),
					resource.TestCheckResourceAttr("superset_dataset.test", "columns.0.advanced_data_type", "internet_address"),
					resource.TestCheckResourceAttr("superset_dataset.test", "columns.0.extra", `{"warning_markdown": "careful"}`),
					resource.TestCheckResourceAttr("superset_dataset.test", "columns.0.is_active", "false"),
					func(*terraform.State) error {
						metric := fake.metric("count")
						if metric["currency"] != `{"symbol":"USD","symbolPosition":"prefix"}` {
							return fmt.Errorf("currency sent as %v, want the configured text", metric["currency"])
						}
						column := fake.column("amount")
						if column["is_active"] != false || column["advanced_data_type"] != "internet_address" {
							return fmt.Errorf("column fields sent as %v", column)
						}
						return nil
					},
				),
			},
			{
				// The currency comes back as an object and the extra as text: no drift.
				Config:   datasetSettings,
				PlanOnly: true,
			},
			{
				// An unset currency code column is cleared with a null; an unset catalog is left as
				// Superset has it, since Superset refuses to clear it on most connections.
				Config: providerConfig + `
resource "superset_dataset" "test" {
  table_name     = "sales"
  database_name  = "PostgreSQL Database"
  schema         = "public"
  is_sqllab_view = true
  offset         = 3
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_dataset.test", "catalog", "pgdb"),
					resource.TestCheckNoResourceAttr("superset_dataset.test", "currency_code_column"),
					sentNull("currency_code_column"),
				),
			},
			{
				// Once cleared, the currency code column is no longer sent at all.
				Config: physicalDataset,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_dataset.test", "is_sqllab_view", "false"),
					resource.TestCheckResourceAttr("superset_dataset.test", "offset", "0"),
					func(*terraform.State) error {
						if _, present := fake.sentKey("currency_code_column"); present {
							return fmt.Errorf("currency_code_column must not be sent once cleared")
						}
						return nil
					},
				),
			},
		},
	})
}

func TestAccDatasetResourceSixOnlyFieldsOnLegacySuperset(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeDataset{legacy: true}
	fake.serve()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				// Without the fields of Superset 6, nothing unknown is sent.
				Config: providerConfig + `
resource "superset_dataset" "test" {
  table_name    = "sales"
  database_name = "PostgreSQL Database"
  offset        = 2
  metrics       = [{ metric_name = "count", expression = "COUNT(*)", currency = "{\"symbol\": \"EUR\", \"symbolPosition\": \"suffix\"}" }]
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_dataset.test", "offset", "2"),
					resource.TestCheckResourceAttr("superset_dataset.test", "metrics.0.currency", `{"symbol": "EUR", "symbolPosition": "suffix"}`),
				),
			},
			{
				Config: providerConfig + `
resource "superset_dataset" "test" {
  table_name    = "sales"
  database_name = "PostgreSQL Database"
  catalog       = "pgdb"
}
`,
				ExpectError: regexp.MustCompile("Unknown field"),
			},
		},
	})
}
