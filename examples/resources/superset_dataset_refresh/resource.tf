# Sync the columns of a dataset from its source table. Bump the version whenever the table
# gains or loses columns, so Superset picks the change up on the next apply.
resource "superset_dataset_refresh" "sales" {
  dataset_id = superset_dataset.sales.id
  triggers = {
    version = "2"
  }
}
