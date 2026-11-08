# The datasets of an export, with their columns and metrics kept in sync with the files.
resource "superset_dataset_import" "warehouse" {
  source       = "${path.module}/exports/warehouse"
  sync_columns = true
  sync_metrics = true

  database_passwords = {
    "databases/warehouse.yaml" = var.warehouse_password
  }
}

# The charts of the same export. The dependency orders creation and destruction: Superset
# refuses to delete a dataset that charts still use.
resource "superset_chart_import" "warehouse" {
  source     = "${path.module}/exports/warehouse"
  depends_on = [superset_dataset_import.warehouse]
}
