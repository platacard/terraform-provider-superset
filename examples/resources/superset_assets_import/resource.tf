# The directory holds an export made in the Superset UI or with `superset export-dashboards`.
# Everything in it is imported at once and overwritten whenever a file changes.
resource "superset_assets_import" "analytics" {
  source = "${path.module}/exports/analytics"

  # Needed when the import creates the database; keyed by the file inside the export.
  database_passwords = {
    "databases/warehouse.yaml" = var.warehouse_password
  }
}

output "analytics_dashboard_ids" {
  value = superset_assets_import.analytics.dashboard_ids
}
