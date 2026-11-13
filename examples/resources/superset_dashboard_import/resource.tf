# The directory holds an export made in the Superset UI or with `superset export-dashboards`:
# dashboards/, charts/, datasets/ and databases/. The dashboards are imported again whenever
# a file changes.
resource "superset_dashboard_import" "signups" {
  source = "${path.module}/exports/signups"

  # Needed when the import creates the database; keyed by the file inside the export.
  database_passwords = {
    "databases/warehouse.yaml" = var.warehouse_password
  }
}

output "signups_dashboard_ids" {
  value = superset_dashboard_import.signups.dashboard_ids
}

# The files can also be given inline, keyed by their path in the export, for example when the
# provider runs where it cannot read files.
resource "superset_dashboard_import" "signups_inline" {
  files = {
    for file in fileset("${path.module}/exports/signups", "**/*.yaml") :
    file => file("${path.module}/exports/signups/${file}")
  }

  database_passwords = {
    "warehouse.yaml" = var.warehouse_password # the file name alone is enough
  }
}
