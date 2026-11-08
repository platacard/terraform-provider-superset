# An export zip file made in the Superset UI; the charts, their datasets and databases are imported.
resource "superset_chart_import" "kpis" {
  source = "${path.module}/exports/kpi_charts.zip"

  database_passwords = {
    "databases/warehouse.yaml" = var.warehouse_password
  }
}
