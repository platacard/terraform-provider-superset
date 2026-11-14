data "superset_dashboard" "sales" {
  slug = "sales"
}

# Every chart placed on the sales dashboard.
data "superset_charts" "on_sales" {
  dashboard_id = data.superset_dashboard.sales.id
}

# Every table chart of a dataset.
data "superset_charts" "tables" {
  datasource_id = superset_dataset.sales.id
  viz_type      = "table"
}

resource "superset_report" "sales_charts" {
  for_each = { for chart in data.superset_charts.on_sales.charts : chart.id => chart }

  name     = "Sales: ${each.value.name}"
  type     = "Report"
  crontab  = "0 9 * * 1"
  chart_id = each.value.id
  recipients = [
    { type = "Email", target = "sales@example.com" },
  ]
}
