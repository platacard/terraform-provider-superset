data "superset_chart" "sales_by_day" {
  name = "Sales by day"
}

resource "superset_report" "weekly_sales" {
  name     = "Weekly sales"
  type     = "Report"
  crontab  = "0 9 * * 1"
  chart_id = data.superset_chart.sales_by_day.id
  recipients = [
    { type = "Email", target = "sales@example.com" },
  ]
}
