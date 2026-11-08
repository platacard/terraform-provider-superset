# A dashboard sent every Monday morning as a PNG.
resource "superset_report" "weekly_sales" {
  name         = "Weekly sales"
  type         = "Report"
  crontab      = "0 9 * * 1"
  timezone     = "Europe/Berlin"
  dashboard_id = superset_dashboard.sales.id
  description  = "Sales of the past week"

  recipients = [
    { type = "Email", target = "sales@example.com, management@example.com" },
    { type = "Slack", target = "#sales" },
  ]
}

# An alert that sends the chart when the query returns a value below the threshold.
resource "superset_report" "low_sales" {
  name                = "Low sales"
  type                = "Alert"
  crontab             = "0 * * * *"
  chart_id            = superset_chart.sales_today.id
  report_format       = "TEXT"
  database_name       = superset_database.warehouse.connection_name
  sql                 = "SELECT sum(amount) FROM sales WHERE sold_at >= current_date"
  validator_type      = "operator"
  validator_op        = "<"
  validator_threshold = 1000
  grace_period        = 3600

  recipients = [
    { type = "Email", target = "ops@example.com" },
  ]
}
