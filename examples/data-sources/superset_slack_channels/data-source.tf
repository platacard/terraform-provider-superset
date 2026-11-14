data "superset_slack_channels" "sales" {
  search = "sales"
}

# Send the weekly report to the first channel named like "sales".
resource "superset_report" "weekly_sales" {
  name         = "Weekly sales"
  type         = "Report"
  crontab      = "0 9 * * 1"
  dashboard_id = superset_dashboard.sales.id
  recipients = [
    { type = "SlackV2", target = data.superset_slack_channels.sales.channels[0].id },
  ]
}
