data "superset_report" "weekly_sales" {
  name = "Weekly sales"
  type = "Report"
}

output "weekly_sales_last_state" {
  value = data.superset_report.weekly_sales.last_state
}
