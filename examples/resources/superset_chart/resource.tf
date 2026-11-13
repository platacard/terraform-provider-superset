resource "superset_chart" "daily_signups" {
  name          = "Daily signups"
  datasource_id = superset_dataset.signups.id
  viz_type      = "echarts_timeseries_line"
  description   = "Signups per day over the last month"
  cache_timeout = 300

  # Managed externally: Superset shows a banner on the chart and disables its editing in the UI.
  is_managed_externally = true
  external_url          = "https://git.example.com/analytics/blob/main/superset/charts.tf"

  params = jsonencode({
    viz_type   = "echarts_timeseries_line"
    x_axis     = "day"
    metrics    = ["count"]
    time_range = "Last month"
  })
}

# A chart linked to a dashboard from its own side, for a dashboard whose layout Terraform does
# not manage; a dashboard managed with a position_json links its charts itself.
resource "superset_chart" "signups_total" {
  name          = "Signups total"
  datasource_id = superset_dataset.signups.id
  viz_type      = "big_number_total"
  params        = jsonencode({ viz_type = "big_number_total", metric = "count" })
  dashboards    = [data.superset_dashboard.overview.id]
}
