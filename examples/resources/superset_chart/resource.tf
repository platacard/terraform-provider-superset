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
