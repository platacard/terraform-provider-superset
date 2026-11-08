resource "superset_chart" "daily_signups" {
  name          = "Daily signups"
  datasource_id = superset_dataset.signups.id
  viz_type      = "echarts_timeseries_line"
  description   = "Signups per day over the last month"
  cache_timeout = 300

  params = jsonencode({
    viz_type   = "echarts_timeseries_line"
    x_axis     = "day"
    metrics    = ["count"]
    time_range = "Last month"
  })
}
