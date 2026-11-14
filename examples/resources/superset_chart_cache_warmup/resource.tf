# Compute the chart for the cache after every nightly load, in the context of the dashboard
# filters.
resource "superset_chart_cache_warmup" "sales_by_day" {
  chart_id     = superset_chart.sales_by_day.id
  dashboard_id = superset_dashboard.sales.id
  triggers = {
    loaded_at = var.last_load
  }
}
