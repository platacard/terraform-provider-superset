resource "superset_chart" "example" {
  slice_name      = "Revenue by Region"
  datasource_id   = superset_dataset.sales.id
  datasource_type = "table"
  viz_type        = "pie"
  description     = "Pie chart showing revenue distribution by region"
}

# Chart with cache timeout
resource "superset_chart" "cached_chart" {
  slice_name      = "Daily Active Users"
  datasource_id   = superset_dataset.analytics.id
  datasource_type = "table"
  viz_type        = "big_number_total"
  description     = "Total daily active users with 5-minute cache"
  cache_timeout   = 300
}

# Minimal chart (required fields only)
resource "superset_chart" "minimal" {
  slice_name    = "Simple Table"
  datasource_id = 1
}

