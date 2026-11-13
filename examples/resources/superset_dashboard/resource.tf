resource "superset_dashboard" "signups" {
  title     = "Signups"
  slug      = "signups"
  published = true
  roles     = [superset_role.analysts.id]

  # Managed externally: Superset shows a banner on the dashboard and disables its editing in the UI.
  is_managed_externally = true
  external_url          = "https://git.example.com/analytics/blob/main/superset/dashboards.tf"

  json_metadata = jsonencode({
    refresh_frequency = 300
    color_scheme      = "supersetColors"
  })

  # The layout places the chart on the dashboard and links it to the dashboard.
  position_json = jsonencode({
    DASHBOARD_VERSION_KEY = "v2"
    ROOT_ID               = { type = "ROOT", id = "ROOT_ID", children = ["GRID_ID"] }
    GRID_ID               = { type = "GRID", id = "GRID_ID", parents = ["ROOT_ID"], children = ["ROW-1"] }
    "ROW-1" = {
      type     = "ROW"
      id       = "ROW-1"
      parents  = ["ROOT_ID", "GRID_ID"]
      children = ["CHART-1"]
      meta     = { background = "BACKGROUND_TRANSPARENT" }
    }
    "CHART-1" = {
      type     = "CHART"
      id       = "CHART-1"
      parents  = ["ROOT_ID", "GRID_ID", "ROW-1"]
      children = []
      meta     = { chartId = superset_chart.daily_signups.id, width = 6, height = 50 }
    }
  })
}
