resource "superset_theme" "corporate" {
  name = "Corporate"
  json_data = jsonencode({
    token = {
      colorPrimary = "#1f77b4"
      fontFamily   = "Inter, sans-serif"
    }
  })

  # The default theme of the whole instance.
  system_default = true
}

# A dashboard that applies another theme.
resource "superset_theme" "executive" {
  name      = "Executive"
  json_data = jsonencode({ token = { colorPrimary = "#2a9d8f" } })
}

resource "superset_dashboard" "board" {
  title    = "Board"
  theme_id = superset_theme.executive.id
}
