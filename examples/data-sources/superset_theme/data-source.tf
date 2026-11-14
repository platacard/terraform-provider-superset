data "superset_theme" "dark" {
  name = "THEME_DARK"
}

resource "superset_dashboard" "sales" {
  title    = "Sales"
  theme_id = data.superset_theme.dark.id
}
