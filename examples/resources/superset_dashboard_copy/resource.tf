data "superset_dashboard" "template" {
  slug = "sales-template"
}

# A copy that shows the same charts as the template.
resource "superset_dashboard_copy" "emea" {
  source_dashboard_id = data.superset_dashboard.template.id
  title               = "Sales EMEA"
  css                 = ".dashboard-header { background: #004c6d; }"
  json_metadata = jsonencode({
    color_scheme = "supersetColors"
  })
}

# A copy with its own charts, duplicated from the template, which can be changed without
# touching the template.
resource "superset_dashboard_copy" "apac" {
  source_dashboard_id = data.superset_dashboard.template.id
  title               = "Sales APAC"
  duplicate_slices    = true
}

output "apac_chart_ids" {
  value = superset_dashboard_copy.apac.chart_ids
}
