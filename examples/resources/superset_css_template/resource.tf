resource "superset_css_template" "dark_header" {
  template_name = "Dark header"
  css           = <<-EOT
    .dashboard-header {
      background-color: #1f1f1f;
      color: #ffffff;
    }
  EOT
}
