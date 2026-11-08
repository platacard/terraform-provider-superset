data "superset_dashboard" "sales" {
  slug = "sales"
}

resource "superset_dashboard_embedding" "sales" {
  dashboard_id    = data.superset_dashboard.sales.id
  allowed_domains = ["https://intranet.example.com"]
}
