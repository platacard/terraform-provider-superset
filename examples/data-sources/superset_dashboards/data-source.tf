# Every published dashboard that carries the "public" tag.
data "superset_dashboards" "public" {
  published = true
  tag       = "public"
}

resource "superset_dashboard_embedding" "public" {
  for_each = toset([for id in data.superset_dashboards.public.ids : tostring(id)])

  dashboard_id    = tonumber(each.key)
  allowed_domains = ["https://www.example.com"]
}
