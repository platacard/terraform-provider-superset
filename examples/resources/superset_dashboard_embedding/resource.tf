resource "superset_dashboard_embedding" "signups" {
  dashboard_id    = superset_dashboard.signups.id
  allowed_domains = ["https://app.example.com", "https://portal.example.com"]
}

# The embedded SDK loads the dashboard by this uuid.
output "signups_embedded_uuid" {
  value = superset_dashboard_embedding.signups.uuid
}
