resource "superset_saved_query" "daily_signups" {
  database_id         = superset_database.analytics.id
  label               = "Daily signups"
  description         = "Signups per day, parameterised by date"
  schema              = "public"
  sql                 = "SELECT count(*) FROM signups WHERE day = '{{ day }}'"
  template_parameters = jsonencode({ day = "2026-01-01" })
}
