# A table of a database. Superset detects the columns and creates the count metric itself.
resource "superset_dataset" "sales" {
  table_name    = "sales"
  database_name = superset_database.warehouse.connection_name
  schema        = "public"
  description   = "One row per sale"
  cache_timeout = 600
  main_dttm_col = "sold_at"

  extra = jsonencode({
    certification = { certified_by = "Data team", details = "Reviewed monthly" }
  })
}

# A virtual dataset defined by a query with a Jinja parameter.
resource "superset_dataset" "signups_per_day" {
  table_name      = "signups_per_day"
  database_name   = superset_database.warehouse.connection_name
  sql             = "SELECT day, count(*) AS signups FROM signups WHERE region = '{{ region }}' GROUP BY day"
  template_params = jsonencode({ region = "eu" })
}
