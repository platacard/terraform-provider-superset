data "superset_saved_query" "top_customers" {
  label = "Top customers"
}

resource "superset_dataset" "top_customers" {
  database_name = data.superset_saved_query.top_customers.database_name
  table_name    = "top_customers"
  schema        = data.superset_saved_query.top_customers.schema
  sql           = data.superset_saved_query.top_customers.sql
}
