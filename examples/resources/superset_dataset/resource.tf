resource "superset_dataset" "example" {
  table_name    = "example_table"
  database_name = "PostgreSQL"
  schema        = "public"
  sql           = "SELECT 1 as test_column"
}