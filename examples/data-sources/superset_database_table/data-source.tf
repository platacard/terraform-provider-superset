data "superset_database_table" "sales" {
  database_name = "Warehouse"
  schema        = "public"
  table_name    = "sales"
}

# The dataset declares every column of the table, with the detected types kept.
resource "superset_dataset" "sales" {
  table_name    = "sales"
  database_name = "Warehouse"
  schema        = "public"
  columns       = [for column in data.superset_database_table.sales.columns : { column_name = column.name }]
}
