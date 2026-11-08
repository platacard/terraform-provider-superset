data "superset_database_tables" "public" {
  database_name = "Warehouse"
  schema        = "public"
}

# One dataset per table of the schema.
resource "superset_dataset" "public" {
  for_each = { for table in data.superset_database_tables.public.tables : table.name => table if table.type == "table" }

  table_name    = each.key
  database_name = "Warehouse"
  schema        = "public"
}
