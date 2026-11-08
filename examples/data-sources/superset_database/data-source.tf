data "superset_database" "warehouse" {
  database_name = "Warehouse"
}

resource "superset_dataset" "sales" {
  table_name    = "sales"
  database_name = data.superset_database.warehouse.database_name
  schema        = "public"
}
