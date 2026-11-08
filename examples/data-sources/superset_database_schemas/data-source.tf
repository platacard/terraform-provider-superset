data "superset_database_schemas" "warehouse" {
  database_name = "Warehouse"
}

output "schemas" {
  value = data.superset_database_schemas.warehouse.schemas
}
