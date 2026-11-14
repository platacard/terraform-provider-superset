data "superset_database_catalogs" "warehouse" {
  database_name = "Warehouse"
}

# The schemas of every catalog of the connection.
data "superset_database_schemas" "per_catalog" {
  for_each = toset(data.superset_database_catalogs.warehouse.catalogs)

  database_name = "Warehouse"
  catalog       = each.key
}

output "schemas_per_catalog" {
  value = { for catalog, schemas in data.superset_database_schemas.per_catalog : catalog => schemas.schemas }
}
