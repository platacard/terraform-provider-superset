# Sync the catalog and schema permissions of a connection. Bump the version whenever the
# source gains schemas, so the roles can be granted access to them.
resource "superset_database_permissions_sync" "warehouse" {
  database_id = superset_database.warehouse.id
  triggers = {
    version = "2"
  }
}
