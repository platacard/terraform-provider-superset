# Permissions on Superset views.
resource "superset_role_permissions" "example" {
  role_name = "Example-DB-Connect"
  resource_permissions = [
    { permission = "database_access", view_menu = "[Trino].(id:34)" },
    { permission = "schema_access", view_menu = "[Trino].[devstorage]" },
  ]
}

# Object-level permissions: access to one dataset and its schema. The view menus follow the
# naming of Superset, which superset_dataset exposes as perm and schema_perm.
resource "superset_role_permissions" "analysts" {
  role_name = superset_role.analysts.name
  resource_permissions = [
    { permission = "datasource_access", view_menu = superset_dataset.sales.perm },
    { permission = "schema_access", view_menu = superset_dataset.sales.schema_perm },
    { permission = "database_access", view_menu = "[Warehouse].(id:${superset_database.warehouse.id})" },
  ]
}
