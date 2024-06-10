resource "superset_role_permissions" "example" {
  role_name = "Example-DB-Connect"
  resource_permissions = [
    { permission = "database_access", view_menu = "[Trino].(id:34)" },
    { permission = "schema_access", view_menu = "[Trino].[devstorage]" },
  ]
}
