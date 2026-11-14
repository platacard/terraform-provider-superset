# A view menu of a custom security manager, readable by the Analysts role.
resource "superset_permission_view" "custom_reports_read" {
  permission_name = "can_read"
  view_menu_name  = "CustomReports"
}

resource "superset_role" "analysts" {
  name = "Analysts"
}

resource "superset_role_permissions" "analysts" {
  role_name = superset_role.analysts.name
  resource_permissions = [
    { permission = "can_read", view_menu = "CustomReports" },
  ]
  depends_on = [superset_permission_view.custom_reports_read]
}
