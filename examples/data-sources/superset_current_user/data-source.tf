data "superset_current_user" "me" {}

# The user the provider runs as owns the dashboard, so it stays editable by that user.
resource "superset_dashboard" "sales" {
  title  = "Sales"
  owners = [data.superset_current_user.me.id]
}
