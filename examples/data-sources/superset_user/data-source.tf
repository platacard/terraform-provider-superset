data "superset_user" "analyst" {
  username = "analyst"
}

resource "superset_dashboard" "sales" {
  title  = "Sales"
  owners = [data.superset_user.analyst.id]
}
