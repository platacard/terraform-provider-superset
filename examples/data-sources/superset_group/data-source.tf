data "superset_group" "analysts" {
  name = "analysts"
}

resource "superset_user" "analyst" {
  username   = "analyst"
  first_name = "Ana"
  last_name  = "Lyst"
  email      = "analyst@example.com"
  password   = "AnalystSamplePass123!"
  roles      = [4]
  groups     = [data.superset_group.analysts.id]
}
