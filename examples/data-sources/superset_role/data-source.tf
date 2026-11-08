data "superset_role" "gamma" {
  name = "Gamma"
}

# The id can be referenced wherever a role id is expected, for example when assigning roles to a user.
resource "superset_user" "analyst" {
  username   = "analyst"
  first_name = "Data"
  last_name  = "Analyst"
  email      = "analyst@example.com"
  password   = "ChangeMe123!"
  roles      = [data.superset_role.gamma.id]
}
