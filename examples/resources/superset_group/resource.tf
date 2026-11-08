resource "superset_role" "analysts" {
  name = "Analysts"
}

resource "superset_group" "finance" {
  name        = "finance"
  label       = "Finance"
  description = "The finance team"
  roles       = [superset_role.analysts.id]
  users       = [superset_user.alice.id]
}
