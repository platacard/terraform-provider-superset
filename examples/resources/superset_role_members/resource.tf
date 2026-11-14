resource "superset_role" "analysts" {
  name = "Analysts"
}

data "superset_user" "ana" {
  username = "ana"
}

data "superset_user" "ben" {
  username = "ben"
}

data "superset_group" "finance" {
  name = "finance"
}

# Exactly these users and groups have the Analysts role; members added in the UI are removed
# on the next apply.
resource "superset_role_members" "analysts" {
  role_id   = superset_role.analysts.id
  user_ids  = [data.superset_user.ana.id, data.superset_user.ben.id]
  group_ids = [data.superset_group.finance.id]
}
