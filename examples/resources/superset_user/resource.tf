resource "superset_user" "example_user" {
  username   = "example.user"
  first_name = "Example"
  last_name  = "Sample"
  email      = "example.sample@example.com"
  password   = "ExampleSamplePass123!"
  active     = true
  roles      = [3, 4]
}

# On Superset 6 a user can be made a member of groups from the user side; manage a
# membership either here or in the `users` of the group, not in both.
resource "superset_group" "analysts" {
  name  = "analysts"
  roles = [4]
}

resource "superset_user" "analyst" {
  username   = "analyst.user"
  first_name = "Analyst"
  last_name  = "Sample"
  email      = "analyst.sample@example.com"
  password   = "AnalystSamplePass123!"
  roles      = [4]
  groups     = [superset_group.analysts.id]
}
