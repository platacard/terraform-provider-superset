resource "superset_group" "example_group" {
  name  = "Data Team"
  roles = [3, 4]
  users = [10, 20]
}
