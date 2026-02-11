resource "superset_user" "example_user" {
  username   = "example.user"
  first_name = "Example"
  last_name  = "Sample"
  email      = "example.sample@example.com"
  password   = "ExampleSamplePass123!"
  active     = true
  roles      = [3, 4]
}
