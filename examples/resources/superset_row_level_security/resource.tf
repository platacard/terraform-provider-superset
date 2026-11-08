data "superset_role" "analysts" {
  name = "Analysts"
}

# Members of the Analysts role only see EMEA rows of the sales dataset.
resource "superset_row_level_security" "emea_only" {
  name        = "EMEA only"
  description = "Analysts see their own region"
  filter_type = "Regular"
  group_key   = "region"
  clause      = "region = 'EMEA'"
  tables      = [superset_dataset.sales.id]
  roles       = [data.superset_role.analysts.id]
}
