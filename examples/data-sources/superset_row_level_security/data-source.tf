data "superset_row_level_security" "region" {
  name = "Region of the user"
}

output "region_rule_datasets" {
  value = data.superset_row_level_security.region.tables
}
