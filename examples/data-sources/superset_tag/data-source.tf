data "superset_tag" "finance" {
  name = "finance"
}

output "finance_tag_id" {
  value = data.superset_tag.finance.id
}
