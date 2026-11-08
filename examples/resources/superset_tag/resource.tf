resource "superset_tag" "finance" {
  name        = "finance"
  description = "Owned by the finance team"
}

# The tags of a chart or a dashboard are set on the object; a tag that does not exist yet is created.
resource "superset_chart" "revenue" {
  name          = "Revenue"
  datasource_id = superset_dataset.sales.id
  viz_type      = "table"
  params        = jsonencode({ viz_type = "table", all_columns = ["day", "amount"] })
  tags          = [superset_tag.finance.name, "weekly-review"]
}
