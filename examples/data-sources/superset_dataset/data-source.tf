data "superset_dataset" "sales" {
  table_name    = "sales"
  schema        = "public"
  database_name = "Warehouse"
}

resource "superset_chart" "sales_by_day" {
  name          = "Sales by day"
  datasource_id = data.superset_dataset.sales.id
  viz_type      = "table"
  params        = jsonencode({ viz_type = "table", all_columns = ["day", "amount"] })
}

# The permission of the dataset, for a role.
resource "superset_role_permissions" "analysts" {
  role_name = "Analysts"
  resource_permissions = [
    { permission = "datasource_access", view_menu = data.superset_dataset.sales.perm },
  ]
}
