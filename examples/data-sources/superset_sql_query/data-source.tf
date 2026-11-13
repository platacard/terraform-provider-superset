# One row per region of the sales table, read through SQL Lab.
data "superset_sql_query" "regions" {
  database_name = "Warehouse"
  schema        = "public"
  sql           = "SELECT DISTINCT region FROM sales ORDER BY region"
}

# A row level security filter per region: the rows come back as maps of texts.
resource "superset_row_level_security" "region" {
  for_each = toset([for row in data.superset_sql_query.regions.rows : row.region])

  name   = "sales ${each.key}"
  tables = [superset_dataset.sales.id]
  clause = "region = '${each.key}'"
}

# Typed values, when the texts are not enough.
output "regions" {
  value = jsondecode(data.superset_sql_query.regions.rows_json)
}
