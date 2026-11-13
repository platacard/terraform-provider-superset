# A view created through SQL Lab and dropped with the resource; the connection must allow DML.
# Bump the version to run the statement again.
resource "superset_sql_execution" "daily_sales" {
  database_name = "Warehouse"
  schema        = "public"
  sql           = "CREATE OR REPLACE VIEW daily_sales AS SELECT day, sum(amount) AS amount FROM sales GROUP BY day"
  destroy_sql   = "DROP VIEW IF EXISTS daily_sales"
  triggers = {
    version = "1"
  }
}

# The dataset on the view waits for it.
resource "superset_dataset" "daily_sales" {
  table_name    = "daily_sales"
  database_name = "Warehouse"
  schema        = "public"
  depends_on    = [superset_sql_execution.daily_sales]
}
