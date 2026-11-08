# A table of a database. Superset detects the columns and creates the count metric itself
# unless columns and metrics take them over.
resource "superset_dataset" "sales" {
  table_name    = "sales"
  database_name = superset_database.warehouse.connection_name
  schema        = "public"
  description   = "One row per sale"
  cache_timeout = 600
  main_dttm_col = "sold_at"

  extra = jsonencode({
    certification = { certified_by = "Data team", details = "Reviewed monthly" }
  })

  # With columns set, the dataset has exactly these columns. Only the attributes that are set
  # are written: the detected type of a column of the source is kept.
  columns = [
    { column_name = "id", groupby = false },
    { column_name = "amount", verbose_name = "Amount" },
    { column_name = "sold_at", is_dttm = true },
    { column_name = "amount_cents", expression = "amount * 100", type = "NUMERIC" },
  ]

  # With metrics set, the dataset has exactly these metrics; declare count to keep it.
  metrics = [
    { metric_name = "count", expression = "COUNT(*)", verbose_name = "Sales" },
    { metric_name = "revenue", expression = "SUM(amount)", verbose_name = "Revenue", d3format = ",.2f" },
  ]
}

# A virtual dataset defined by a query with a Jinja parameter.
resource "superset_dataset" "signups_per_day" {
  table_name      = "signups_per_day"
  database_name   = superset_database.warehouse.connection_name
  sql             = "SELECT day, count(*) AS signups FROM signups WHERE region = '{{ region }}' GROUP BY day"
  template_params = jsonencode({ region = "eu" })
}
