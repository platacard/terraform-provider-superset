# A connection that allows uploads into the public schema.
resource "superset_database" "warehouse" {
  connection_name   = "Warehouse"
  sqlalchemy_uri    = var.warehouse_uri
  expose_in_sqllab  = true
  allow_dml         = true
  allow_run_async   = false
  allow_file_upload = true
  extra = jsonencode({
    schemas_allowed_for_file_upload = ["public"]
  })
}

# Reference data kept next to the dashboards, uploaded again whenever the file changes.
resource "superset_database_upload" "regions" {
  database_name = superset_database.warehouse.connection_name
  table_name    = "regions"
  schema        = "public"
  file          = "${path.module}/regions.csv"
  column_dates  = ["valid_from"]
  triggers = {
    hash = filemd5("${path.module}/regions.csv")
  }
  drop_on_destroy = true
}

# A small table written in the configuration.
resource "superset_database_upload" "currencies" {
  database_name  = superset_database.warehouse.connection_name
  table_name     = "currencies"
  schema         = "public"
  content        = <<-CSV
    code,name
    EUR,Euro
    USD,US dollar
  CSV
  already_exists = "replace"
}
