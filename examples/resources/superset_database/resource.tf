# A connection given as its parts.
resource "superset_database" "warehouse" {
  connection_name  = "Warehouse"
  db_engine        = "postgresql"
  db_user          = "superset"
  db_pass          = var.warehouse_password
  db_host          = "pg.db.ro.domain.com"
  db_port          = 5432
  db_name          = "warehouse"
  allow_ctas       = false
  allow_cvas       = false
  allow_dml        = false
  allow_run_async  = true
  expose_in_sqllab = true
}

# A connection given as a URI, reached through an SSH tunnel, with file uploads into one schema.
resource "superset_database" "analytics" {
  connection_name   = "Analytics"
  sqlalchemy_uri    = "postgresql://superset:${var.analytics_password}@pg.internal:5432/analytics"
  allow_ctas        = true
  allow_cvas        = false
  allow_dml         = false
  allow_run_async   = true
  expose_in_sqllab  = true
  allow_file_upload = true
  cache_timeout     = 600
  force_ctas_schema = "scratch"

  extra = jsonencode({
    client_encoding                 = "utf8"
    schemas_allowed_for_file_upload = ["uploads"]
  })

  ssh_tunnel = {
    server_address = "bastion.internal"
    server_port    = 22
    username       = "superset"
    private_key    = file("${path.module}/bastion_rsa.pem")
  }
}
