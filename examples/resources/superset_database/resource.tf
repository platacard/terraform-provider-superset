resource "superset_database" "example" {
  connection_name  = "SuperSetDBConnection"
  db_engine        = "postgresql"
  db_user          = "supersetuser"
  db_pass          = "dbpassword"
  db_host          = "pg.db.ro.domain.com"
  db_port          = 5432
  db_name          = "supersetdb"
  allow_ctas       = false
  allow_cvas       = false
  allow_dml        = false
  allow_run_async  = true
  expose_in_sqllab = false
}
