resource "superset_meta_database" "example" {
  database_name  = "SuperSetDBConnection"
  sqlalchemy_uri = "superset://" # optional
  allowed_databases = [
    "[Team]-Service1-Dev-RO[d_team_service1_db]",
    "[Team]-Service2-Prod-RO[d_team_market_service2_db]"
  ]
  expose_in_sqllab      = true
  allow_ctas            = false
  allow_cvas            = false
  allow_dml             = false
  allow_run_async       = true
  is_managed_externally = false
}
