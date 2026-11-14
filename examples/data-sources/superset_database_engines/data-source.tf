data "superset_database_engines" "all" {}

# Fail the plan when the instance has no PostgreSQL driver.
data "superset_database_engines" "postgres" {
  engine = "postgresql"
}

check "postgres_driver" {
  assert {
    condition     = length(data.superset_database_engines.postgres.engines) > 0
    error_message = "Superset has no PostgreSQL driver installed."
  }
}

output "engines" {
  value = [for e in data.superset_database_engines.all.engines : "${e.name} (${e.engine})"]
}
