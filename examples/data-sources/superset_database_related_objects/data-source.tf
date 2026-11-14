# Everything that depends on the warehouse connection, for example to check before replacing it.
data "superset_database_related_objects" "warehouse" {
  database_name = "Warehouse"
}

output "warehouse_chart_ids" {
  value = data.superset_database_related_objects.warehouse.chart_ids
}
