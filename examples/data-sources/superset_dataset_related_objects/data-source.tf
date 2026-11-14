data "superset_dataset" "sales" {
  table_name = "sales"
}

# The charts and dashboards built on the sales dataset.
data "superset_dataset_related_objects" "sales" {
  dataset_id = data.superset_dataset.sales.id
}

output "sales_dashboards" {
  value = [for d in data.superset_dataset_related_objects.sales.dashboards : d.title]
}
