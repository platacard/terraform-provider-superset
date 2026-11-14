# Drop the cached results of the sales datasets after every reload.
resource "superset_cache_invalidation" "sales" {
  dataset_ids = [superset_dataset.sales.id, superset_dataset.sales_by_region.id]
  triggers = {
    loaded_at = var.last_load
  }
}
