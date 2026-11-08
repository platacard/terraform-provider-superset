resource "superset_annotation_layer" "releases" {
  name        = "Releases"
  description = "Production releases"
}

# An instant: the start and the end are the same.
resource "superset_annotation" "v2" {
  layer_id    = superset_annotation_layer.releases.id
  short_descr = "v2.0.0"
  long_descr  = "New checkout flow"
  start_dttm  = "2026-01-15T09:00:00"
  end_dttm    = "2026-01-15T09:00:00"
}

# A period, with extra settings for the charts that display the layer.
resource "superset_annotation" "freeze" {
  layer_id    = superset_annotation_layer.releases.id
  short_descr = "Holiday freeze"
  start_dttm  = "2026-12-20T00:00:00"
  end_dttm    = "2027-01-05T00:00:00"

  json_metadata = jsonencode({ color = "#999999" })
}
