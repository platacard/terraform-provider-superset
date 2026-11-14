data "superset_annotation_layer" "releases" {
  name = "Releases"
}

resource "superset_annotation" "v2" {
  layer_id    = data.superset_annotation_layer.releases.id
  short_descr = "v2.0.0"
  start_dttm  = "2026-03-01T09:00:00"
  end_dttm    = "2026-03-01T09:00:00"
}
