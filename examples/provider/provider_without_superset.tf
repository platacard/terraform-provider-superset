# The provider is declared, but Superset is not available in this environment:
# no Superset resources are managed, so nothing is validated and no request is sent.
provider "superset" {
  validate_credentials = false
}
