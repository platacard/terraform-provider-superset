# Superset runs with WTF_CSRF_ENABLED = False:
# no CSRF token is requested and write requests carry the access token only.
provider "superset" {
  host         = "https://domain.com"
  username     = "username"
  password     = "password"
  csrf_enabled = false
}
