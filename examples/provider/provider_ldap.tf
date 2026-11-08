provider "superset" {
  host          = "https://domain.com"
  username      = "ldap-user"
  password      = var.ldap_password
  auth_provider = "ldap"
}
