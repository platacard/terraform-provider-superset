---
page_title: "superset Provider"
description: |-
  Superset provider for managing Superset resources. The provider logs in to Superset when it is configured; set validate_credentials = false to defer it until the first API call.
---

# superset Provider

Superset provider for managing Superset resources. The provider logs in to Superset when it is configured; set `validate_credentials = false` to defer it until the first API call.

## Example Usage

```terraform
provider "superset" {
  host     = "https://domain.com" # Replace with your Superset instance URL
  username = "username"           # Replace with your Superset username
  password = "password"           # Replace with your Superset password
}
```

The settings can also be provided with the `SUPERSET_HOST`, `SUPERSET_USERNAME`, `SUPERSET_PASSWORD` and `SUPERSET_AUTH_PROVIDER` environment variables.
Values set in the configuration take precedence.

## Authentication

The provider authenticates with the Superset login API (`/api/v1/security/login`) and uses the returned access token for all requests.

* **When the provider logs in.** With `validate_credentials = true` (the default) the provider logs in when it is configured,
  so missing or invalid credentials fail `terraform plan` right away. With `validate_credentials = false` it logs in on the
  first request made by a resource or data source.
* **Concurrency.** One access token is shared by all operations of a provider instance. When Terraform runs operations in
  parallel, only one of them logs in and the others wait for the token.
* **Expired tokens.** If Superset rejects the access token (HTTP 401, e.g. it has expired during a long apply),
  the provider logs in again and retries the request once. A second 401 is returned as an error.

## Authentication provider

The login request names an authentication provider. With `auth_provider = "db"` (the default) the credentials are those of
a user of the Superset database. An instance configured with `AUTH_TYPE = AUTH_LDAP` only accepts LDAP logins and rejects
the default with `Alternative authentication provider is not allowed`; select the LDAP provider for it:

```terraform
provider "superset" {
  host          = "https://domain.com"
  username      = "ldap-user"
  password      = var.ldap_password
  auth_provider = "ldap"
}
```

The LDAP user must have a role that allows the operations of the configuration, for example through
`AUTH_USER_REGISTRATION_ROLE` or the role mapping of the instance.

## Security API

The `superset_role`, `superset_role_permissions` and `superset_user` resources and the `superset_roles`, `superset_role`,
`superset_role_permissions` and `superset_users` data sources use the Flask-AppBuilder security API (`/api/v1/security/users/`,
`/roles/`, `/permissions-resources/`). Superset only exposes it with `FAB_ADD_SECURITY_API = True` in `superset_config.py`
(the default in recent versions). Without it these endpoints answer 404, which the provider reports with this explanation.

## Feature flags

Some resources depend on feature flags of the Superset instance (`FEATURE_FLAGS` in `superset_config.py`):

* `ENABLE_TEMPLATE_PROCESSING`: Jinja templates in the SQL of a `superset_dataset`. Superset runs the query when the dataset
  is created, so without the flag a query that contains `{{ ... }}` fails to create the dataset.
* `SSH_TUNNELING`: the `ssh_tunnel` of a `superset_database`.
* `EMBEDDED_SUPERSET`: guest tokens for a dashboard made embeddable with `superset_dashboard_embedding`.
* `DASHBOARD_RBAC`: the `roles` of a `superset_dashboard` take effect only with it.
* `ALERT_REPORTS`: the `superset_report` resource. Without the flag, the report API answers 404 to every request.
* `TAGGING_SYSTEM`: the `superset_tag` resource and the `tags` of `superset_chart` and `superset_dashboard`. The provider only
  attaches the tags an object is missing and detaches a tag before deleting it, because Superset 3.1 leaves broken relations
  behind otherwise. Superset 3.1 has a bug of its own with the flag on: it fails to delete a chart or a dashboard whenever
  another dataset, chart or dashboard has the same numeric id, which is common, because the relations of its own `owner:` and
  `type:` tags are matched by id only. Keep the flag off on Superset 3.1.

## Declaring the provider where Superset is not available

Terraform configures every provider declared in a configuration, even if it manages no resources.
To declare the provider in a module that is also used where Superset is not deployed, disable the credentials validation:

```terraform
# The provider is declared, but Superset is not available in this environment:
# no Superset resources are managed, so nothing is validated and no request is sent.
provider "superset" {
  validate_credentials = false
}
```

In this mode `host`, `username` and `password` may be empty and the provider sends no request.
Any resource or data source that is used anyway fails with an error that lists the missing settings.

## CSRF protection

Superset protects its own API endpoints (databases, datasets and others) with a CSRF check when `WTF_CSRF_ENABLED = True`,
which is the default. A write request that carries only the access token is rejected with `400 Bad Request: The CSRF token is missing.`

With `csrf_enabled = true` (the default) the provider requests a token from `/api/v1/security/csrf_token/` before every
create, update and delete request and sends it in the `X-CSRFToken` header together with the session cookie.
This also works on an instance where CSRF protection is disabled, at the cost of one extra request per write operation.

To skip that request on an instance that runs with `WTF_CSRF_ENABLED = False`, disable it:

```terraform
# Superset runs with WTF_CSRF_ENABLED = False:
# no CSRF token is requested and write requests carry the access token only.
provider "superset" {
  host         = "https://domain.com"
  username     = "username"
  password     = "password"
  csrf_enabled = false
}
```

With `csrf_enabled = false` write requests to a Superset instance that enforces CSRF fail with the error above.

<!-- schema generated by tfplugindocs -->
## Schema

### Optional

- `auth_provider` (String) Authentication provider named in the login request: `db` (the default) for users of the Superset database, `ldap` for an instance with `AUTH_TYPE = AUTH_LDAP`. Can also be set with the `SUPERSET_AUTH_PROVIDER` environment variable.
- `csrf_enabled` (Boolean) Whether to request a CSRF token from Superset and send it with every write request. Defaults to `true`, which is required by a Superset instance with the default `WTF_CSRF_ENABLED = True`. Set it to `false` for an instance where CSRF protection is disabled to skip the extra request before each write operation.
- `host` (String) The URL of the Superset instance. This should include the protocol (http or https) and the hostname or IP address. Example: 'https://superset.example.com'.
- `password` (String, Sensitive) The password to authenticate with Superset. This value is sensitive and will not be displayed in logs or state files.
- `username` (String) The username to authenticate with Superset. This user should have the necessary permissions to manage resources within Superset.
- `validate_credentials` (Boolean) Whether to validate the connection settings and log in to Superset when the provider is configured. Defaults to `true`. When `false`, the provider does not send any request until a resource or data source calls the Superset API, and `host`, `username` and `password` may be left empty. This allows declaring the provider in configurations where Superset is not available and no Superset resources are managed.
