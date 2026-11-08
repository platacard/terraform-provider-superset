## 0.6.0 (Unreleased)

FEATURES:
* **New Data Source**: `superset_role` - Look up a role by name, for example to reference its id in `superset_user.roles`.

IMPROVEMENTS:
* provider: Lists (roles, users, databases, datasets, permissions) are read page by page, so instances with more entries than the server's page size limit (100 by default) are handled completely instead of being cut off.
* resource/superset_role_permissions, data-source/superset_role_permissions: Roles are looked up by name with a filtered request, and permissions are resolved with two small requests per view menu, run concurrently and cached for the run, instead of scanning the whole permission catalogue, which on large instances could not be fetched completely. Permissions are sent to Superset in the order of the configuration.

BUG FIXES:
* resource/superset_user: Reading the user no longer reorders `roles` to the order returned by Superset, which made every `terraform plan` report an update when the configured order differed.
* resource/superset_role: A failing role lookup (for example a network error) is reported instead of being treated as "role does not exist" and followed by a creation attempt.

## 0.5.0 (2026-11-08)

FEATURES:
* provider: New `csrf_enabled` attribute (defaults to `true`). When `false`, the provider does not request a CSRF token before write requests, for Superset instances that run with `WTF_CSRF_ENABLED = False`.

BUG FIXES:
* resource/superset_dataset, resource/superset_role, resource/superset_role_permissions, resource/superset_user: Write requests now carry the CSRF token, like the database resources already did, so they work on a Superset instance with the default `WTF_CSRF_ENABLED = True`.
* resource/superset_role_permissions: Reading the resource no longer reorders `resource_permissions` to the order returned by the API, which made every `terraform plan` report an update even when nothing had changed. `last_updated` is only set on create and update.

## 0.4.0 (2026-10-05)

FEATURES:
* provider: New `validate_credentials` attribute (defaults to `true`). When `false`, the provider does not log in on configure and `host`, `username` and `password` may be empty, so it can be declared where Superset is not available.

IMPROVEMENTS:
* provider: The API client logs in lazily on the first request and shares one access token between concurrent operations.
* provider: An expired access token (401) is refreshed once and the request is retried, instead of failing long-running applies.

## 0.3.0 (2025-11-14)

FEATURES:
* **New Resource**: `superset_user` - Manage Superset users
* **New Data Source**: `superset_users` - Fetch users from Superset

## 0.2.0 (2025-08-29)

FEATURES:
* **New Resource**: `superset_meta_database` - Support for Superset meta database connections for cross-database queries
* **New Resource**: `superset_dataset` - Manage individual Superset datasets
* **New Data Source**: `superset_datasets` - Fetch all datasets from Superset 
 
IMPROVEMENTS:
* Added comprehensive test coverage for meta database resource
* Added schema-level default values for boolean attributes
* Added global caching for database API calls to improve performance across multiple client instances
* Added pagination support (page_size:5000) to datasets API calls
  