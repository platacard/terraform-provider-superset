## 0.4.0 (Unreleased)

FEATURES:
* provider: New `validate_credentials` attribute (defaults to `true`). When `false`, the provider does not log in on configure and `host`, `username` and `password` may be empty, so it can be declared where Superset is not available.

IMPROVEMENTS:
* provider: The API client logs in lazily on the first request and shares one access token between concurrent operations.
* provider: An expired access token (401) is refreshed once and the request is retried, instead of failing long-running applies.

## 0.3.0 (TBD)

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
  