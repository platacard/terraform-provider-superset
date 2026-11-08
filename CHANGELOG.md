## 0.7.0 (Unreleased)

FEATURES:
* **New Resource**: `superset_report` - Manage the alerts and reports of the Alerts & Reports page: a chart or a dashboard sent to email or Slack recipients on a schedule, or when a SQL query meets a condition.
* **New Resource**: `superset_tag` - Manage tags, with the `TAGGING_SYSTEM` feature flag.
* resource/superset_chart, resource/superset_dashboard: New `tags` attribute with the names of the tags attached to the object; when unset, the tags are left as they are.
* **New Resource**: `superset_annotation` - Manage the annotations of an annotation layer: events or periods that charts show on their time axis.
* **New Data Source**: `superset_database`, `superset_dataset`, `superset_chart`, `superset_dashboard` - Look one object up by its name (the slug or title for a dashboard, the table name, schema and database for a dataset), to reference objects that Terraform does not manage.
* **New Resource**: `superset_dataset_refresh` - Sync the columns of a dataset from its source, as the dataset editor does, when the resource is created and whenever its `triggers` change.

IMPROVEMENTS:
* resource/superset_dataset: New `columns` attribute managing the columns of the dataset (name, type, expression of a calculated column, label, description, datetime flag and format, `groupby`, `filterable`); when unset, the columns are left to Superset, and within a column only the attributes that are set are written, so the detected type is kept.
* resource/superset_dataset: New `metrics` attribute managing the metrics of the dataset (name, expression, label, description, D3 format, warning, type); when unset, the metrics are left to Superset.
* resource/superset_dataset: New attributes `description`, `owners`, `cache_timeout`, `main_dttm_col`, `template_params`, `extra`, `fetch_values_predicate`, `filter_select_enabled`, `normalize_columns`, `always_filter_main_dttm` and `default_endpoint`. A change of `database_name` now replaces the dataset instead of being ignored, and a dataset deleted outside Terraform is recreated instead of failing the plan.
* resource/superset_dataset: New computed `perm` and `schema_perm` attributes with the view menus of the dataset and schema permissions, for `superset_role_permissions`.
* provider: When Superset does not expose the users, roles or permissions endpoints, the error explains that `FAB_ADD_SECURITY_API = True` is required instead of showing a raw 404, and such a 404 is no longer mistaken for a deleted object.

## 0.6.0 (2026-11-08)

FEATURES:
* provider: New `auth_provider` attribute (`db`, the default, or `ldap`), also settable with `SUPERSET_AUTH_PROVIDER`, naming the authentication provider of the login for instances that authenticate against LDAP.
* **New Resource**: `superset_chart` - Manage charts.
* **New Resource**: `superset_dashboard` - Manage dashboards, their settings, their layout and the charts placed on them.
* **New Resource**: `superset_dashboard_embedding` - Make a dashboard embeddable with the Superset embedded SDK.
* **New Resource**: `superset_dashboard_import`, `superset_chart_import`, `superset_dataset_import` - Import export bundles (a directory in the export layout or a zip file) and import them again whenever their files change.
* **New Resource**: `superset_saved_query` - Manage queries saved in SQL Lab.
* **New Resource**: `superset_annotation_layer` - Manage annotation layers.
* **New Resource**: `superset_row_level_security` - Manage row level security rules (SQL clauses applied to datasets for roles).
* **New Resource**: `superset_css_template` - Manage CSS templates that dashboards can apply.
* **New Data Source**: `superset_css_template` - Look up a CSS template by name.
* **New Data Source**: `superset_role` - Look up a role by name, for example to reference its id in `superset_user.roles`.

IMPROVEMENTS:
* provider: A failed login reports the response of Superset, for example `Alternative authentication provider is not allowed`, instead of the status code alone.
* resource/superset_database: The connection can be given as a full `sqlalchemy_uri` instead of its parts, and the resource manages `allow_file_upload`, `impersonate_user`, `cache_timeout`, `force_ctas_schema`, `extra`, `masked_encrypted_extra`, `server_cert` and an `ssh_tunnel`. A host or tunnel server changed in the UI is reported as drift.
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
  