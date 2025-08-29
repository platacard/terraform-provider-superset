## 0.2.0 (2025-08-25)

FEATURES:
* **New Resource**: `superset_meta_database` - Support for Superset meta database connections for cross-database queries
* **New Resource**: `superset_dataset` - Manage individual Superset datasets
* **New Data Source**: `superset_datasets` - Fetch all datasets from Superset 
 
IMPROVEMENTS:
* Added comprehensive test coverage for meta database resource
* Added schema-level default values for boolean attributes
* Added global caching for database API calls to improve performance across multiple client instances
* Added pagination support (page_size:5000) to datasets API calls
  