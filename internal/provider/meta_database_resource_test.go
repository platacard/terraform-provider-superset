package provider

import (
	"net/http"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/jarcoal/httpmock"
)

func TestAccMetaDatabaseResource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	// Clear any existing responders to avoid conflicts
	httpmock.Reset()

	// Define responses that change based on request context
	var currentState string = "initial"

	// Mock the Superset API login response
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))

	// Mock the Superset API CSRF token response
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/csrf_token/",
		httpmock.NewStringResponder(200, `{"result": "fake-csrf-token"}`))

	// Mock the search for existing meta database (not found initially)
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/database/",
		httpmock.NewStringResponder(200, `{
			"result": []
		}`))

	// Mock the Superset API response for creating a meta database
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/database/", func(req *http.Request) (*http.Response, error) {
		currentState = "created"
		return httpmock.NewStringResponse(201, `{
			"id": 350,
			"result": {
				"id": 350,
				"database_name": "TestMetaConnection",
				"engine": "superset",
				"configuration_method": "sqlalchemy_form",
				"sqlalchemy_uri": "superset://",
				"expose_in_sqllab": true,
				"allow_ctas": false,
				"allow_cvas": false,
				"allow_dml": false,
				"allow_run_async": true,
				"is_managed_externally": false,
				"extra": "{\"metadata_params\": {}, \"engine_params\": {\"allowed_dbs\": [\"db1\", \"db2\"]}, \"metadata_cache_timeout\": {}, \"schemas_allowed_for_csv_upload\": []}"
			}
		}`), nil
	})

	// Mock the Superset API response for reading a meta database (state-dependent)
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/database/350", func(req *http.Request) (*http.Response, error) {
		if currentState == "updated" {
			return httpmock.NewStringResponse(200, `{
				"result": {
					"id": 350,
					"database_name": "TestMetaConnection",
					"engine": "superset",
					"configuration_method": "sqlalchemy_form",
					"sqlalchemy_uri": "superset://",
					"expose_in_sqllab": false,
					"allow_ctas": false,
					"allow_cvas": false,
					"allow_dml": false,
					"allow_run_async": true,
					"is_managed_externally": false,
					"extra": "{\"metadata_params\": {}, \"engine_params\": {\"allowed_dbs\": [\"db1\", \"db2\", \"db3\"]}, \"metadata_cache_timeout\": {}, \"schemas_allowed_for_csv_upload\": []}"
				}
			}`), nil
		}
		// Default state (created)
		return httpmock.NewStringResponse(200, `{
			"result": {
				"id": 350,
				"database_name": "TestMetaConnection",
				"engine": "superset",
				"configuration_method": "sqlalchemy_form",
				"sqlalchemy_uri": "superset://",
				"expose_in_sqllab": true,
				"allow_ctas": false,
				"allow_cvas": false,
				"allow_dml": false,
				"allow_run_async": true,
				"is_managed_externally": false,
				"extra": "{\"metadata_params\": {}, \"engine_params\": {\"allowed_dbs\": [\"db1\", \"db2\"]}, \"metadata_cache_timeout\": {}, \"schemas_allowed_for_csv_upload\": []}"
			}
		}`), nil
	})

	// Mock the Superset API response for updating a meta database
	httpmock.RegisterResponder("PUT", "http://superset-host/api/v1/database/350", func(req *http.Request) (*http.Response, error) {
		currentState = "updated"
		return httpmock.NewStringResponse(200, `{
			"result": {
				"id": 350,
				"database_name": "TestMetaConnection",
				"engine": "superset",
				"configuration_method": "sqlalchemy_form",
				"sqlalchemy_uri": "superset://",
				"expose_in_sqllab": false,
				"allow_ctas": false,
				"allow_cvas": false,
				"allow_dml": false,
				"allow_run_async": true,
				"is_managed_externally": false,
				"extra": "{\"metadata_params\": {}, \"engine_params\": {\"allowed_dbs\": [\"db1\", \"db2\", \"db3\"]}, \"metadata_cache_timeout\": {}, \"schemas_allowed_for_csv_upload\": []}"
			}
		}`), nil
	})

	// Mock the Superset API response for deleting a meta database
	httpmock.RegisterResponder("DELETE", "http://superset-host/api/v1/database/350",
		httpmock.NewStringResponder(200, ""))

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			// Create and Read testing
			{
				Config: providerConfig + testAccMetaDatabaseResourceConfig,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_meta_database.test", "database_name", "TestMetaConnection"),
					resource.TestCheckResourceAttr("superset_meta_database.test", "sqlalchemy_uri", "superset://"),
					resource.TestCheckResourceAttr("superset_meta_database.test", "allowed_databases.#", "2"),
					resource.TestCheckResourceAttr("superset_meta_database.test", "allowed_databases.0", "db1"),
					resource.TestCheckResourceAttr("superset_meta_database.test", "allowed_databases.1", "db2"),
					resource.TestCheckResourceAttr("superset_meta_database.test", "expose_in_sqllab", "true"),
					resource.TestCheckResourceAttr("superset_meta_database.test", "allow_ctas", "false"),
					resource.TestCheckResourceAttr("superset_meta_database.test", "allow_cvas", "false"),
					resource.TestCheckResourceAttr("superset_meta_database.test", "allow_dml", "false"),
					resource.TestCheckResourceAttr("superset_meta_database.test", "allow_run_async", "true"),
					resource.TestCheckResourceAttr("superset_meta_database.test", "is_managed_externally", "false"),
					resource.TestCheckResourceAttrSet("superset_meta_database.test", "id"),
				),
			},
			// ImportState testing
			{
				ResourceName:      "superset_meta_database.test",
				ImportState:       true,
				ImportStateVerify: true,
			},
			// Update and Read testing
			{
				Config: providerConfig + testAccMetaDatabaseResourceConfigUpdated,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_meta_database.test", "database_name", "TestMetaConnection"),
					resource.TestCheckResourceAttr("superset_meta_database.test", "sqlalchemy_uri", "superset://"),
					resource.TestCheckResourceAttr("superset_meta_database.test", "allowed_databases.#", "3"),
					resource.TestCheckResourceAttr("superset_meta_database.test", "allowed_databases.0", "db1"),
					resource.TestCheckResourceAttr("superset_meta_database.test", "allowed_databases.1", "db2"),
					resource.TestCheckResourceAttr("superset_meta_database.test", "allowed_databases.2", "db3"),
					resource.TestCheckResourceAttr("superset_meta_database.test", "expose_in_sqllab", "false"),
					resource.TestCheckResourceAttr("superset_meta_database.test", "allow_ctas", "false"),
					resource.TestCheckResourceAttr("superset_meta_database.test", "allow_cvas", "false"),
					resource.TestCheckResourceAttr("superset_meta_database.test", "allow_dml", "false"),
					resource.TestCheckResourceAttr("superset_meta_database.test", "allow_run_async", "true"),
					resource.TestCheckResourceAttr("superset_meta_database.test", "is_managed_externally", "false"),
				),
			},
			// Delete testing automatically occurs in TestCase
		},
	})
}

func TestAccMetaDatabaseResourceExisting(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	// Clear any existing responders to avoid conflicts
	httpmock.Reset()

	// Mock the Superset API login response
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))

	// Mock the Superset API CSRF token response
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/csrf_token/",
		httpmock.NewStringResponder(200, `{"result": "fake-csrf-token"}`))

	// Mock the search for existing meta database (found)
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/database/",
		httpmock.NewStringResponder(200, `{
			"result": [
				{
					"id": 351,
					"database_name": "ExistingMetaConnection",
					"sqlalchemy_uri": "superset://"
				}
			]
		}`))

	// Mock the detailed get for existing meta database
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/database/351",
		httpmock.NewStringResponder(200, `{
			"result": {
				"id": 351,
				"database_name": "ExistingMetaConnection",
				"engine": "superset",
				"configuration_method": "sqlalchemy_form",
				"sqlalchemy_uri": "superset://",
				"expose_in_sqllab": true,
				"allow_ctas": false,
				"allow_cvas": false,
				"allow_dml": false,
				"allow_run_async": true,
				"is_managed_externally": false,
				"extra": "{\"metadata_params\": {}, \"engine_params\": {\"allowed_dbs\": [\"existing_db1\"]}, \"metadata_cache_timeout\": {}, \"schemas_allowed_for_csv_upload\": []}"
			}
		}`))

	// Mock the create call for new meta database (since this test creates, not imports)
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/database/",
		httpmock.NewStringResponder(201, `{
			"id": 351,
			"result": {
				"id": 351,
				"database_name": "ExistingMetaConnection",
				"engine": "superset",
				"configuration_method": "sqlalchemy_form",
				"sqlalchemy_uri": "superset://",
				"expose_in_sqllab": true,
				"allow_ctas": false,
				"allow_cvas": false,
				"allow_dml": false,
				"allow_run_async": true,
				"is_managed_externally": false,
				"extra": "{\"metadata_params\": {}, \"engine_params\": {\"allowed_dbs\": [\"new_db1\", \"new_db2\"]}, \"metadata_cache_timeout\": {}, \"schemas_allowed_for_csv_upload\": []}"
			}
		}`))

	// Mock the update call for the existing meta database
	httpmock.RegisterResponder("PUT", "http://superset-host/api/v1/database/351",
		httpmock.NewStringResponder(200, `{
			"result": {
				"id": 351,
				"database_name": "ExistingMetaConnection",
				"engine": "superset",
				"configuration_method": "sqlalchemy_form",
				"sqlalchemy_uri": "superset://",
				"expose_in_sqllab": true,
				"allow_ctas": false,
				"allow_cvas": false,
				"allow_dml": false,
				"allow_run_async": true,
				"is_managed_externally": false,
				"extra": "{\"metadata_params\": {}, \"engine_params\": {\"allowed_dbs\": [\"new_db1\", \"new_db2\"]}, \"metadata_cache_timeout\": {}, \"schemas_allowed_for_csv_upload\": []}"
			}
		}`))

	// Mock the read response after update for existing database
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/database/351",
		httpmock.NewStringResponder(200, `{
			"result": {
				"id": 351,
				"database_name": "ExistingMetaConnection",
				"engine": "superset",
				"configuration_method": "sqlalchemy_form",
				"sqlalchemy_uri": "superset://",
				"expose_in_sqllab": true,
				"allow_ctas": false,
				"allow_cvas": false,
				"allow_dml": false,
				"allow_run_async": true,
				"is_managed_externally": false,
				"extra": "{\"metadata_params\": {}, \"engine_params\": {\"allowed_dbs\": [\"new_db1\", \"new_db2\"]}, \"metadata_cache_timeout\": {}, \"schemas_allowed_for_csv_upload\": []}"
			}
		}`))

	// Mock delete
	httpmock.RegisterResponder("DELETE", "http://superset-host/api/v1/database/351",
		httpmock.NewStringResponder(200, ""))

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			// Test importing existing meta database
			{
				Config: providerConfig + testAccMetaDatabaseResourceConfigExisting,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_meta_database.existing", "database_name", "ExistingMetaConnection"),
					resource.TestCheckResourceAttr("superset_meta_database.existing", "sqlalchemy_uri", "superset://"),
					resource.TestCheckResourceAttr("superset_meta_database.existing", "allowed_databases.#", "2"),
					resource.TestCheckResourceAttr("superset_meta_database.existing", "allowed_databases.0", "new_db1"),
					resource.TestCheckResourceAttr("superset_meta_database.existing", "allowed_databases.1", "new_db2"),
					resource.TestCheckResourceAttrSet("superset_meta_database.existing", "id"),
				),
			},
		},
	})
}

func TestAccMetaDatabaseResourceDefaults(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	// Clear any existing responders to avoid conflicts
	httpmock.Reset()

	// Mock the Superset API login response
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))

	// Mock the Superset API CSRF token response
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/csrf_token/",
		httpmock.NewStringResponder(200, `{"result": "fake-csrf-token"}`))

	// Mock the search for existing meta database (not found)
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/database/",
		httpmock.NewStringResponder(200, `{
			"result": []
		}`))

	// Mock both create and read response with default values applied
	defaultsResponse := `{
		"id": 352,
		"result": {
			"id": 352,
			"database_name": "DefaultsMetaConnection",
			"engine": "superset",
			"configuration_method": "sqlalchemy_form",
			"sqlalchemy_uri": "superset://",
			"expose_in_sqllab": true,
			"allow_ctas": false,
			"allow_cvas": false,
			"allow_dml": false,
			"allow_run_async": true,
			"is_managed_externally": false,
			"extra": "{\"metadata_params\": {}, \"engine_params\": {\"allowed_dbs\": [\"minimal_db\"]}, \"metadata_cache_timeout\": {}, \"schemas_allowed_for_csv_upload\": []}"
		}
	}`

	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/database/",
		httpmock.NewStringResponder(201, defaultsResponse))

	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/database/352", func(req *http.Request) (*http.Response, error) {
		// This should return the values with defaults applied
		return httpmock.NewStringResponse(200, `{
			"result": {
				"id": 352,
				"database_name": "DefaultsMetaConnection",
				"engine": "superset",
				"configuration_method": "sqlalchemy_form",
				"sqlalchemy_uri": "superset://",
				"expose_in_sqllab": true,
				"allow_ctas": false,
				"allow_cvas": false,
				"allow_dml": false,
				"allow_run_async": true,
				"is_managed_externally": false,
				"extra": "{\"metadata_params\": {}, \"engine_params\": {\"allowed_dbs\": [\"minimal_db\"]}, \"metadata_cache_timeout\": {}, \"schemas_allowed_for_csv_upload\": []}"
			}
		}`), nil
	})

	// Mock delete
	httpmock.RegisterResponder("DELETE", "http://superset-host/api/v1/database/352",
		httpmock.NewStringResponder(200, ""))

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			// Test with minimal configuration (using defaults)
			{
				Config: providerConfig + testAccMetaDatabaseResourceConfigMinimal,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_meta_database.minimal", "database_name", "DefaultsMetaConnection"),
					resource.TestCheckResourceAttr("superset_meta_database.minimal", "sqlalchemy_uri", "superset://"),
					resource.TestCheckResourceAttr("superset_meta_database.minimal", "allowed_databases.#", "1"),
					resource.TestCheckResourceAttr("superset_meta_database.minimal", "allowed_databases.0", "minimal_db"),
					// Check default values
					resource.TestCheckResourceAttr("superset_meta_database.minimal", "expose_in_sqllab", "true"),
					resource.TestCheckResourceAttr("superset_meta_database.minimal", "allow_ctas", "false"),
					resource.TestCheckResourceAttr("superset_meta_database.minimal", "allow_cvas", "false"),
					resource.TestCheckResourceAttr("superset_meta_database.minimal", "allow_dml", "false"),
					resource.TestCheckResourceAttr("superset_meta_database.minimal", "allow_run_async", "true"),
					resource.TestCheckResourceAttr("superset_meta_database.minimal", "is_managed_externally", "false"),
					resource.TestCheckResourceAttrSet("superset_meta_database.minimal", "id"),
				),
			},
		},
	})
}

const testAccMetaDatabaseResourceConfig = `
resource "superset_meta_database" "test" {
  database_name = "TestMetaConnection"
  sqlalchemy_uri = "superset://"

  allowed_databases = [
    "db1",
    "db2"
  ]
  
  expose_in_sqllab      = true
  allow_ctas            = false
  allow_cvas            = false
  allow_dml             = false
  allow_run_async       = true
  is_managed_externally = false
}
`

const testAccMetaDatabaseResourceConfigUpdated = `
resource "superset_meta_database" "test" {
  database_name = "TestMetaConnection"
  sqlalchemy_uri = "superset://"

  allowed_databases = [
    "db1",
    "db2",
    "db3"
  ]
  
  expose_in_sqllab      = false
  allow_ctas            = false
  allow_cvas            = false
  allow_dml             = false
  allow_run_async       = true
  is_managed_externally = false
}
`

const testAccMetaDatabaseResourceConfigExisting = `
resource "superset_meta_database" "existing" {
  database_name = "ExistingMetaConnection"
  sqlalchemy_uri = "superset://"

  allowed_databases = [
    "new_db1",
    "new_db2"
  ]
  
  expose_in_sqllab      = true
  allow_ctas            = false
  allow_cvas            = false
  allow_dml             = false
  allow_run_async       = true
  is_managed_externally = false
}
`

const testAccMetaDatabaseResourceConfigMinimal = `
resource "superset_meta_database" "minimal" {
  database_name = "DefaultsMetaConnection"

  allowed_databases = [
    "minimal_db"
  ]
}
`
