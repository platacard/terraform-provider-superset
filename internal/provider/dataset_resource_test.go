package provider

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/jarcoal/httpmock"
	"terraform-provider-superset/internal/client"
)

func TestAccDatasetResource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	// Clear the global database cache to ensure our mocks are used
	client.ClearGlobalDatabaseCache()

	// Mock authentication response
	mockLoginResponse := `{
		"access_token": "fake-token",
		"refresh_token": "fake-refresh-token"
	}`

	// Mock database list response (make it match what the test framework expects)
	mockDatabasesResponse := `{
		"result": [
			{
				"id": 1,
				"database_name": "PostgreSQL Database",
				"backend": "postgresql"
			},
			{
				"id": 2,
				"database_name": "MySQL Database",
				"backend": "mysql"
			},
			{
				"id": 3,
				"database_name": "SQLite Database",
				"backend": "sqlite"
			},
			{
				"id": 4,
				"database_name": "Test Database 1",
				"backend": "postgresql"
			},
			{
				"id": 5,
				"database_name": "Test Database 2",
				"backend": "mysql"
			},
			{
				"id": 6,
				"database_name": "Test Database 3",
				"backend": "sqlite"
			}
		]
	}`

	// Mock dataset creation response
	mockDatasetCreateResponse := `{
		"id": 123,
		"table_name": "test_table",
		"database": {
			"id": 1,
			"database_name": "PostgreSQL Database"
		},
		"schema": "public"
	}`

	// Mock dataset read response - initial version
	mockDatasetReadResponseInitial := `{
		"result": {
			"id": 123,
			"table_name": "test_table",
			"database": {
				"id": 1,
				"database_name": "PostgreSQL Database"
			},
			"schema": "public",
			"sql": null
		}
	}`

	// Mock dataset read response - after update
	mockDatasetReadResponseUpdated := `{
		"result": {
			"id": 123,
			"table_name": "updated_table",
			"database": {
				"id": 1,
				"database_name": "PostgreSQL Database"
			},
			"schema": "updated_schema",
			"sql": null
		}
	}`

	// Mock dataset update response (PUT returns empty on success)
	mockDatasetUpdateResponse := `{}`

	// Setup mocks
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, mockLoginResponse))

	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/database/?q=(page_size:5000)",
		httpmock.NewStringResponder(200, mockDatabasesResponse))

	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/dataset/",
		httpmock.NewStringResponder(201, mockDatasetCreateResponse))

	// Setup dynamic GET responder that returns different responses based on call count
	callCount := 0
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/dataset/123",
		func(req *http.Request) (*http.Response, error) {
			callCount++
			if callCount <= 2 { // First two calls return initial values
				return httpmock.NewStringResponse(200, mockDatasetReadResponseInitial), nil
			} else { // Subsequent calls return updated values
				return httpmock.NewStringResponse(200, mockDatasetReadResponseUpdated), nil
			}
		})

	httpmock.RegisterResponder("PUT", "http://superset-host/api/v1/dataset/123",
		httpmock.NewStringResponder(200, mockDatasetUpdateResponse))

	httpmock.RegisterResponder("DELETE", "http://superset-host/api/v1/dataset/123",
		httpmock.NewStringResponder(200, "{}"))

	resource.Test(t, resource.TestCase{
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			// Create and Read testing
			{
				Config: testAccDatasetResourceConfig("test_table", "PostgreSQL Database", "public"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_dataset.test", "table_name", "test_table"),
					resource.TestCheckResourceAttr("superset_dataset.test", "database_name", "PostgreSQL Database"),
					resource.TestCheckResourceAttr("superset_dataset.test", "schema", "public"),
					resource.TestCheckResourceAttrSet("superset_dataset.test", "id"),
				),
			},
			// ImportState testing
			{
				ResourceName:      "superset_dataset.test",
				ImportState:       true,
				ImportStateVerify: true,
				ImportStateId:     "123",
			},
			// Update and Read testing
			{
				Config: testAccDatasetResourceConfig("updated_table", "PostgreSQL Database", "updated_schema"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_dataset.test", "table_name", "updated_table"),
					resource.TestCheckResourceAttr("superset_dataset.test", "database_name", "PostgreSQL Database"),
					resource.TestCheckResourceAttr("superset_dataset.test", "schema", "updated_schema"),
				),
			},
			// Delete testing automatically occurs in TestCase
		},
	})
}

func TestAccDatasetResourceWithSQL(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	// Clear the global database cache to ensure our mocks are used
	client.ClearGlobalDatabaseCache()

	// Mock authentication response
	mockLoginResponse := `{
		"access_token": "fake-token",
		"refresh_token": "fake-refresh-token"
	}`

	// Mock database list response (same as first test)
	mockDatabasesResponse := `{
		"result": [
			{
				"id": 1,
				"database_name": "PostgreSQL Database",
				"backend": "postgresql"
			},
			{
				"id": 2,
				"database_name": "MySQL Database",
				"backend": "mysql"
			},
			{
				"id": 3,
				"database_name": "SQLite Database",
				"backend": "sqlite"
			},
			{
				"id": 4,
				"database_name": "Test Database 1",
				"backend": "postgresql"
			},
			{
				"id": 5,
				"database_name": "Test Database 2",
				"backend": "mysql"
			},
			{
				"id": 6,
				"database_name": "Test Database 3",
				"backend": "sqlite"
			}
		]
	}`

	// Mock dataset creation response with SQL
	mockDatasetCreateResponse := `{
		"id": 124,
		"table_name": "sql_dataset",
		"database": {
			"id": 1,
			"database_name": "PostgreSQL Database"
		},
		"sql": "SELECT * FROM users"
	}`

	// Mock dataset read response with SQL
	mockDatasetReadResponse := `{
		"result": {
			"id": 124,
			"table_name": "sql_dataset",
			"database": {
				"id": 1,
				"database_name": "PostgreSQL Database"
			},
			"schema": null,
			"sql": "SELECT * FROM users"
		}
	}`

	// Setup mocks
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, mockLoginResponse))

	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/database/?q=(page_size:5000)",
		httpmock.NewStringResponder(200, mockDatabasesResponse))

	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/dataset/",
		httpmock.NewStringResponder(201, mockDatasetCreateResponse))

	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/dataset/124",
		httpmock.NewStringResponder(200, mockDatasetReadResponse))

	httpmock.RegisterResponder("DELETE", "http://superset-host/api/v1/dataset/124",
		httpmock.NewStringResponder(200, "{}"))

	resource.Test(t, resource.TestCase{
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			// Create and Read testing with SQL
			{
				Config: testAccDatasetResourceConfigWithSQL("sql_dataset", "PostgreSQL Database", "SELECT * FROM users"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_dataset.test", "table_name", "sql_dataset"),
					resource.TestCheckResourceAttr("superset_dataset.test", "database_name", "PostgreSQL Database"),
					resource.TestCheckResourceAttr("superset_dataset.test", "sql", "SELECT * FROM users"),
					resource.TestCheckResourceAttrSet("superset_dataset.test", "id"),
				),
			},
		},
	})
}

func testAccDatasetResourceConfig(tableName, databaseName, schema string) string {
	return fmt.Sprintf(`
resource "superset_dataset" "test" {
  table_name    = %[1]q
  database_name = %[2]q
  schema        = %[3]q
}
`, tableName, databaseName, schema)
}

func testAccDatasetResourceConfigWithSQL(tableName, databaseName, sql string) string {
	return fmt.Sprintf(`
resource "superset_dataset" "test" {
  table_name    = %[1]q
  database_name = %[2]q
  sql           = %[3]q
}
`, tableName, databaseName, sql)
}
