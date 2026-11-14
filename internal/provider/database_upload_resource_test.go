package provider

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/plancheck"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
	"github.com/jarcoal/httpmock"
)

// fakeUploads serves the upload endpoint of database 3 (Warehouse), the tables of its public
// schema, the dataset Superset creates for an uploaded table (77) and the SQL Lab drop. The
// schema "secret" is not allowed for uploads.
type fakeUploads struct {
	mu       sync.Mutex
	tables   map[string]bool
	uploads  []map[string]string
	files    []string
	contents []string
	dropped  []string
	deleted  []int64
}

func (f *fakeUploads) serve() {
	f.tables = map[string]bool{}
	serveLookups()
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/csrf_token/",
		httpmock.NewStringResponder(200, `{"result": "fake-csrf-token"}`))
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/database/3/upload/", func(req *http.Request) (*http.Response, error) {
		if err := req.ParseMultipartForm(1 << 20); err != nil {
			return httpmock.NewStringResponse(400, err.Error()), nil
		}
		fields := map[string]string{}
		for key, values := range req.MultipartForm.Value {
			fields[key] = values[0]
		}
		file, header, err := req.FormFile("file")
		if err != nil {
			return httpmock.NewStringResponse(400, err.Error()), nil
		}
		content, _ := io.ReadAll(file)
		f.mu.Lock()
		defer f.mu.Unlock()
		f.uploads = append(f.uploads, fields)
		f.files = append(f.files, header.Filename)
		f.contents = append(f.contents, string(content))
		if fields["schema"] == "secret" {
			return httpmock.NewStringResponse(403, `{"errors": [{"message": "Database schema is not allowed for csv uploads.", "error_type": "GENERIC_COMMAND_ERROR", "level": "warning"}]}`), nil
		}
		if f.tables[fields["table_name"]] && fields["already_exists"] == "fail" {
			return httpmock.NewStringResponse(422, `{"errors": [{"message": "Table already exists. You can change your 'if table already exists' strategy to append or replace or provide a different Table Name to use.", "error_type": "GENERIC_COMMAND_ERROR", "level": "warning"}]}`), nil
		}
		f.tables[fields["table_name"]] = true
		return httpmock.NewStringResponse(201, `{"message": "OK"}`), nil
	})
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/database/3/tables/", func(req *http.Request) (*http.Response, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		rows := []string{}
		for table := range f.tables {
			rows = append(rows, fmt.Sprintf(`{"value": %q, "type": "table", "extra": null}`, table))
		}
		return httpmock.NewStringResponse(200, fmt.Sprintf(`{"count": %d, "result": [%s]}`, len(rows), strings.Join(rows, ","))), nil
	})
	httpmock.RegisterResponder("GET", listEndpoint("/api/v1/dataset/"), func(req *http.Request) (*http.Response, error) {
		q := req.URL.Query().Get("q")
		f.mu.Lock()
		defer f.mu.Unlock()
		for table := range f.tables {
			if strings.Contains(q, fmt.Sprintf("(col:table_name,opr:eq,value:'%s'),(col:schema,opr:eq,value:'public'),(col:database,opr:rel_o_m,value:3)", table)) {
				return httpmock.NewStringResponse(200, fmt.Sprintf(`{"count": 1, "result": [{"id": 77, "table_name": %q, "schema": "public", "sql": null, "kind": "physical", "database": {"id": 3, "database_name": "Warehouse"}}]}`, table)), nil
			}
		}
		return httpmock.NewStringResponse(200, `{"count": 0, "result": []}`), nil
	})
	httpmock.RegisterResponder("DELETE", "http://superset-host/api/v1/dataset/77", func(*http.Request) (*http.Response, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.deleted = append(f.deleted, 77)
		return httpmock.NewStringResponse(200, `{"message": "OK"}`), nil
	})
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/sqllab/execute/", func(req *http.Request) (*http.Response, error) {
		body, _ := io.ReadAll(req.Body)
		var payload map[string]any
		_ = json.Unmarshal(body, &payload)
		f.mu.Lock()
		defer f.mu.Unlock()
		sql, _ := payload["sql"].(string)
		f.dropped = append(f.dropped, sql)
		return httpmock.NewStringResponse(200, `{"query_id": 9, "status": "success", "columns": [], "data": [], "query": {"limit": 1, "limitingFactor": "NOT_LIMITED"}}`), nil
	})
}

func (f *fakeUploads) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.uploads)
}

func (f *fakeUploads) last() (map[string]string, string, string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.uploads) == 0 {
		return nil, "", ""
	}
	return f.uploads[len(f.uploads)-1], f.files[len(f.files)-1], f.contents[len(f.contents)-1]
}

const uploadCSV = "code,name,valid_from\nEUR,Euro,1999-01-01\n"

func uploadConfig(extra string) string {
	return providerConfig + fmt.Sprintf(`
resource "superset_database_upload" "test" {
  database_name = "Warehouse"
  table_name    = "regions"
  schema        = "public"
  content       = %q
%s}
`, uploadCSV, extra)
}

func TestAccDatabaseUploadResource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	f := &fakeUploads{}
	f.serve()
	uploads := func(want int) resource.TestCheckFunc {
		return func(*terraform.State) error {
			if got := f.count(); got != want {
				return fmt.Errorf("uploads: %d, want %d", got, want)
			}
			return nil
		}
	}

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: uploadConfig(`  column_dates    = ["valid_from"]
  null_values     = ["", "n/a"]
  header_row      = 0
  day_first       = false
  triggers        = { version = "1" }
  drop_on_destroy = true
`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_database_upload.test", "id", "3:public.regions"),
					resource.TestCheckResourceAttr("superset_database_upload.test", "database_id", "3"),
					resource.TestCheckResourceAttr("superset_database_upload.test", "type", "csv"),
					resource.TestCheckResourceAttr("superset_database_upload.test", "already_exists", "replace"),
					resource.TestCheckResourceAttr("superset_database_upload.test", "dataset_id", "77"),
					uploads(1),
					func(*terraform.State) error {
						fields, file, content := f.last()
						want := map[string]string{"table_name": "regions", "schema": "public", "type": "csv", "already_exists": "replace", "column_dates": "valid_from", "null_values": ",n/a", "header_row": "0", "day_first": "false"}
						for key, value := range want {
							if fields[key] != value {
								return fmt.Errorf("field %s: %q, want %q (fields %v)", key, fields[key], value, fields)
							}
						}
						if _, ok := fields["delimiter"]; ok {
							return fmt.Errorf("an unset option was sent: %v", fields)
						}
						if file != "content.csv" || content != uploadCSV {
							return fmt.Errorf("file %q with %q", file, content)
						}
						return nil
					},
				),
			},
			{
				// A new trigger uploads the file again, in place.
				Config: uploadConfig(`  column_dates    = ["valid_from"]
  null_values     = ["", "n/a"]
  header_row      = 0
  day_first       = false
  triggers        = { version = "2" }
  drop_on_destroy = true
`),
				ConfigPlanChecks: resource.ConfigPlanChecks{
					PreApply: []plancheck.PlanCheck{plancheck.ExpectResourceAction("superset_database_upload.test", plancheck.ResourceActionUpdate)},
				},
				Check: uploads(2),
			},
			{
				// The same configuration does not upload again.
				Config: uploadConfig(`  column_dates    = ["valid_from"]
  null_values     = ["", "n/a"]
  header_row      = 0
  day_first       = false
  triggers        = { version = "2" }
  drop_on_destroy = true
`),
				ConfigPlanChecks: resource.ConfigPlanChecks{
					PreApply: []plancheck.PlanCheck{plancheck.ExpectEmptyPlan()},
				},
				Check: uploads(2),
			},
			{
				// A table dropped outside Terraform removes the resource from the state.
				PreConfig: func() {
					f.mu.Lock()
					delete(f.tables, "regions")
					f.mu.Unlock()
				},
				RefreshState:       true,
				ExpectNonEmptyPlan: true,
			},
			{
				// Uploaded again, then destroyed with its table and dataset.
				Config: uploadConfig(`  triggers        = { version = "2" }
  drop_on_destroy = true
`),
				Check: uploads(3),
			},
		},
	})
	f.mu.Lock()
	defer f.mu.Unlock()
	if strings.Join(f.dropped, ";") != "DROP TABLE IF EXISTS public.regions" || len(f.deleted) != 1 {
		t.Fatalf("destroy ran %q and deleted datasets %v, want the table dropped and dataset 77 deleted", f.dropped, f.deleted)
	}
}

func TestAccDatabaseUploadResourceFile(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	f := &fakeUploads{}
	f.serve()
	dir := t.TempDir()
	file := filepath.Join(dir, "regions.csv")
	if err := os.WriteFile(file, []byte(uploadCSV), 0o600); err != nil {
		t.Fatal(err)
	}

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + fmt.Sprintf(`
resource "superset_database_upload" "test" {
  database_id    = 3
  table_name     = "regions"
  schema         = "public"
  file           = %q
  already_exists = "fail"
}
`, file),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_database_upload.test", "database_id", "3"),
					resource.TestCheckNoResourceAttr("superset_database_upload.test", "database_name"),
					func(*terraform.State) error {
						fields, name, content := f.last()
						if name != "regions.csv" || content != uploadCSV || fields["already_exists"] != "fail" {
							return fmt.Errorf("file %q with %q, fields %v", name, content, fields)
						}
						return nil
					},
				),
			},
			{
				// With already_exists = fail a second upload of an existing table fails.
				Config: providerConfig + fmt.Sprintf(`
resource "superset_database_upload" "test" {
  database_id    = 3
  table_name     = "regions"
  schema         = "public"
  file           = %q
  already_exists = "fail"
  triggers       = { version = "2" }
}
`, file),
				ExpectError: regexp.MustCompile(`Table already exists`),
			},
		},
	})
	// The table and the dataset stay without drop_on_destroy.
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.dropped) != 0 || len(f.deleted) != 0 {
		t.Fatalf("destroy ran %q and deleted %v, want nothing", f.dropped, f.deleted)
	}
}

func TestAccDatabaseUploadResourceErrors(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	f := &fakeUploads{}
	f.serve()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
resource "superset_database_upload" "test" {
  database_name = "Warehouse"
  table_name    = "regions"
  schema        = "secret"
  content       = "a,b\n1,2\n"
}
`,
				ExpectError: regexp.MustCompile(`not allowed for csv uploads`),
			},
			{
				Config: providerConfig + `
resource "superset_database_upload" "test" {
  database_name = "Warehouse"
  database_id   = 3
  table_name    = "regions"
  content       = "a,b\n1,2\n"
}
`,
				ExpectError: regexp.MustCompile(`Set exactly one of database_name and database_id`),
			},
			{
				Config: providerConfig + `
resource "superset_database_upload" "test" {
  database_name = "Warehouse"
  table_name    = "regions"
  content       = "a,b\n1,2\n"
  file          = "regions.csv"
}
`,
				ExpectError: regexp.MustCompile(`Set exactly one of file and content`),
			},
		},
	})
}

// On a version without the upload endpoint the apply explains what it needs.
func TestAccDatabaseUploadResourceOnLegacySuperset(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	serveLookups()
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/csrf_token/",
		httpmock.NewStringResponder(200, `{"result": "fake-csrf-token"}`))
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/database/3/upload/", httpmock.NewStringResponder(404, missingRoute))

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config:      uploadConfig(""),
				ExpectError: regexp.MustCompile(`(?s)needs Superset 4\.1`),
			},
		},
	})
}
