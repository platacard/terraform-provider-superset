package provider

import (
	"archive/zip"
	"bytes"
	"errors"
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
	"github.com/hashicorp/terraform-plugin-testing/terraform"
	"github.com/jarcoal/httpmock"
)

const (
	exportDashboardUUID = "11111111-1111-1111-1111-111111111111"
	exportChartUUID     = "22222222-2222-2222-2222-222222222222"
	exportDatasetUUID   = "33333333-3333-3333-3333-333333333333"
)

// exportFiles is an export of one dashboard with its chart, dataset and database.
var exportFiles = map[string]string{
	"metadata.yaml":                   "version: 1.0.0\ntype: Dashboard\ntimestamp: '2026-01-01T00:00:00+00:00'\n",
	"dashboards/signups.yaml":         "dashboard_title: Signups\nslug: signups\nuuid: " + exportDashboardUUID + "\nposition: {}\nversion: 1.0.0\n",
	"charts/daily_signups.yaml":       "slice_name: Daily signups\nviz_type: table\nuuid: " + exportChartUUID + "\ndataset_uuid: " + exportDatasetUUID + "\nversion: 1.0.0\n",
	"datasets/warehouse/signups.yaml": "table_name: signups\nschema: public\nuuid: " + exportDatasetUUID + "\ndatabase_uuid: 44444444-4444-4444-4444-444444444444\nversion: 1.0.0\n",
	"databases/warehouse.yaml":        "database_name: warehouse\nsqlalchemy_uri: postgresql://u:XXXXXXXXXX@h:5432/d\nuuid: 44444444-4444-4444-4444-444444444444\nversion: 1.0.0\n",
}

func writeExportDir(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "signups_export")
	for name, content := range exportFiles {
		file := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(file), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(file, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// fakeImport serves the import endpoints of the fake Superset host. Every import stores the
// uploaded bundle and its form fields; the imported dashboard gets id 5. With legacy set, the
// list endpoint rejects the uuid filter and the export endpoint tells candidates apart, as on
// Superset 3.1.
type fakeImport struct {
	mu       sync.Mutex
	legacy   bool
	imports  int
	fields   map[string]string
	entries  map[string]string
	deleted  bool
	exported int
}

func (f *fakeImport) serve() {
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/csrf_token/",
		httpmock.NewStringResponder(200, `{"result": "fake-csrf-token"}`))

	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/dashboard/import/",
		func(req *http.Request) (*http.Response, error) {
			reader, err := req.MultipartReader()
			if err != nil {
				return httpmock.NewStringResponse(400, `{"message": "not multipart"}`), nil //nolint:nilerr // the provider must see an HTTP error, not a transport error
			}
			fields, entries := map[string]string{}, map[string]string{}
			for {
				part, err := reader.NextPart()
				if err == io.EOF {
					break
				}
				if err != nil {
					return httpmock.NewStringResponse(400, `{"message": "bad multipart"}`), nil //nolint:nilerr // the provider must see an HTTP error, not a transport error
				}
				data, _ := io.ReadAll(part)
				if part.FormName() != "formData" {
					fields[part.FormName()] = string(data)
					continue
				}
				archive, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
				if err != nil {
					return httpmock.NewStringResponse(400, `{"message": "not a zip"}`), nil //nolint:nilerr // the provider must see an HTTP error, not a transport error
				}
				for _, entry := range archive.File {
					file, _ := entry.Open()
					content, _ := io.ReadAll(file)
					file.Close()
					entries[entry.Name] = string(content)
				}
			}
			if fields["overwrite"] != "true" && f.imports > 0 && !f.deleted {
				return httpmock.NewStringResponse(422, `{"errors": [{"message": "Dashboard already exists and overwrite=true was not passed"}]}`), nil
			}
			if f.imports == 0 && fields["passwords"] == "" {
				return httpmock.NewStringResponse(422, `{"errors": [{"message": "Must provide a password for the database"}]}`), nil
			}
			f.mu.Lock()
			f.imports++
			f.fields, f.entries, f.deleted = fields, entries, false
			f.mu.Unlock()
			return httpmock.NewStringResponse(200, `{"message": "OK"}`), nil
		})
	httpmock.RegisterResponder("GET", listEndpoint("/api/v1/dashboard/"),
		func(req *http.Request) (*http.Response, error) {
			query := req.URL.Query().Get("q")
			f.mu.Lock()
			defer f.mu.Unlock()
			if strings.Contains(query, "col:uuid") {
				if f.legacy {
					return httpmock.NewStringResponse(400, `{"message": "Filter column not allowed"}`), nil
				}
				if f.deleted || !strings.Contains(query, exportDashboardUUID) {
					return httpmock.NewJsonResponse(200, map[string]any{"count": 0, "result": []any{}})
				}
				return httpmock.NewJsonResponse(200, map[string]any{"count": 1, "result": []any{map[string]any{"id": 5}}})
			}
			// Two dashboards share the slug on the legacy host, so the uuid tells them apart.
			if f.deleted {
				return httpmock.NewJsonResponse(200, map[string]any{"count": 0, "result": []any{}})
			}
			return httpmock.NewJsonResponse(200, map[string]any{"count": 2, "result": []any{map[string]any{"id": 4}, map[string]any{"id": 5}}})
		})
	httpmock.RegisterResponder("GET", `=~^http://superset-host/api/v1/dashboard/export/\?q=!\((\d+)\)$`,
		func(req *http.Request) (*http.Response, error) {
			id := httpmock.MustGetSubmatchAsInt(req, 1)
			uuid := "99999999-9999-9999-9999-999999999999"
			if id == 5 {
				uuid = exportDashboardUUID
			}
			var buffer bytes.Buffer
			archive := zip.NewWriter(&buffer)
			entry, _ := archive.Create("dashboard_export/dashboards/x.yaml")
			_, _ = entry.Write([]byte("dashboard_title: Signups\nuuid: " + uuid + "\n"))
			_ = archive.Close()
			f.mu.Lock()
			f.exported++
			f.mu.Unlock()
			return httpmock.NewBytesResponse(200, buffer.Bytes()), nil
		})
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/dashboard/5",
		func(req *http.Request) (*http.Response, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.deleted {
				return httpmock.NewStringResponse(404, `{"message": "Not found"}`), nil
			}
			return httpmock.NewStringResponse(200, `{"id": 5, "result": {"dashboard_title": "Signups"}}`), nil
		})
	httpmock.RegisterResponder("DELETE", "http://superset-host/api/v1/dashboard/5",
		func(req *http.Request) (*http.Response, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.deleted = true
			return httpmock.NewStringResponse(200, `{"message": "OK"}`), nil
		})
}

func (f *fakeImport) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.imports
}

func (f *fakeImport) field(name string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fields[name]
}

func (f *fakeImport) entry(name string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.entries[name]
}

func importConfig(dir string) string {
	return providerConfig + fmt.Sprintf(`
resource "superset_dashboard_import" "test" {
  source = %q
  database_passwords = {
    "databases/warehouse.yaml" = "secret"
  }
}
`, dir)
}

func TestAccDashboardImportResource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeImport{}
	fake.serve()
	dir := writeExportDir(t)

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: importConfig(dir),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_dashboard_import.test", "overwrite", "true"),
					resource.TestCheckResourceAttr("superset_dashboard_import.test", "dashboard_ids.%", "1"),
					resource.TestCheckResourceAttr("superset_dashboard_import.test", "dashboard_ids."+exportDashboardUUID, "5"),
					resource.TestCheckResourceAttrSet("superset_dashboard_import.test", "content_hash"),
					func(*terraform.State) error {
						if fake.count() != 1 {
							return fmt.Errorf("expected one import, got %d", fake.count())
						}
						if fake.field("passwords") != `{"databases/warehouse.yaml":"secret"}` {
							return errors.New("the database passwords must be sent as a JSON map keyed by file path")
						}
						if fake.entry("signups_export/metadata.yaml") != "version: 1.0.0\ntype: Dashboard\n" {
							return fmt.Errorf("unexpected metadata: %q", fake.entry("signups_export/metadata.yaml"))
						}
						if !strings.Contains(fake.entry("signups_export/dashboards/signups.yaml"), "slug: signups") {
							return errors.New("the dashboard file must be in the bundle under the export directory")
						}
						return nil
					},
				),
			},
			{
				// Nothing changed: no import.
				Config:   importConfig(dir),
				PlanOnly: true,
			},
			{
				// A changed file plans and performs an import.
				PreConfig: func() {
					file := filepath.Join(dir, "dashboards", "signups.yaml")
					content, _ := os.ReadFile(file)
					_ = os.WriteFile(file, bytes.Replace(content, []byte("Signups"), []byte("Signups v2"), 1), 0o644)
				},
				Config: importConfig(dir),
				Check: func(*terraform.State) error {
					if fake.count() != 2 {
						return fmt.Errorf("expected a second import, got %d", fake.count())
					}
					if !strings.Contains(fake.entry("signups_export/dashboards/signups.yaml"), "Signups v2") {
						return errors.New("the changed file must be imported")
					}
					return nil
				},
			},
			{
				// A dashboard deleted outside Terraform is imported again.
				PreConfig: func() {
					fake.mu.Lock()
					fake.deleted = true
					fake.mu.Unlock()
				},
				Config: importConfig(dir),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_dashboard_import.test", "dashboard_ids."+exportDashboardUUID, "5"),
					func(*terraform.State) error {
						if fake.count() != 3 {
							return fmt.Errorf("expected a third import, got %d", fake.count())
						}
						return nil
					},
				),
			},
		},
	})
}

func TestAccDashboardImportResourceLegacyLookup(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeImport{legacy: true}
	fake.serve()
	dir := writeExportDir(t)

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: importConfig(dir),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_dashboard_import.test", "dashboard_ids."+exportDashboardUUID, "5"),
					func(*terraform.State) error {
						if fake.exported == 0 {
							return errors.New("the candidates must be told apart by their exported uuid")
						}
						return nil
					},
				),
			},
		},
	})
}

func TestAccDashboardImportResourceErrors(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeImport{}
	fake.serve()
	dir := writeExportDir(t)

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + fmt.Sprintf(`
resource "superset_dashboard_import" "test" {
  source = %q
}
`, filepath.Join(dir, "charts")),
				ExpectError: regexp.MustCompile("no export layout"),
			},
			{
				Config: providerConfig + fmt.Sprintf(`
resource "superset_dashboard_import" "test" {
  source = %q
}
`, dir),
				ExpectError: regexp.MustCompile("Must provide a password for the database"),
			},
		},
	})
}
