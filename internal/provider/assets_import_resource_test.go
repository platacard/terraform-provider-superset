package provider

import (
	"archive/zip"
	"bytes"
	"fmt"
	"github.com/hashicorp/terraform-plugin-testing/knownvalue"
	"github.com/hashicorp/terraform-plugin-testing/plancheck"
	"github.com/hashicorp/terraform-plugin-testing/tfjsonpath"
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

// fakeAssetsImport serves the assets import endpoint of the fake Superset host and the
// endpoints that locate, check and delete the imported dashboard (id 5), chart (id 6) and
// dataset (id 7).
type fakeAssetsImport struct {
	mu      sync.Mutex
	imports int
	fields  map[string]string
	entries map[string]string
	deleted map[string]bool
	// deletions lists the deleted objects in order, as "<kind> <id>".
	deletions []string
}

var fakeAssetIDs = map[string]struct {
	path string
	uuid string
	id   int
}{
	"dashboard": {path: "dashboard", uuid: exportDashboardUUID, id: 5},
	"chart":     {path: "chart", uuid: exportChartUUID, id: 6},
	"dataset":   {path: "dataset", uuid: exportDatasetUUID, id: 7},
}

func (f *fakeAssetsImport) serve() {
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/csrf_token/",
		httpmock.NewStringResponder(200, `{"result": "fake-csrf-token"}`))
	f.deleted = map[string]bool{}

	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/assets/import/",
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
				if part.FormName() != "bundle" {
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
			if f.imports == 0 && fields["passwords"] == "" {
				return httpmock.NewStringResponse(422, `{"errors": [{"message": "Must provide a password for the database"}]}`), nil
			}
			f.mu.Lock()
			f.imports++
			f.fields, f.entries, f.deleted = fields, entries, map[string]bool{}
			f.mu.Unlock()
			return httpmock.NewStringResponse(200, `{"message": "OK"}`), nil
		})
	for kind, asset := range fakeAssetIDs {
		httpmock.RegisterResponder("GET", listEndpoint("/api/v1/"+asset.path+"/"),
			func(req *http.Request) (*http.Response, error) {
				f.mu.Lock()
				defer f.mu.Unlock()
				if f.deleted[kind] || f.imports == 0 || !strings.Contains(req.URL.Query().Get("q"), asset.uuid) {
					return httpmock.NewJsonResponse(200, map[string]any{"count": 0, "result": []any{}})
				}
				return httpmock.NewJsonResponse(200, map[string]any{"count": 1, "result": []any{map[string]any{"id": asset.id}}})
			})
		httpmock.RegisterResponder("GET", fmt.Sprintf("http://superset-host/api/v1/%s/%d", asset.path, asset.id),
			func(req *http.Request) (*http.Response, error) {
				f.mu.Lock()
				defer f.mu.Unlock()
				if f.deleted[kind] {
					return httpmock.NewStringResponse(404, `{"message": "Not found"}`), nil
				}
				return httpmock.NewStringResponse(200, fmt.Sprintf(`{"id": %d, "result": {}}`, asset.id)), nil
			})
		httpmock.RegisterResponder("DELETE", fmt.Sprintf("http://superset-host/api/v1/%s/%d", asset.path, asset.id),
			func(req *http.Request) (*http.Response, error) {
				f.mu.Lock()
				defer f.mu.Unlock()
				f.deleted[kind] = true
				f.deletions = append(f.deletions, fmt.Sprintf("%s %d", kind, asset.id))
				return httpmock.NewStringResponse(200, `{"message": "OK"}`), nil
			})
	}
}

func (f *fakeAssetsImport) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.imports
}

func (f *fakeAssetsImport) field(name string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fields[name]
}

func assetsImportConfig(dir, extra string) string {
	return providerConfig + fmt.Sprintf(`
resource "superset_assets_import" "test" {
  source = %q
  database_passwords = {
    "databases/warehouse.yaml" = "secret"
  }
%s
}
`, dir, extra)
}

func TestAccAssetsImportResource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeAssetsImport{}
	fake.serve()
	dir := writeExportDir(t)

	imports := func(want int) resource.TestCheckFunc {
		return func(*terraform.State) error {
			if got := fake.count(); got != want {
				return fmt.Errorf("the bundle was imported %d times, want %d", got, want)
			}
			return nil
		}
	}

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: assetsImportConfig(dir, ""),
				Check: resource.ComposeAggregateTestCheckFunc(
					imports(1),
					resource.TestCheckResourceAttr("superset_assets_import.test", "dashboard_ids."+exportDashboardUUID, "5"),
					resource.TestCheckResourceAttr("superset_assets_import.test", "chart_ids."+exportChartUUID, "6"),
					resource.TestCheckResourceAttr("superset_assets_import.test", "dataset_ids."+exportDatasetUUID, "7"),
					resource.TestCheckResourceAttrSet("superset_assets_import.test", "content_hash"),
					resource.TestCheckResourceAttr("superset_assets_import.test", "sparse", "false"),
					func(*terraform.State) error {
						fake.mu.Lock()
						defer fake.mu.Unlock()
						if fake.fields["passwords"] != `{"databases/warehouse.yaml":"secret"}` {
							return fmt.Errorf("unexpected passwords sent: %q", fake.fields["passwords"])
						}
						if _, present := fake.fields["sparse"]; present {
							return fmt.Errorf("sparse must not be sent when unset")
						}
						if _, present := fake.fields["overwrite"]; present {
							return fmt.Errorf("the assets endpoint takes no overwrite field")
						}
						if !strings.Contains(fake.entries["signups_export/metadata.yaml"], "type: assets") {
							return fmt.Errorf("the bundle must carry an assets metadata, got %q", fake.entries["signups_export/metadata.yaml"])
						}
						return nil
					},
				),
			},
			{
				// Nothing changed: no import.
				Config:   assetsImportConfig(dir, ""),
				PlanOnly: true,
			},
			{
				// A changed file imports the bundle again.
				PreConfig: func() {
					file := filepath.Join(dir, "dashboards", "signups.yaml")
					content, _ := os.ReadFile(file)
					_ = os.WriteFile(file, append(content, []byte("description: changed\n")...), 0o644)
				},
				Config: assetsImportConfig(dir, ""),
				Check:  imports(2),
			},
			{
				// A chart deleted outside Terraform marks the content hash as outdated. The state
				// keeps the identifiers, so that a destroy would still delete the other objects.
				PreConfig: func() {
					fake.mu.Lock()
					fake.deleted["chart"] = true
					fake.mu.Unlock()
				},
				RefreshState:       true,
				ExpectNonEmptyPlan: true,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_assets_import.test", "content_hash", outdatedContentHash),
					resource.TestCheckResourceAttr("superset_assets_import.test", "dashboard_ids.%", "1"),
					resource.TestCheckResourceAttr("superset_assets_import.test", "dataset_ids.%", "1"),
				),
			},
			{
				// The plan imports the bundle again, and plans the real hash as a known value, so
				// that tools which ignore unknown planned values see the change too.
				Config: assetsImportConfig(dir, ""),
				ConfigPlanChecks: resource.ConfigPlanChecks{
					PreApply: []plancheck.PlanCheck{
						plancheck.ExpectResourceAction("superset_assets_import.test", plancheck.ResourceActionUpdate),
						plancheck.ExpectKnownValue("superset_assets_import.test", tfjsonpath.New("content_hash"), knownvalue.StringRegexp(regexp.MustCompile(`^[0-9a-f]{64}$`))),
					},
				},
				Check: resource.ComposeAggregateTestCheckFunc(
					imports(3),
					resource.TestCheckResourceAttr("superset_assets_import.test", "chart_ids.%", "1"),
				),
			},
			{
				Config: assetsImportConfig(dir, `  sparse = true`),
				Check: resource.ComposeAggregateTestCheckFunc(
					imports(4),
					resource.TestCheckResourceAttr("superset_assets_import.test", "sparse", "true"),
					func(*terraform.State) error {
						if fake.field("sparse") != "true" {
							return fmt.Errorf("sparse must be sent as true, got %q", fake.field("sparse"))
						}
						return nil
					},
				),
			},
			{
				// The destroy deletes the dashboard, then the chart, then the dataset.
				Config:  providerConfig,
				Destroy: true,
				Check: func(*terraform.State) error {
					fake.mu.Lock()
					defer fake.mu.Unlock()
					want := []string{"dashboard 5", "chart 6", "dataset 7"}
					if strings.Join(fake.deletions, ", ") != strings.Join(want, ", ") {
						return fmt.Errorf("unexpected deletions: %v, want %v", fake.deletions, want)
					}
					return nil
				},
			},
		},
	})
}
