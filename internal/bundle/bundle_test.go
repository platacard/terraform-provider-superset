package bundle

import (
	"archive/zip"
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	dashboardYAML = "dashboard_title: Signups\nslug: signups\nuuid: 11111111-1111-1111-1111-111111111111\nposition: {}\nversion: 1.0.0\n"
	chartYAML     = "slice_name: Daily signups\nviz_type: table\nuuid: 22222222-2222-2222-2222-222222222222\ndataset_uuid: 33333333-3333-3333-3333-333333333333\nversion: 1.0.0\n"
	datasetYAML   = "table_name: signups\nschema: public\nuuid: 33333333-3333-3333-3333-333333333333\ndatabase_uuid: 44444444-4444-4444-4444-444444444444\nversion: 1.0.0\n"
	databaseYAML  = "database_name: warehouse\nsqlalchemy_uri: postgresql://u:XXXXXXXXXX@h:5432/d\nuuid: 44444444-4444-4444-4444-444444444444\nversion: 1.0.0\n"
)

func writeExport(t *testing.T, dir string) {
	t.Helper()
	files := map[string]string{
		"metadata.yaml":                   "version: 1.0.0\ntype: Dashboard\ntimestamp: '2026-01-01T00:00:00+00:00'\n",
		"dashboards/signups.yaml":         dashboardYAML,
		"charts/daily_signups.yaml":       chartYAML,
		"datasets/warehouse/signups.yaml": datasetYAML,
		"databases/warehouse.yaml":        databaseYAML,
	}
	for name, content := range files {
		file := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(file), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(file, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func entries(t *testing.T, content []byte) map[string]string {
	t.Helper()
	archive, err := zip.NewReader(bytes.NewReader(content), int64(len(content)))
	if err != nil {
		t.Fatal(err)
	}
	result := map[string]string{}
	for _, entry := range archive.File {
		data, err := readEntry(entry)
		if err != nil {
			t.Fatal(err)
		}
		result[entry.Name] = string(data)
	}
	return result
}

func TestLoadDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "signups_export")
	writeExport(t, dir)

	for _, kind := range []Kind{Dashboards, Charts, Datasets} {
		b, err := Load(dir, kind)
		if err != nil {
			t.Fatalf("%s: %v", kind.Type, err)
		}
		files := entries(t, b.Content)
		if got := files["signups_export/metadata.yaml"]; got != "version: 1.0.0\ntype: "+kind.Type+"\n" {
			t.Fatalf("%s: unexpected metadata %q", kind.Type, got)
		}
		if _, ok := files["signups_export/databases/warehouse.yaml"]; !ok {
			t.Fatalf("%s: the database file is missing from the bundle", kind.Type)
		}
		if len(b.Objects) != 1 {
			t.Fatalf("%s: expected one object, got %+v", kind.Type, b.Objects)
		}
	}

	b, _ := Load(dir, Dashboards)
	if o := b.Objects[0]; o.UUID != "11111111-1111-1111-1111-111111111111" || o.Name != "Signups" || o.Slug != "signups" {
		t.Fatalf("unexpected dashboard object %+v", o)
	}
	b, _ = Load(dir, Datasets)
	if o := b.Objects[0]; o.Name != "signups" || o.Schema != "public" {
		t.Fatalf("unexpected dataset object %+v", o)
	}
}

func TestHashIgnoresMetadataAndFollowsContent(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "export")
	writeExport(t, dir)
	first, _ := Load(dir, Dashboards)

	if err := os.WriteFile(filepath.Join(dir, "metadata.yaml"), []byte("version: 1.0.0\ntype: Dashboard\ntimestamp: '2030-01-01T00:00:00+00:00'\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	second, _ := Load(dir, Dashboards)
	if first.Hash != second.Hash {
		t.Fatal("the metadata file must not change the hash")
	}

	if err := os.WriteFile(filepath.Join(dir, "dashboards", "signups.yaml"), []byte(strings.Replace(dashboardYAML, "Signups", "Signups v2", 1)), 0o644); err != nil {
		t.Fatal(err)
	}
	third, _ := Load(dir, Dashboards)
	if first.Hash == third.Hash {
		t.Fatal("a changed object must change the hash")
	}
	if third.Objects[0].Name != "Signups v2" {
		t.Fatalf("unexpected name %q", third.Objects[0].Name)
	}
}

func TestLoadUnpackedExport(t *testing.T) {
	dir := t.TempDir()
	writeExport(t, filepath.Join(dir, "dashboard_export_20260101"))

	b, err := Load(dir, Dashboards)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := entries(t, b.Content)["dashboard_export_20260101/dashboards/signups.yaml"]; !ok {
		t.Fatal("the unpacked export directory must become the top-level directory")
	}
}

func TestLoadZipFile(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "export")
	writeExport(t, dir)
	fromDir, _ := Load(dir, Dashboards)

	file := filepath.Join(t.TempDir(), "bundle.zip")
	if err := os.WriteFile(file, fromDir.Content, 0o644); err != nil {
		t.Fatal(err)
	}
	fromZip, err := Load(file, Dashboards)
	if err != nil {
		t.Fatal(err)
	}
	if fromZip.Hash != fromDir.Hash || len(fromZip.Objects) != 1 {
		t.Fatal("a zip file must load like the directory it was built from")
	}
}

func TestLoadErrors(t *testing.T) {
	if _, err := Load(t.TempDir(), Dashboards); err == nil || !strings.Contains(err.Error(), "no export layout") {
		t.Fatalf("an empty directory must be refused, got %v", err)
	}

	var flat bytes.Buffer
	archive := zip.NewWriter(&flat)
	_ = addFile(archive, "metadata.yaml", []byte("version: 1.0.0\n"))
	_ = addFile(archive, "dashboards/x.yaml", []byte(dashboardYAML))
	_ = archive.Close()
	if _, _, err := inspect(flat.Bytes(), Dashboards); err == nil || !strings.Contains(err.Error(), "single top-level directory") {
		t.Fatalf("a flat bundle must be refused, got %v", err)
	}

	var noUUID bytes.Buffer
	archive = zip.NewWriter(&noUUID)
	_ = addFile(archive, "export/dashboards/x.yaml", []byte("dashboard_title: X\n"))
	_ = archive.Close()
	if _, _, err := inspect(noUUID.Bytes(), Dashboards); err == nil || !strings.Contains(err.Error(), "no uuid") {
		t.Fatalf("an object without uuid must be refused, got %v", err)
	}
}

func TestFromFilesMatchesADirectory(t *testing.T) {
	files := map[string]string{
		"metadata.yaml":                   "version: 1.0.0\ntype: Dashboard\n",
		"dashboards/signups.yaml":         dashboardYAML,
		"charts/daily_signups.yaml":       chartYAML,
		"datasets/warehouse/signups.yaml": datasetYAML,
		"databases/warehouse.yaml":        databaseYAML,
	}
	fromFiles, err := FromFiles(files, Dashboards)
	if err != nil {
		t.Fatal(err)
	}

	// A directory named like the top-level directory of FromFiles gives the same bundle.
	dir := filepath.Join(t.TempDir(), filesRoot)
	writeExport(t, dir)
	fromDir, err := Load(dir, Dashboards)
	if err != nil {
		t.Fatal(err)
	}
	if fromFiles.Hash != fromDir.Hash {
		t.Errorf("hash %s differs from the hash %s of the same files in a directory", fromFiles.Hash, fromDir.Hash)
	}
	if !bytes.Equal(fromFiles.Content, fromDir.Content) {
		t.Error("the zip differs from the zip of the same files in a directory")
	}
	if len(fromFiles.Objects) != 1 || fromFiles.Objects[0].UUID != "11111111-1111-1111-1111-111111111111" {
		t.Errorf("unexpected objects %+v", fromFiles.Objects)
	}
	got := entries(t, fromFiles.Content)
	if got["bundle/metadata.yaml"] != "version: 1.0.0\ntype: Dashboard\n" {
		t.Errorf("unexpected metadata %q", got["bundle/metadata.yaml"])
	}

	// The same files give the same bundle every time.
	again, err := FromFiles(files, Dashboards)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(again.Content, fromFiles.Content) {
		t.Error("the zip is not deterministic")
	}
}

func TestFromFilesErrors(t *testing.T) {
	for name, files := range map[string]map[string]string{
		"no layout":      {"notes/readme.yaml": "uuid: x\n"},
		"empty":          {},
		"absolute path":  {"/dashboards/a.yaml": dashboardYAML},
		"parent dir":     {"../dashboards/a.yaml": dashboardYAML},
		"unclean path":   {"dashboards/../charts/a.yaml": chartYAML},
		"backslash path": {`dashboards\a.yaml`: dashboardYAML},
		"empty path":     {"": dashboardYAML, "charts/a.yaml": chartYAML},
		"no uuid":        {"dashboards/a.yaml": "dashboard_title: A\n"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := FromFiles(files, Dashboards); err == nil {
				t.Error("expected an error")
			}
		})
	}
}
