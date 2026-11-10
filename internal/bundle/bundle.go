// Package bundle builds and inspects the export bundles that Superset imports: a zip file with a
// single top-level directory holding metadata.yaml and one YAML file per exported object under
// dashboards/, charts/, datasets/ and databases/.
package bundle

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// Kind is a kind of top-level object of a bundle: the type Superset expects in metadata.yaml and
// the directory that holds the objects.
type Kind struct {
	Type string
	Dir  string
}

var (
	Dashboards = Kind{Type: "Dashboard", Dir: "dashboards"}
	Charts     = Kind{Type: "Slice", Dir: "charts"}
	Datasets   = Kind{Type: "SqlaTable", Dir: "datasets"}
	// Assets is the kind of a bundle that the assets endpoint imports as a whole; its objects
	// are listed per kind with Objects, since no directory is its own.
	Assets = Kind{Type: "assets", Dir: "assets"}
)

// objectDirs are the directories of an export layout.
var objectDirs = []string{"dashboards", "charts", "datasets", "databases"}

const metadataFile = "metadata.yaml"

// Object is one top-level object of a bundle, as far as its YAML identifies it.
type Object struct {
	Path   string
	UUID   string
	Name   string
	Slug   string
	Schema string
}

// Bundle is a bundle ready for import.
type Bundle struct {
	Content []byte
	Hash    string
	Objects []Object
}

// Load builds a bundle of the kind from a zip file or from a directory in the export layout. A
// directory is zipped under a single top-level directory with a metadata.yaml of the kind, so
// the same export directory can feed the import of dashboards, charts or datasets. A zip file is
// used as it is. The hash covers the paths and contents of the files, except metadata.yaml.
func Load(source string, kind Kind) (*Bundle, error) {
	info, err := os.Stat(source)
	if err != nil {
		return nil, err
	}

	var content []byte
	if info.IsDir() {
		content, err = zipDirectory(source, kind)
	} else {
		content, err = os.ReadFile(source)
	}
	if err != nil {
		return nil, err
	}

	objects, hash, err := inspect(content, kind)
	if err != nil {
		return nil, err
	}
	return &Bundle{Content: content, Hash: hash, Objects: objects}, nil
}

// filesRoot is the top-level directory of a bundle built from files.
const filesRoot = "bundle"

// FromFiles builds a bundle of the kind from file contents keyed by their path in the export
// layout, such as dashboards/sales.yaml or databases/warehouse.yaml. It is the same bundle that
// Load builds from a directory holding these files: the files are zipped under one top-level
// directory with a metadata.yaml of the kind, and a metadata.yaml among the files is ignored.
func FromFiles(files map[string]string, kind Kind) (*Bundle, error) {
	names := make([]string, 0, len(files))
	layout := false
	for name := range files {
		if err := checkFilePath(name); err != nil {
			return nil, err
		}
		if name == metadataFile {
			continue
		}
		if contains(objectDirs, strings.SplitN(name, "/", 2)[0]) {
			layout = true
		}
		names = append(names, name)
	}
	if !layout {
		return nil, fmt.Errorf("the files hold no export layout: expected paths in a directory such as %s", strings.Join(objectDirs, ", "))
	}
	sort.Strings(names)

	var buffer bytes.Buffer
	archive := zip.NewWriter(&buffer)
	for _, name := range names {
		if err := addFile(archive, path.Join(filesRoot, name), []byte(files[name])); err != nil {
			return nil, err
		}
	}
	metadata := fmt.Sprintf("version: 1.0.0\ntype: %s\n", kind.Type)
	if err := addFile(archive, path.Join(filesRoot, metadataFile), []byte(metadata)); err != nil {
		return nil, err
	}
	if err := archive.Close(); err != nil {
		return nil, err
	}

	content := buffer.Bytes()
	objects, hash, err := inspect(content, kind)
	if err != nil {
		return nil, err
	}
	return &Bundle{Content: content, Hash: hash, Objects: objects}, nil
}

// checkFilePath accepts a relative path with forward slashes that stays inside the bundle.
func checkFilePath(name string) error {
	switch {
	case name == "":
		return fmt.Errorf("a file of the bundle has an empty path")
	case strings.Contains(name, "\\"):
		return fmt.Errorf("the path %q of a bundle file must use forward slashes", name)
	case strings.HasPrefix(name, "/") || path.Clean(name) != name || name == ".." || strings.HasPrefix(name, "../"):
		return fmt.Errorf("the path %q of a bundle file must be relative and clean, such as dashboards/sales.yaml", name)
	}
	return nil
}

// Objects returns the top-level objects of the kind found in bundle content.
func Objects(content []byte, kind Kind) ([]Object, error) {
	objects, _, err := inspect(content, kind)
	return objects, err
}

// zipDirectory zips the files of the directory under one top-level directory. When the directory
// holds nothing but one directory with the export layout, that one is used.
func zipDirectory(dir string, kind Kind) ([]byte, error) {
	root, err := exportRoot(dir)
	if err != nil {
		return nil, err
	}

	var files []string
	err = filepath.WalkDir(root, func(file string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type().IsRegular() {
			files = append(files, file)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(files)

	var buffer bytes.Buffer
	archive := zip.NewWriter(&buffer)
	top := filepath.Base(root)
	for _, file := range files {
		relative, err := filepath.Rel(root, file)
		if err != nil {
			return nil, err
		}
		if relative == metadataFile {
			continue
		}
		data, err := os.ReadFile(file)
		if err != nil {
			return nil, err
		}
		if err := addFile(archive, path.Join(top, filepath.ToSlash(relative)), data); err != nil {
			return nil, err
		}
	}
	metadata := fmt.Sprintf("version: 1.0.0\ntype: %s\n", kind.Type)
	if err := addFile(archive, path.Join(top, metadataFile), []byte(metadata)); err != nil {
		return nil, err
	}
	if err := archive.Close(); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func addFile(archive *zip.Writer, name string, data []byte) error {
	entry, err := archive.Create(name)
	if err != nil {
		return err
	}
	_, err = entry.Write(data)
	return err
}

// exportRoot returns the directory that holds the export layout: dir itself, or its only
// subdirectory when dir holds nothing else, as an unpacked export does.
func exportRoot(dir string) (string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}
	for _, entry := range entries {
		if entry.IsDir() && contains(objectDirs, entry.Name()) {
			return dir, nil
		}
	}
	if len(entries) == 1 && entries[0].IsDir() {
		return exportRoot(filepath.Join(dir, entries[0].Name()))
	}
	return "", fmt.Errorf("%s holds no export layout: expected a directory such as %s", dir, strings.Join(objectDirs, ", "))
}

func contains(values []string, value string) bool {
	for _, v := range values {
		if v == value {
			return true
		}
	}
	return false
}

// inspect lists the objects of the kind in the bundle and computes its hash.
func inspect(content []byte, kind Kind) ([]Object, string, error) {
	archive, err := zip.NewReader(bytes.NewReader(content), int64(len(content)))
	if err != nil {
		return nil, "", fmt.Errorf("the bundle is not a zip file: %w", err)
	}

	entries := make([]*zip.File, 0, len(archive.File))
	for _, entry := range archive.File {
		if !entry.FileInfo().IsDir() {
			entries = append(entries, entry)
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })

	digest := sha256.New()
	var objects []Object
	for _, entry := range entries {
		data, err := readEntry(entry)
		if err != nil {
			return nil, "", err
		}
		inner := strings.SplitN(entry.Name, "/", 2)
		if len(inner) < 2 {
			return nil, "", fmt.Errorf("the bundle must hold a single top-level directory, found the file %s at its root", entry.Name)
		}
		if inner[1] == metadataFile {
			continue
		}
		fmt.Fprintf(digest, "%s\n%d\n", entry.Name, len(data))
		digest.Write(data)

		if path.Dir(inner[1]) == kind.Dir || strings.HasPrefix(inner[1], kind.Dir+"/") {
			object, err := parseObject(entry.Name, data)
			if err != nil {
				return nil, "", err
			}
			objects = append(objects, object)
		}
	}
	return objects, hex.EncodeToString(digest.Sum(nil)), nil
}

func readEntry(entry *zip.File) ([]byte, error) {
	reader, err := entry.Open()
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

// parseObject reads the identifying fields of an exported object.
func parseObject(name string, data []byte) (Object, error) {
	var fields struct {
		UUID      string `yaml:"uuid"`
		Title     string `yaml:"dashboard_title"`
		Slug      string `yaml:"slug"`
		SliceName string `yaml:"slice_name"`
		TableName string `yaml:"table_name"`
		Schema    string `yaml:"schema"`
	}
	if err := yaml.Unmarshal(data, &fields); err != nil {
		return Object{}, fmt.Errorf("%s: %w", name, err)
	}
	if fields.UUID == "" {
		return Object{}, fmt.Errorf("%s: the object has no uuid", name)
	}
	object := Object{Path: name, UUID: fields.UUID, Slug: fields.Slug, Schema: fields.Schema}
	for _, candidate := range []string{fields.Title, fields.SliceName, fields.TableName} {
		if candidate != "" {
			object.Name = candidate
			break
		}
	}
	return object, nil
}
