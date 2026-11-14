package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strconv"

	"github.com/platacard/terraform-provider-superset/internal/bundle"
)

// ImportKind names the import endpoint of a kind of object and how its objects are looked up.
type ImportKind struct {
	// Path is the API path of the kind, such as "dashboard".
	Path string
	// Bundle is the kind of the bundle objects.
	Bundle bundle.Kind
	// NameColumn is the column of the list endpoint that holds the name of an object.
	NameColumn string
}

var (
	DashboardImport = ImportKind{Path: "dashboard", Bundle: bundle.Dashboards, NameColumn: "dashboard_title"}
	ChartImport     = ImportKind{Path: "chart", Bundle: bundle.Charts, NameColumn: "slice_name"}
	DatasetImport   = ImportKind{Path: "dataset", Bundle: bundle.Datasets, NameColumn: "table_name"}
)

// ImportOptions are the form fields of an import request. The maps are keyed by the path of the
// database file inside the bundle, such as "databases/warehouse.yaml".
type ImportOptions struct {
	Overwrite                    bool
	DatabasePasswords            map[string]string
	SSHTunnelPasswords           map[string]string
	SSHTunnelPrivateKeys         map[string]string
	SSHTunnelPrivateKeyPasswords map[string]string
	SyncColumns                  bool
	SyncMetrics                  bool
}

// ImportBundle uploads a bundle to the import endpoint of the kind.
func (c *Client) ImportBundle(kind ImportKind, content []byte, options ImportOptions) error {
	fields := map[string]string{"overwrite": strconv.FormatBool(options.Overwrite)}
	if kind.Bundle == bundle.Datasets {
		fields["sync_columns"] = strconv.FormatBool(options.SyncColumns)
		fields["sync_metrics"] = strconv.FormatBool(options.SyncMetrics)
	}
	if err := addSecretFields(fields, options); err != nil {
		return err
	}
	return c.uploadBundle(fmt.Sprintf("/api/v1/%s/import/", kind.Path), "formData", fields, content, "import the "+kind.Path+" bundle")
}

// ImportAssets uploads a bundle of any assets to the assets import endpoint, which overwrites
// the objects it holds: databases, datasets, charts, dashboards and saved queries. A sparse
// import lets the dashboards of the bundle reference charts that are not in it, on recent
// versions of Superset; the field is only sent when set, as older versions do not know it.
func (c *Client) ImportAssets(content []byte, options ImportOptions, sparse bool) error {
	fields := map[string]string{}
	if sparse {
		fields["sparse"] = "true"
	}
	if err := addSecretFields(fields, options); err != nil {
		return err
	}
	return c.uploadBundle("/api/v1/assets/import/", "bundle", fields, content, "import the assets bundle")
}

// addSecretFields adds the passwords and SSH tunnel secrets of the options as JSON form fields.
func addSecretFields(fields map[string]string, options ImportOptions) error {
	secrets := map[string]map[string]string{
		"passwords":                        options.DatabasePasswords,
		"ssh_tunnel_passwords":             options.SSHTunnelPasswords,
		"ssh_tunnel_private_keys":          options.SSHTunnelPrivateKeys,
		"ssh_tunnel_private_key_passwords": options.SSHTunnelPrivateKeyPasswords,
	}
	for field, values := range secrets {
		if len(values) == 0 {
			continue
		}
		encoded, err := json.Marshal(values)
		if err != nil {
			return err
		}
		fields[field] = string(encoded)
	}
	return nil
}

// uploadBundle posts a bundle as a multipart form with the given fields.
func (c *Client) uploadBundle(endpoint, fileField string, fields map[string]string, content []byte, action string) error {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	for field, value := range fields {
		if err := form.WriteField(field, value); err != nil {
			return err
		}
	}
	file, err := form.CreateFormFile(fileField, "bundle.zip")
	if err != nil {
		return err
	}
	if _, err := file.Write(content); err != nil {
		return err
	}
	if err := form.Close(); err != nil {
		return err
	}

	if headers == nil {
		headers = map[string]string{}
	}
	headers["Content-Type"] = form.FormDataContentType()
	resp, err := c.send(http.MethodPost, endpoint, body.Bytes(), headers, cookies)
	if err != nil {
		return err
	}
	return decodeResponse(resp, nil, action)
}

// errUUIDFilterUnsupported means that the list endpoint cannot filter by uuid, as on Superset 3.1.
var errUUIDFilterUnsupported = errors.New("the list endpoint does not filter by uuid")

// LocateImported returns the id of the object of the kind that an imported object became.
// Recent versions of Superset find it by uuid. Superset 3.1 does not expose uuids through its
// list endpoints, so there the candidates are found by slug or name and told apart by the uuid
// in their export.
func (c *Client) LocateImported(kind ImportKind, object bundle.Object) (int64, error) {
	ids, err := c.idsByUUID(kind, object.UUID)
	if err == nil {
		if len(ids) == 0 {
			return 0, fmt.Errorf("%s %s (%s): %w", kind.Path, object.Name, object.UUID, ErrNotFound)
		}
		return ids[0], nil
	}
	if !errors.Is(err, errUUIDFilterUnsupported) {
		return 0, err
	}

	candidates, err := c.idsByNaturalKey(kind, object)
	if err != nil {
		return 0, err
	}
	if len(candidates) == 1 {
		return candidates[0], nil
	}
	for _, id := range candidates {
		uuid, err := c.exportedUUID(kind, id)
		if err != nil {
			return 0, err
		}
		if uuid == object.UUID {
			return id, nil
		}
	}
	return 0, fmt.Errorf("%s %s (%s): %w", kind.Path, object.Name, object.UUID, ErrNotFound)
}

type idRow struct {
	ID int64 `json:"id"`
}

func (c *Client) idsByUUID(kind ImportKind, uuid string) ([]int64, error) {
	resp, err := c.DoRequest(http.MethodGet, fmt.Sprintf("/api/v1/%s/?q=%s", kind.Path, listQuery([]listFilter{equals("uuid", uuid)}, 0)), nil)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusBadRequest {
		resp.Body.Close()
		return nil, errUUIDFilterUnsupported
	}
	page, err := decodeListPage(resp, kind.Path)
	if err != nil {
		return nil, err
	}
	return rowIDs(page.Result)
}

func (c *Client) idsByNaturalKey(kind ImportKind, object bundle.Object) ([]int64, error) {
	var filters []listFilter
	switch {
	case kind.Bundle == bundle.Dashboards && object.Slug != "":
		filters = []listFilter{equals("slug", object.Slug)}
	case kind.Bundle == bundle.Datasets && object.Schema != "":
		filters = []listFilter{equals(kind.NameColumn, object.Name), equals("schema", object.Schema)}
	default:
		filters = []listFilter{equals(kind.NameColumn, object.Name)}
	}
	rows, err := c.listAll(fmt.Sprintf("/api/v1/%s/", kind.Path), filters...)
	if err != nil {
		return nil, err
	}
	return rowIDs(rows)
}

func rowIDs(rows []json.RawMessage) ([]int64, error) {
	ids := make([]int64, 0, len(rows))
	for _, raw := range rows {
		var row idRow
		if err := json.Unmarshal(raw, &row); err != nil {
			return nil, err
		}
		ids = append(ids, row.ID)
	}
	return ids, nil
}

// exportedUUID exports one object and returns the uuid of its YAML.
func (c *Client) exportedUUID(kind ImportKind, id int64) (string, error) {
	resp, err := c.DoRequest(http.MethodGet, fmt.Sprintf("/api/v1/%s/export/?q=!(%d)", kind.Path, id), nil)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return "", fmt.Errorf("failed to export %s %d, status code: %d", kind.Path, id, resp.StatusCode)
	}
	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	objects, err := bundle.Objects(content, kind.Bundle)
	if err != nil {
		return "", fmt.Errorf("export of %s %d: %w", kind.Path, id, err)
	}
	if len(objects) == 0 {
		return "", fmt.Errorf("export of %s %d holds no %s", kind.Path, id, kind.Bundle.Dir)
	}
	return objects[0].UUID, nil
}

// ObjectExists reports whether the object of the kind with the given id exists.
func (c *Client) ObjectExists(kind ImportKind, id int64) (bool, error) {
	resp, err := c.DoRequest(http.MethodGet, fmt.Sprintf("/api/v1/%s/%d", kind.Path, id), nil)
	if err != nil {
		return false, err
	}
	err = decodeResponse(resp, nil, fmt.Sprintf("fetch %s %d", kind.Path, id))
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	return err == nil, err
}

// DeleteObject deletes the object of the kind with the given id; an object that no longer exists
// is reported as ErrNotFound.
func (c *Client) DeleteObject(kind ImportKind, id int64) error {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodDelete, fmt.Sprintf("/api/v1/%s/%d", kind.Path, id), nil, headers, cookies)
	if err != nil {
		return err
	}
	return decodeResponse(resp, nil, fmt.Sprintf("delete %s %d", kind.Path, id))
}
