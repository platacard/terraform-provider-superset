package client

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"sort"
)

const dashboardsPath = "/api/v1/dashboard/"

// Dashboard is a collection of charts laid out on a grid. Metadata is the json_metadata
// document and Layout the position_json document of the API.
type Dashboard struct {
	ID                   int64
	Title                string
	Slug                 string
	CSS                  string
	Metadata             string
	Layout               string
	Published            bool
	CertifiedBy          string
	CertificationDetails string
	Owners               []int64
	Roles                []int64
	// Tags are the names of the tags people attached to the object; the owner, type and
	// favorite relations Superset keeps as tags are left out.
	Tags []string
	// ThemeID is the theme the dashboard applies, on Superset 6. It is only sent when set, or
	// as null when ClearTheme is set, since older versions do not know the field.
	ThemeID    *int64
	ClearTheme bool
	// IsManagedExternally marks the dashboard as managed outside Superset, which shows a
	// banner and disables its editing in the UI; ExternalURL is the link of that banner.
	// Superset accepts the URL but never returns it.
	IsManagedExternally bool
	ExternalURL         string
	// UUID is the identifier of the dashboard in export files and for the embedded SDK. It
	// is only read: Superset 3.1 does not return it.
	UUID string
}

// dashboardPayload is the body of the create and update requests. The create endpoint rejects a
// null css, metadata or layout, so those are sent as empty strings when empty, which clears them
// on an update as well. The other nullable fields are sent as null when empty, so an update can
// clear them. Owners are only sent when set: Superset makes the current user the owner of a
// dashboard created without owners. Roles are always sent, so an update can revoke them.
type dashboardPayload struct {
	Title                string  `json:"dashboard_title"`
	Slug                 *string `json:"slug"`
	CSS                  string  `json:"css"`
	Metadata             string  `json:"json_metadata"`
	Layout               string  `json:"position_json"`
	Published            bool    `json:"published"`
	CertifiedBy          *string `json:"certified_by"`
	CertificationDetails *string `json:"certification_details"`
	Owners               []int64 `json:"owners,omitempty"`
	Roles                []int64 `json:"roles"`
	ThemeID              *int64  `json:"theme_id,omitempty"`
	IsManagedExternally  bool    `json:"is_managed_externally"`
	ExternalURL          *string `json:"external_url"`
	clearTheme           bool
}

// MarshalJSON adds a null theme to the payload when the theme is cleared, which omitempty
// could not express.
func (p dashboardPayload) MarshalJSON() ([]byte, error) {
	type plain dashboardPayload
	if !p.clearTheme {
		return marshal(plain(p))
	}
	encoded, err := marshal(plain(p))
	if err != nil {
		return nil, err
	}
	return append(encoded[:len(encoded)-1], []byte(`,"theme_id":null}`)...), nil
}

func newDashboardPayload(dashboard Dashboard, metadata string) dashboardPayload {
	return dashboardPayload{
		ThemeID:              dashboard.ThemeID,
		clearTheme:           dashboard.ClearTheme && dashboard.ThemeID == nil,
		Title:                dashboard.Title,
		Slug:                 nullIfEmpty(dashboard.Slug),
		CSS:                  dashboard.CSS,
		Metadata:             metadata,
		Layout:               dashboard.Layout,
		Published:            dashboard.Published,
		CertifiedBy:          nullIfEmpty(dashboard.CertifiedBy),
		CertificationDetails: nullIfEmpty(dashboard.CertificationDetails),
		Owners:               dashboard.Owners,
		Roles:                nonNil(dashboard.Roles),
		IsManagedExternally:  dashboard.IsManagedExternally,
		ExternalURL:          nullIfEmpty(dashboard.ExternalURL),
	}
}

// dashboardMetadata builds the metadata of an update: the configured keys over the current ones,
// so settings the configuration does not mention survive the update, plus the layout embedded as
// positions, which makes the update endpoint link the charts of the layout to the dashboard. A
// layout sent on its own is stored but its charts are not linked.
func dashboardMetadata(current, configured, layout string) (string, error) {
	document := map[string]json.RawMessage{}
	for _, source := range []string{current, configured} {
		if source == "" {
			continue
		}
		var keys map[string]json.RawMessage
		if err := json.Unmarshal([]byte(source), &keys); err != nil {
			return "", fmt.Errorf("dashboard metadata: %w", err)
		}
		maps.Copy(document, keys)
	}
	if layout != "" {
		document["positions"] = json.RawMessage(layout)
	}
	if len(document) == 0 {
		return "", nil
	}

	encoded, err := json.Marshal(document)
	if err != nil {
		return "", fmt.Errorf("dashboard layout: %w", err)
	}
	return string(encoded), nil
}

// dashboardResult is the dashboard as the API returns it.
type dashboardResult struct {
	Title                string       `json:"dashboard_title"`
	Slug                 string       `json:"slug"`
	CSS                  string       `json:"css"`
	Metadata             string       `json:"json_metadata"`
	Layout               string       `json:"position_json"`
	Published            bool         `json:"published"`
	CertifiedBy          string       `json:"certified_by"`
	CertificationDetails string       `json:"certification_details"`
	Owners               []namedItem  `json:"owners"`
	Roles                []namedItem  `json:"roles"`
	Tags                 []tagSummary `json:"tags"`
	Theme                *struct {
		ID int64 `json:"id"`
	} `json:"theme"`
	IsManagedExternally bool   `json:"is_managed_externally"`
	UUID                string `json:"uuid"`
}

func dashboardPath(id int64) string {
	return fmt.Sprintf("%s%d", dashboardsPath, id)
}

// CreateDashboard creates a dashboard and returns its id. The charts of a layout are only
// linked to the dashboard by an update, so a dashboard with a layout is updated right after
// its creation. When that update fails, the id of the created dashboard is returned with the
// error, so the caller can still record the dashboard.
func (c *Client) CreateDashboard(dashboard Dashboard) (int64, error) {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return 0, err
	}

	payload := newDashboardPayload(dashboard, dashboard.Metadata)
	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodPost, dashboardsPath, payload, headers, cookies)
	if err != nil {
		return 0, err
	}

	var created struct {
		ID int64 `json:"id"`
	}
	if err := decodeResponse(resp, &created, "create dashboard"); err != nil {
		return 0, err
	}
	if dashboard.Layout != "" {
		if err := c.UpdateDashboard(created.ID, dashboard); err != nil {
			return created.ID, fmt.Errorf("link the charts of the layout: %w", err)
		}
	}
	return created.ID, nil
}

// GetDashboard returns the dashboard with the given id, or ErrNotFound.
func (c *Client) GetDashboard(id int64) (*Dashboard, error) {
	resp, err := c.DoRequest(http.MethodGet, dashboardPath(id), nil)
	if err != nil {
		return nil, err
	}

	var body struct {
		Result dashboardResult `json:"result"`
	}
	if err := decodeResponse(resp, &body, "fetch dashboard"); err != nil {
		return nil, err
	}

	dashboard := &Dashboard{
		ID:                   id,
		Title:                body.Result.Title,
		Slug:                 body.Result.Slug,
		CSS:                  body.Result.CSS,
		Metadata:             body.Result.Metadata,
		Layout:               body.Result.Layout,
		Published:            body.Result.Published,
		CertifiedBy:          body.Result.CertifiedBy,
		CertificationDetails: body.Result.CertificationDetails,
		Owners:               ids(body.Result.Owners),
		Roles:                ids(body.Result.Roles),
		Tags:                 customTagNames(body.Result.Tags),
		IsManagedExternally:  body.Result.IsManagedExternally,
		UUID:                 body.Result.UUID,
	}
	if body.Result.Theme != nil {
		dashboard.ThemeID = &body.Result.Theme.ID
	}
	return dashboard, nil
}

// UpdateDashboard replaces the attributes of a dashboard. Keys of the metadata that the given
// dashboard does not set keep their current value, and the layout is embedded in the metadata as
// positions, which makes Superset link its charts to the dashboard.
func (c *Client) UpdateDashboard(id int64, dashboard Dashboard) error {
	current, err := c.GetDashboard(id)
	if err != nil {
		return err
	}
	metadata, err := dashboardMetadata(current.Metadata, dashboard.Metadata, dashboard.Layout)
	if err != nil {
		return err
	}

	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	payload := newDashboardPayload(dashboard, metadata)
	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodPut, dashboardPath(id), payload, headers, cookies)
	if err != nil {
		return err
	}
	return decodeResponse(resp, nil, "update dashboard")
}

// DeleteDashboard deletes a dashboard; a dashboard that no longer exists is reported as ErrNotFound.
func (c *Client) DeleteDashboard(id int64) error {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodDelete, dashboardPath(id), nil, headers, cookies)
	if err != nil {
		return err
	}
	return decodeResponse(resp, nil, "delete dashboard")
}

// DashboardSummary is a dashboard as the list endpoint describes it.
type DashboardSummary struct {
	ID        int64  `json:"id"`
	Title     string `json:"dashboard_title"`
	Slug      string `json:"slug"`
	Published bool   `json:"published"`
	Status    string `json:"status"`
	URL       string `json:"url"`
}

// FindDashboards returns the dashboards with exactly the given slug, or, when the slug is
// empty, exactly the given title. The server filters on equality; the value is compared again
// because the comparison can be case-insensitive depending on the metadata database.
func (c *Client) FindDashboards(slug, title string) ([]DashboardSummary, error) {
	column, value := "slug", slug
	if slug == "" {
		column, value = "dashboard_title", title
	}
	rows, err := listInto[DashboardSummary](c, dashboardsPath, equals(column, value))
	if err != nil {
		return nil, fmt.Errorf("dashboards with %s %q: %w", column, value, err)
	}
	matches := make([]DashboardSummary, 0, len(rows))
	for _, row := range rows {
		if (slug != "" && row.Slug == slug) || (slug == "" && row.Title == title) {
			matches = append(matches, row)
		}
	}
	return matches, nil
}

// DashboardFilters narrows the dashboards a listing returns. Every filter that is set must
// match; unset filters (nil pointers and empty strings) are not sent.
type DashboardFilters struct {
	OwnerID   *int64
	Tag       string
	Published *bool
}

// ListDashboards returns every dashboard that matches the filters, in the order of their
// ids. The server narrows the list; the owner and the tag are compared again because the
// server ignores an unknown owner and matches tag names loosely.
func (c *Client) ListDashboards(filters DashboardFilters) ([]DashboardSummary, error) {
	var query []listFilter
	if filters.OwnerID != nil {
		query = append(query, relatedToMany("owners", *filters.OwnerID))
	}
	if filters.Tag != "" {
		query = append(query, taggedWith("dashboard_tags", filters.Tag))
	}
	if filters.Published != nil {
		query = append(query, equalsBool("published", *filters.Published))
	}

	type row struct {
		DashboardSummary
		Owners []idItem    `json:"owners"`
		Tags   []namedItem `json:"tags"`
	}
	rows, err := listInto[row](c, dashboardsPath, query...)
	if err != nil {
		return nil, fmt.Errorf("dashboards: %w", err)
	}

	dashboards := make([]DashboardSummary, 0, len(rows))
	for _, r := range rows {
		switch {
		case filters.OwnerID != nil && !containsID(r.Owners, *filters.OwnerID):
		case filters.Tag != "" && !hasTag(r.Tags, filters.Tag):
		case filters.Published != nil && r.Published != *filters.Published:
		default:
			dashboards = append(dashboards, r.DashboardSummary)
		}
	}
	sort.Slice(dashboards, func(i, j int) bool { return dashboards[i].ID < dashboards[j].ID })
	return dashboards, nil
}
