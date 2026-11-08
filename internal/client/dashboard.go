package client

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
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
}

// dashboardPayload is the body of the create and update requests. Nullable fields are sent as
// null when empty, so an update can clear them. Owners are only sent when set: Superset makes
// the current user the owner of a dashboard created without owners. Roles are always sent, so
// an update can revoke them.
type dashboardPayload struct {
	Title                string  `json:"dashboard_title"`
	Slug                 *string `json:"slug"`
	CSS                  *string `json:"css"`
	Metadata             *string `json:"json_metadata"`
	Layout               *string `json:"position_json"`
	Published            bool    `json:"published"`
	CertifiedBy          *string `json:"certified_by"`
	CertificationDetails *string `json:"certification_details"`
	Owners               []int64 `json:"owners,omitempty"`
	Roles                []int64 `json:"roles"`
}

func newDashboardPayload(dashboard Dashboard, metadata string) dashboardPayload {
	return dashboardPayload{
		Title:                dashboard.Title,
		Slug:                 nullIfEmpty(dashboard.Slug),
		CSS:                  nullIfEmpty(dashboard.CSS),
		Metadata:             nullIfEmpty(metadata),
		Layout:               nullIfEmpty(dashboard.Layout),
		Published:            dashboard.Published,
		CertifiedBy:          nullIfEmpty(dashboard.CertifiedBy),
		CertificationDetails: nullIfEmpty(dashboard.CertificationDetails),
		Owners:               dashboard.Owners,
		Roles:                nonNil(dashboard.Roles),
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
	Title                string      `json:"dashboard_title"`
	Slug                 string      `json:"slug"`
	CSS                  string      `json:"css"`
	Metadata             string      `json:"json_metadata"`
	Layout               string      `json:"position_json"`
	Published            bool        `json:"published"`
	CertifiedBy          string      `json:"certified_by"`
	CertificationDetails string      `json:"certification_details"`
	Owners               []namedItem `json:"owners"`
	Roles                []namedItem `json:"roles"`
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
