package client

import (
	"fmt"
	"net/http"
	"sort"
)

// dashboardCopyPayload is the body of the copy request. The metadata is required by the
// endpoint; its positions decide which charts the copy shows.
type dashboardCopyPayload struct {
	Title           string `json:"dashboard_title"`
	CSS             string `json:"css"`
	Metadata        string `json:"json_metadata"`
	DuplicateSlices bool   `json:"duplicate_slices"`
}

// CopyDashboard copies a dashboard and returns the id of the copy. The metadata of the copy
// is the metadata of the source with the configured keys over it and the layout of the
// source embedded as positions, which links the charts of the layout to the copy; with
// duplicateSlices, the charts are duplicated first and the copy shows the duplicates. A
// source that does not exist is reported as ErrNotFound.
func (c *Client) CopyDashboard(sourceID int64, title, css, metadata string, duplicateSlices bool) (int64, error) {
	source, err := c.GetDashboard(sourceID)
	if err != nil {
		return 0, fmt.Errorf("source dashboard: %w", err)
	}
	document, err := dashboardMetadata(source.Metadata, metadata, source.Layout)
	if err != nil {
		return 0, err
	}
	if document == "" {
		document = "{}"
	}

	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return 0, err
	}

	payload := dashboardCopyPayload{Title: title, CSS: css, Metadata: document, DuplicateSlices: duplicateSlices}
	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodPost, dashboardPath(sourceID)+"/copy/", payload, headers, cookies)
	if err != nil {
		return 0, err
	}

	var body struct {
		Result struct {
			ID int64 `json:"id"`
		} `json:"result"`
	}
	if err := decodeResponse(resp, &body, "copy dashboard"); err != nil {
		return 0, err
	}
	return body.Result.ID, nil
}

// DashboardChartIDs returns the ids of the charts shown on the dashboard, in ascending order,
// or ErrNotFound when the dashboard does not exist.
func (c *Client) DashboardChartIDs(id int64) ([]int64, error) {
	resp, err := c.DoRequest(http.MethodGet, dashboardPath(id)+"/charts", nil)
	if err != nil {
		return nil, err
	}

	var body struct {
		Result []idItem `json:"result"`
	}
	if err := decodeResponse(resp, &body, "fetch the charts of the dashboard"); err != nil {
		return nil, err
	}
	ids := make([]int64, 0, len(body.Result))
	for _, chart := range body.Result {
		ids = append(ids, chart.ID)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}
