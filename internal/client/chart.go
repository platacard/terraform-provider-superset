package client

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

const chartsPath = "/api/v1/chart/"

// Chart is a visualisation of a dataset.
type Chart struct {
	ID                   int64
	Name                 string
	VizType              string
	DatasourceID         int64
	DatasourceType       string
	Params               string
	QueryContext         string
	Description          string
	CacheTimeout         *int64
	CertifiedBy          string
	CertificationDetails string
	Owners               []int64
	// Tags are the names of the tags people attached to the object; the owner, type and
	// favorite relations Superset keeps as tags are left out.
	Tags []string
	// IsManagedExternally marks the chart as managed outside Superset, which shows a banner
	// and disables its editing in the UI; ExternalURL is the link of that banner. Superset
	// accepts the URL but never returns it.
	IsManagedExternally bool
	ExternalURL         string
	// UUID is the identifier of the chart in export files. It is only read: Superset 3.1
	// does not return it.
	UUID string
	// Dashboards are the ids of the dashboards the chart is placed on. A nil slice leaves them
	// as they are in Superset; a non-nil slice, even an empty one, replaces them.
	Dashboards []int64
}

// chartPayload is the body of the create and update requests. Nullable fields are sent as null
// when empty, so an update can clear them. Owners are only sent when set: Superset makes the
// current user the owner of a chart created without owners.
type chartPayload struct {
	Name                 string  `json:"slice_name"`
	VizType              string  `json:"viz_type"`
	DatasourceID         int64   `json:"datasource_id"`
	DatasourceType       string  `json:"datasource_type"`
	Params               string  `json:"params"`
	QueryContext         *string `json:"query_context"`
	Description          *string `json:"description"`
	CacheTimeout         *int64  `json:"cache_timeout"`
	CertifiedBy          *string `json:"certified_by"`
	CertificationDetails *string `json:"certification_details"`
	Owners               []int64 `json:"owners,omitempty"`
	IsManagedExternally  bool    `json:"is_managed_externally"`
	ExternalURL          *string `json:"external_url"`
	// Dashboards is a pointer so an empty list is sent, which clears the dashboards, while a
	// nil one is left out, which keeps them.
	Dashboards *[]int64 `json:"dashboards,omitempty"`
}

func newChartPayload(chart Chart) chartPayload {
	var dashboards *[]int64
	if chart.Dashboards != nil {
		ids := nonNil(chart.Dashboards)
		dashboards = &ids
	}
	return chartPayload{
		Dashboards:           dashboards,
		Name:                 chart.Name,
		VizType:              chart.VizType,
		DatasourceID:         chart.DatasourceID,
		DatasourceType:       chart.DatasourceType,
		Params:               chart.Params,
		QueryContext:         nullIfEmpty(chart.QueryContext),
		Description:          nullIfEmpty(chart.Description),
		CacheTimeout:         chart.CacheTimeout,
		CertifiedBy:          nullIfEmpty(chart.CertifiedBy),
		CertificationDetails: nullIfEmpty(chart.CertificationDetails),
		Owners:               chart.Owners,
		IsManagedExternally:  chart.IsManagedExternally,
		ExternalURL:          nullIfEmpty(chart.ExternalURL),
	}
}

// chartResult is the chart as the single-chart endpoint returns it. Superset 3.1 leaves the
// datasource out of this response, and recent versions return the cache timeout as a string.
type chartResult struct {
	Name                 string       `json:"slice_name"`
	VizType              string       `json:"viz_type"`
	DatasourceID         int64        `json:"datasource_id"`
	DatasourceType       string       `json:"datasource_type"`
	Params               string       `json:"params"`
	QueryContext         string       `json:"query_context"`
	Description          string       `json:"description"`
	CacheTimeout         looseInt     `json:"cache_timeout"`
	CertifiedBy          string       `json:"certified_by"`
	CertificationDetails string       `json:"certification_details"`
	Owners               []namedItem  `json:"owners"`
	Tags                 []tagSummary `json:"tags"`
	IsManagedExternally  bool         `json:"is_managed_externally"`
	UUID                 string       `json:"uuid"`
	Dashboards           []namedItem  `json:"dashboards"`
}

// chartDatasource is the part of a chart list row that names its datasource.
type chartDatasource struct {
	DatasourceID   int64  `json:"datasource_id"`
	DatasourceType string `json:"datasource_type"`
}

// looseInt is an optional integer that Superset returns either as a number or as a string,
// depending on the version.
type looseInt struct {
	value *int64
}

func (l *looseInt) UnmarshalJSON(data []byte) error {
	text := strings.Trim(string(data), `"`)
	if text == "null" || text == "" {
		l.value = nil
		return nil
	}
	n, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return fmt.Errorf("%s is not an integer", data)
	}
	l.value = &n
	return nil
}

func chartPath(id int64) string {
	return fmt.Sprintf("%s%d", chartsPath, id)
}

// CreateChart creates a chart and returns its id.
func (c *Client) CreateChart(chart Chart) (int64, error) {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return 0, err
	}

	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodPost, chartsPath, newChartPayload(chart), headers, cookies)
	if err != nil {
		return 0, err
	}

	var created struct {
		ID int64 `json:"id"`
	}
	if err := decodeResponse(resp, &created, "create chart"); err != nil {
		return 0, err
	}
	return created.ID, nil
}

// GetChart returns the chart with the given id, or ErrNotFound. When the single-chart response
// leaves the datasource out, as Superset 3.1 does, it is taken from the list endpoint instead.
func (c *Client) GetChart(id int64) (*Chart, error) {
	resp, err := c.DoRequest(http.MethodGet, chartPath(id), nil)
	if err != nil {
		return nil, err
	}

	var body struct {
		Result chartResult `json:"result"`
	}
	if err := decodeResponse(resp, &body, "fetch chart"); err != nil {
		return nil, err
	}

	chart := &Chart{
		ID:                   id,
		Name:                 body.Result.Name,
		VizType:              body.Result.VizType,
		DatasourceID:         body.Result.DatasourceID,
		DatasourceType:       body.Result.DatasourceType,
		Params:               body.Result.Params,
		QueryContext:         body.Result.QueryContext,
		Description:          body.Result.Description,
		CacheTimeout:         body.Result.CacheTimeout.value,
		CertifiedBy:          body.Result.CertifiedBy,
		CertificationDetails: body.Result.CertificationDetails,
		Owners:               ids(body.Result.Owners),
		Tags:                 customTagNames(body.Result.Tags),
		IsManagedExternally:  body.Result.IsManagedExternally,
		UUID:                 body.Result.UUID,
		Dashboards:           ids(body.Result.Dashboards),
	}
	if chart.DatasourceID == 0 {
		if err := c.fillChartDatasource(chart); err != nil {
			return nil, err
		}
	}
	return chart, nil
}

func (c *Client) fillChartDatasource(chart *Chart) error {
	rows, err := listInto[chartDatasource](c, chartsPath, hasID(chart.ID))
	if err != nil {
		return fmt.Errorf("chart %d datasource: %w", chart.ID, err)
	}
	if len(rows) == 0 {
		return fmt.Errorf("chart %d datasource: %w", chart.ID, ErrNotFound)
	}
	chart.DatasourceID, chart.DatasourceType = rows[0].DatasourceID, rows[0].DatasourceType
	return nil
}

// UpdateChart replaces the attributes of a chart.
func (c *Client) UpdateChart(id int64, chart Chart) error {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodPut, chartPath(id), newChartPayload(chart), headers, cookies)
	if err != nil {
		return err
	}
	return decodeResponse(resp, nil, "update chart")
}

// DeleteChart deletes a chart; a chart that no longer exists is reported as ErrNotFound.
func (c *Client) DeleteChart(id int64) error {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodDelete, chartPath(id), nil, headers, cookies)
	if err != nil {
		return err
	}
	return decodeResponse(resp, nil, "delete chart")
}

// ChartSummary is a chart as the list endpoint describes it.
type ChartSummary struct {
	ID             int64  `json:"id"`
	Name           string `json:"slice_name"`
	VizType        string `json:"viz_type"`
	DatasourceID   int64  `json:"datasource_id"`
	DatasourceType string `json:"datasource_type"`
	Description    string `json:"description"`
	URL            string `json:"url"`
}

// FindCharts returns the charts with exactly the given name, on the given datasource when one
// is set. The name is compared again because the server comparison can be case-insensitive.
func (c *Client) FindCharts(name string, datasourceID *int64) ([]ChartSummary, error) {
	filters := []listFilter{equals("slice_name", name)}
	if datasourceID != nil {
		filters = append(filters, equalsInt("datasource_id", *datasourceID))
	}
	rows, err := listInto[ChartSummary](c, chartsPath, filters...)
	if err != nil {
		return nil, fmt.Errorf("charts named %q: %w", name, err)
	}
	matches := make([]ChartSummary, 0, len(rows))
	for _, row := range rows {
		if row.Name == name {
			matches = append(matches, row)
		}
	}
	return matches, nil
}

// ChartFilters narrows the charts a listing returns. Every filter that is set must match;
// unset filters (nil pointers and empty strings) are not sent.
type ChartFilters struct {
	OwnerID      *int64
	Tag          string
	VizType      string
	DatasourceID *int64
	DashboardID  *int64
}

// ListCharts returns every chart that matches the filters, in the order of their ids. The
// server narrows the list; the owner, the tag and the dashboard are compared again because
// the server ignores an unknown owner or dashboard and matches tag names loosely.
func (c *Client) ListCharts(filters ChartFilters) ([]ChartSummary, error) {
	var query []listFilter
	if filters.OwnerID != nil {
		query = append(query, relatedToMany("owners", *filters.OwnerID))
	}
	if filters.Tag != "" {
		query = append(query, taggedWith("chart_tags", filters.Tag))
	}
	if filters.VizType != "" {
		query = append(query, equals("viz_type", filters.VizType))
	}
	if filters.DatasourceID != nil {
		query = append(query, equalsInt("datasource_id", *filters.DatasourceID))
	}
	if filters.DashboardID != nil {
		query = append(query, relatedToMany("dashboards", *filters.DashboardID))
	}

	type row struct {
		ChartSummary
		Owners     []idItem    `json:"owners"`
		Tags       []namedItem `json:"tags"`
		Dashboards []idItem    `json:"dashboards"`
	}
	rows, err := listInto[row](c, chartsPath, query...)
	if err != nil {
		return nil, fmt.Errorf("charts: %w", err)
	}

	charts := make([]ChartSummary, 0, len(rows))
	for _, r := range rows {
		switch {
		case filters.OwnerID != nil && !containsID(r.Owners, *filters.OwnerID):
		case filters.Tag != "" && !hasTag(r.Tags, filters.Tag):
		case filters.VizType != "" && r.VizType != filters.VizType:
		case filters.DatasourceID != nil && r.DatasourceID != *filters.DatasourceID:
		case filters.DashboardID != nil && !containsID(r.Dashboards, *filters.DashboardID):
		default:
			charts = append(charts, r.ChartSummary)
		}
	}
	sort.Slice(charts, func(i, j int) bool { return charts[i].ID < charts[j].ID })
	return charts, nil
}
