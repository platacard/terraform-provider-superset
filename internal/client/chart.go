package client

import (
	"fmt"
	"net/http"
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
}

func newChartPayload(chart Chart) chartPayload {
	return chartPayload{
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
	}
}

// chartResult is the chart as the single-chart endpoint returns it. Superset 3.1 leaves the
// datasource out of this response, and recent versions return the cache timeout as a string.
type chartResult struct {
	Name                 string      `json:"slice_name"`
	VizType              string      `json:"viz_type"`
	DatasourceID         int64       `json:"datasource_id"`
	DatasourceType       string      `json:"datasource_type"`
	Params               string      `json:"params"`
	QueryContext         string      `json:"query_context"`
	Description          string      `json:"description"`
	CacheTimeout         looseInt    `json:"cache_timeout"`
	CertifiedBy          string      `json:"certified_by"`
	CertificationDetails string      `json:"certification_details"`
	Owners               []namedItem `json:"owners"`
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
