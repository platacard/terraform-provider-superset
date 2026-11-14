package client

import (
	"fmt"
	"net/http"
)

// CacheWarmup is the outcome of warming up the cache of a chart: the status Superset reports
// and, when the chart could not be computed, the reason.
type CacheWarmup struct {
	ChartID   int64   `json:"chart_id"`
	VizStatus *string `json:"viz_status"`
	VizError  *string `json:"viz_error"`
}

// WarmUpChartCache computes the chart and stores its result in the cache, in the context of
// the filters of a dashboard when one is given. A chart that does not exist is reported as
// ErrNotFound.
func (c *Client) WarmUpChartCache(chartID int64, dashboardID *int64) (*CacheWarmup, error) {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return nil, err
	}

	payload := map[string]int64{"chart_id": chartID}
	if dashboardID != nil {
		payload["dashboard_id"] = *dashboardID
	}
	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodPut, chartsPath+"warm_up_cache", payload, headers, cookies)
	if err != nil {
		return nil, err
	}

	var body struct {
		Result []CacheWarmup `json:"result"`
	}
	if err := decodeResponse(resp, &body, fmt.Sprintf("warm up the cache of chart %d", chartID)); err != nil {
		return nil, err
	}
	if len(body.Result) == 0 {
		return &CacheWarmup{ChartID: chartID}, nil
	}
	return &body.Result[0], nil
}

// InvalidateCache drops the cached results of the datasets with the given uids, which have
// the form "<id>__table". Uids of datasets that do not exist are ignored by Superset.
func (c *Client) InvalidateCache(datasourceUIDs []string) error {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	payload := map[string][]string{"datasource_uids": nonNilStrings(datasourceUIDs)}
	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodPost, "/api/v1/cachekey/invalidate", payload, headers, cookies)
	if err != nil {
		return err
	}
	return decodeResponse(resp, nil, "invalidate the cache")
}

func nonNilStrings(values []string) []string {
	if values == nil {
		return []string{}
	}
	return values
}
