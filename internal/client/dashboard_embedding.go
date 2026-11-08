package client

import (
	"fmt"
	"net/http"
)

// DashboardEmbedding is the embedding configuration of a dashboard: the uuid under which the
// embedded SDK loads the dashboard and the domains allowed to embed it.
type DashboardEmbedding struct {
	DashboardID    int64
	UUID           string
	AllowedDomains []string
}

// dashboardEmbeddingPayload is the body of the request that enables or updates the embedding.
// An empty list of domains allows every domain.
type dashboardEmbeddingPayload struct {
	AllowedDomains []string `json:"allowed_domains"`
}

// dashboardEmbeddingResult is the configuration as the API returns it.
type dashboardEmbeddingResult struct {
	UUID           string   `json:"uuid"`
	AllowedDomains []string `json:"allowed_domains"`
}

func dashboardEmbeddingPath(dashboardID int64) string {
	return fmt.Sprintf("%s%d/embedded", dashboardsPath, dashboardID)
}

// SetDashboardEmbedding enables the embedding of a dashboard with the given allowed domains, or
// replaces the domains when it is already enabled, and returns the uuid of the embedded
// dashboard. The uuid stays the same for as long as the embedding is enabled.
func (c *Client) SetDashboardEmbedding(dashboardID int64, allowedDomains []string) (string, error) {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return "", err
	}

	if allowedDomains == nil {
		allowedDomains = []string{}
	}
	payload := dashboardEmbeddingPayload{AllowedDomains: allowedDomains}
	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodPost, dashboardEmbeddingPath(dashboardID), payload, headers, cookies)
	if err != nil {
		return "", err
	}

	var body struct {
		Result dashboardEmbeddingResult `json:"result"`
	}
	if err := decodeResponse(resp, &body, "enable dashboard embedding"); err != nil {
		return "", err
	}
	return body.Result.UUID, nil
}

// GetDashboardEmbedding returns the embedding configuration of a dashboard. ErrNotFound means
// that the dashboard is not embeddable or does not exist.
func (c *Client) GetDashboardEmbedding(dashboardID int64) (*DashboardEmbedding, error) {
	resp, err := c.DoRequest(http.MethodGet, dashboardEmbeddingPath(dashboardID), nil)
	if err != nil {
		return nil, err
	}

	var body struct {
		Result dashboardEmbeddingResult `json:"result"`
	}
	if err := decodeResponse(resp, &body, "fetch dashboard embedding"); err != nil {
		return nil, err
	}

	embedding := &DashboardEmbedding{
		DashboardID:    dashboardID,
		UUID:           body.Result.UUID,
		AllowedDomains: body.Result.AllowedDomains,
	}
	return embedding, nil
}

// DeleteDashboardEmbedding disables the embedding of a dashboard. Superset reports success for
// a dashboard that is not embeddable, and ErrNotFound for a dashboard that does not exist.
func (c *Client) DeleteDashboardEmbedding(dashboardID int64) error {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodDelete, dashboardEmbeddingPath(dashboardID), nil, headers, cookies)
	if err != nil {
		return err
	}
	return decodeResponse(resp, nil, "disable dashboard embedding")
}
