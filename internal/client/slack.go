package client

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// slackChannelsHint explains the failures of the Slack channels endpoint.
const slackChannelsHint = "The Slack channels endpoint needs Superset 4.1 or later with the ALERT_REPORTS feature flag, and " +
	"a SLACK_API_TOKEN in superset_config.py that may list channels."

// SlackChannel is a channel of the Slack workspace of the instance, as recipients of the
// SlackV2 type name it.
type SlackChannel struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// SlackChannels returns the channels Superset can send reports to, those whose name contains
// the search text when one is given. Superset asks Slack with its own token, so the request
// fails when the token is missing or cannot list channels.
func (c *Client) SlackChannels(search string) ([]SlackChannel, error) {
	path := reportsPath + "slack_channels/"
	if search != "" {
		path += "?" + url.Values{"q": {"(search_string:" + risonString(search) + ")"}}.Encode()
	}
	resp, err := c.DoRequest(http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}

	var body struct {
		Result []SlackChannel `json:"result"`
	}
	if err := decodeResponse(resp, &body, "list the Slack channels"); err != nil {
		if errors.Is(err, ErrNotFound) || strings.Contains(err.Error(), "status code: 404") || strings.Contains(err.Error(), "status code: 422") {
			return nil, fmt.Errorf("%w\n%s", err, slackChannelsHint)
		}
		return nil, err
	}
	return body.Result, nil
}
