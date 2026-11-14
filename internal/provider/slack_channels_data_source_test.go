package provider

import (
	"net/http"
	"regexp"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/jarcoal/httpmock"
)

func TestAccSlackChannelsDataSource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/report/slack_channels/",
		func(req *http.Request) (*http.Response, error) {
			if req.URL.Query().Get("q") == "(search_string:'sales')" {
				return httpmock.NewStringResponse(200, `{"result": [{"id": "C002", "name": "sales"}]}`), nil
			}
			return httpmock.NewStringResponse(200, `{"result": [{"id": "C001", "name": "general"}, {"id": "C002", "name": "sales"}]}`), nil
		})

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
data "superset_slack_channels" "all" {}

data "superset_slack_channels" "sales" {
  search = "sales"
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.superset_slack_channels.all", "channels.#", "2"),
					resource.TestCheckResourceAttr("data.superset_slack_channels.all", "channels.0.id", "C001"),
					resource.TestCheckResourceAttr("data.superset_slack_channels.all", "channels.0.name", "general"),
					resource.TestCheckResourceAttr("data.superset_slack_channels.sales", "channels.#", "1"),
					resource.TestCheckResourceAttr("data.superset_slack_channels.sales", "channels.0.id", "C002"),
				),
			},
		},
	})
}

// Without a Slack token Superset answers 422; without the endpoint, 404. Both get the hint.
func TestAccSlackChannelsDataSourceUnavailable(t *testing.T) {
	for name, response := range map[string]*http.Response{
		"no token":    httpmock.NewStringResponse(422, `{"message": "Failed to list channels: The request to the Slack API failed. (url: https://slack.com/api/conversations.list)\nThe server responded with: {'ok': False, 'error': 'not_authed'}"}`),
		"no endpoint": httpmock.NewStringResponse(404, missingRoute),
	} {
		t.Run(name, func(t *testing.T) {
			httpmock.Activate()
			defer httpmock.DeactivateAndReset()
			httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
				httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))
			httpmock.RegisterResponder("GET", "http://superset-host/api/v1/report/slack_channels/", httpmock.ResponderFromResponse(response))

			resource.Test(t, resource.TestCase{
				PreCheck:                 func() { testAccPreCheck(t) },
				ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
				Steps: []resource.TestStep{
					{
						Config: providerConfig + `
data "superset_slack_channels" "test" {}
`,
						ExpectError: regexp.MustCompile(`SLACK_API_TOKEN`),
					},
				},
			})
		})
	}
}
