package provider

import (
	"context"
	"fmt"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/platacard/terraform-provider-superset/internal/client"
)

var (
	_ datasource.DataSource              = &slackChannelsDataSource{}
	_ datasource.DataSourceWithConfigure = &slackChannelsDataSource{}
)

// NewSlackChannelsDataSource is a helper function to simplify the provider implementation.
func NewSlackChannelsDataSource() datasource.DataSource {
	return &slackChannelsDataSource{}
}

// slackChannelsDataSource lists the Slack channels reports can be sent to.
type slackChannelsDataSource struct {
	client *client.Client
}

type slackChannelsDataSourceModel struct {
	Search   types.String       `tfsdk:"search"`
	Channels []slackChannelItem `tfsdk:"channels"`
}

type slackChannelItem struct {
	ID   types.String `tfsdk:"id"`
	Name types.String `tfsdk:"name"`
}

func (d *slackChannelsDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_slack_channels"
}

func (d *slackChannelsDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Lists the channels of the Slack workspace Superset is connected to, with the ids that recipients of the `SlackV2` type of `superset_report` take. Superset asks Slack with its own token, so the instance needs Superset 4.1 or later, the `ALERT_REPORTS` feature flag and a `SLACK_API_TOKEN` that may list channels.",
		Attributes: map[string]schema.Attribute{
			"search": schema.StringAttribute{
				Description: "Keep the channels whose name contains this text.",
				Optional:    true,
			},
			"channels": schema.ListNestedAttribute{
				Description: "The channels, as Slack lists them.",
				Computed:    true,
				NestedObject: schema.NestedAttributeObject{
					Attributes: map[string]schema.Attribute{
						"id": schema.StringAttribute{
							Description: "Identifier of the channel in Slack, for example `C0123456789`.",
							Computed:    true,
						},
						"name": schema.StringAttribute{
							Description: "Name of the channel, without the `#`.",
							Computed:    true,
						},
					},
				},
			},
		},
	}
}

func (d *slackChannelsDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var config slackChannelsDataSourceModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &config)...)
	if resp.Diagnostics.HasError() {
		return
	}

	channels, err := d.client.SlackChannels(config.Search.ValueString())
	if err != nil {
		resp.Diagnostics.AddError("Unable to List the Slack Channels of Superset", fmt.Sprintf("Could not list the Slack channels: %s", err))
		return
	}

	config.Channels = make([]slackChannelItem, 0, len(channels))
	for _, channel := range channels {
		config.Channels = append(config.Channels, slackChannelItem{ID: types.StringValue(channel.ID), Name: types.StringValue(channel.Name)})
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &config)...)
}

func (d *slackChannelsDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	d.client = dataSourceClient(req, &resp.Diagnostics)
}
