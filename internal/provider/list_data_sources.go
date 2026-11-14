package provider

import (
	"context"
	"fmt"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/platacard/terraform-provider-superset/internal/client"
)

// The list data sources return every chart or dashboard that matches a few filters, so a
// configuration can act on a set of objects: send every chart of a dashboard, embed every
// published dashboard, or tag every chart of an owner.

// idsValue returns the ids as a list value.
func idsValue(ctx context.Context, ids []int64) (types.List, diag.Diagnostics) {
	return types.ListValueFrom(ctx, types.Int64Type, nonNilIDs(ids))
}

// Charts

var (
	_ datasource.DataSource              = &chartsDataSource{}
	_ datasource.DataSourceWithConfigure = &chartsDataSource{}
)

// NewChartsDataSource is a helper function to simplify the provider implementation.
func NewChartsDataSource() datasource.DataSource {
	return &chartsDataSource{}
}

// chartsDataSource lists the charts that match its filters.
type chartsDataSource struct {
	client *client.Client
}

type chartsDataSourceModel struct {
	OwnerID      types.Int64  `tfsdk:"owner_id"`
	Tag          types.String `tfsdk:"tag"`
	VizType      types.String `tfsdk:"viz_type"`
	DatasourceID types.Int64  `tfsdk:"datasource_id"`
	DashboardID  types.Int64  `tfsdk:"dashboard_id"`
	IDs          types.List   `tfsdk:"ids"`
	Charts       []chartItem  `tfsdk:"charts"`
}

type chartItem struct {
	ID             types.Int64  `tfsdk:"id"`
	Name           types.String `tfsdk:"name"`
	VizType        types.String `tfsdk:"viz_type"`
	DatasourceID   types.Int64  `tfsdk:"datasource_id"`
	DatasourceType types.String `tfsdk:"datasource_type"`
	Description    types.String `tfsdk:"description"`
	URL            types.String `tfsdk:"url"`
}

func (d *chartsDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_charts"
}

func (d *chartsDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Lists the charts that match every filter that is set, for example every chart of a dashboard to send with `superset_report`, or every chart of a dataset. Without filters, every chart. The list is ordered by id.",
		Attributes: map[string]schema.Attribute{
			"owner_id": schema.Int64Attribute{
				Description: "Keep the charts owned by the user with this id.",
				Optional:    true,
			},
			"tag": schema.StringAttribute{
				Description: "Keep the charts that carry the tag with exactly this name.",
				Optional:    true,
			},
			"viz_type": schema.StringAttribute{
				Description: "Keep the charts with exactly this visualization type, for example `table`.",
				Optional:    true,
			},
			"datasource_id": schema.Int64Attribute{
				Description: "Keep the charts of the dataset with this id.",
				Optional:    true,
			},
			"dashboard_id": schema.Int64Attribute{
				Description: "Keep the charts placed on the dashboard with this id.",
				Optional:    true,
			},
			"ids": schema.ListAttribute{
				Description: "Identifiers of the charts, in the order of the list.",
				Computed:    true,
				ElementType: types.Int64Type,
			},
			"charts": schema.ListNestedAttribute{
				Description: "The charts that match.",
				Computed:    true,
				NestedObject: schema.NestedAttributeObject{
					Attributes: map[string]schema.Attribute{
						"id": schema.Int64Attribute{
							Description: "Numeric identifier of the chart.",
							Computed:    true,
						},
						"name": schema.StringAttribute{
							Description: "Name of the chart.",
							Computed:    true,
						},
						"viz_type": schema.StringAttribute{
							Description: "Visualization type of the chart.",
							Computed:    true,
						},
						"datasource_id": schema.Int64Attribute{
							Description: "Identifier of the dataset of the chart.",
							Computed:    true,
						},
						"datasource_type": schema.StringAttribute{
							Description: "Type of the dataset of the chart, usually `table`.",
							Computed:    true,
						},
						"description": schema.StringAttribute{
							Description: "Description of the chart.",
							Computed:    true,
						},
						"url": schema.StringAttribute{
							Description: "Path of the chart in Superset.",
							Computed:    true,
						},
					},
				},
			},
		},
	}
}

func (d *chartsDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var config chartsDataSourceModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &config)...)
	if resp.Diagnostics.HasError() {
		return
	}

	charts, err := d.client.ListCharts(client.ChartFilters{
		OwnerID:      config.OwnerID.ValueInt64Pointer(),
		Tag:          config.Tag.ValueString(),
		VizType:      config.VizType.ValueString(),
		DatasourceID: config.DatasourceID.ValueInt64Pointer(),
		DashboardID:  config.DashboardID.ValueInt64Pointer(),
	})
	if err != nil {
		resp.Diagnostics.AddError("Unable to List Superset Charts", fmt.Sprintf("Could not list the charts: %s", err))
		return
	}

	ids := make([]int64, 0, len(charts))
	config.Charts = make([]chartItem, 0, len(charts))
	for _, chart := range charts {
		ids = append(ids, chart.ID)
		config.Charts = append(config.Charts, chartItem{
			ID:             types.Int64Value(chart.ID),
			Name:           types.StringValue(chart.Name),
			VizType:        types.StringValue(chart.VizType),
			DatasourceID:   types.Int64Value(chart.DatasourceID),
			DatasourceType: types.StringValue(chart.DatasourceType),
			Description:    optionalString(chart.Description),
			URL:            optionalString(chart.URL),
		})
	}
	var diags diag.Diagnostics
	config.IDs, diags = idsValue(ctx, ids)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &config)...)
}

func (d *chartsDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	d.client = dataSourceClient(req, &resp.Diagnostics)
}

// Dashboards

var (
	_ datasource.DataSource              = &dashboardsDataSource{}
	_ datasource.DataSourceWithConfigure = &dashboardsDataSource{}
)

// NewDashboardsDataSource is a helper function to simplify the provider implementation.
func NewDashboardsDataSource() datasource.DataSource {
	return &dashboardsDataSource{}
}

// dashboardsDataSource lists the dashboards that match its filters.
type dashboardsDataSource struct {
	client *client.Client
}

type dashboardsDataSourceModel struct {
	OwnerID    types.Int64     `tfsdk:"owner_id"`
	Tag        types.String    `tfsdk:"tag"`
	Published  types.Bool      `tfsdk:"published"`
	IDs        types.List      `tfsdk:"ids"`
	Dashboards []dashboardItem `tfsdk:"dashboards"`
}

type dashboardItem struct {
	ID        types.Int64  `tfsdk:"id"`
	Title     types.String `tfsdk:"title"`
	Slug      types.String `tfsdk:"slug"`
	Published types.Bool   `tfsdk:"published"`
	Status    types.String `tfsdk:"status"`
	URL       types.String `tfsdk:"url"`
}

func (d *dashboardsDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_dashboards"
}

func (d *dashboardsDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Lists the dashboards that match every filter that is set, for example every published dashboard to embed with `superset_dashboard_embedding`, or every dashboard of an owner. Without filters, every dashboard. The list is ordered by id.",
		Attributes: map[string]schema.Attribute{
			"owner_id": schema.Int64Attribute{
				Description: "Keep the dashboards owned by the user with this id.",
				Optional:    true,
			},
			"tag": schema.StringAttribute{
				Description: "Keep the dashboards that carry the tag with exactly this name.",
				Optional:    true,
			},
			"published": schema.BoolAttribute{
				Description: "Keep the published dashboards (`true`) or the drafts (`false`).",
				Optional:    true,
			},
			"ids": schema.ListAttribute{
				Description: "Identifiers of the dashboards, in the order of the list.",
				Computed:    true,
				ElementType: types.Int64Type,
			},
			"dashboards": schema.ListNestedAttribute{
				Description: "The dashboards that match.",
				Computed:    true,
				NestedObject: schema.NestedAttributeObject{
					Attributes: map[string]schema.Attribute{
						"id": schema.Int64Attribute{
							Description: "Numeric identifier of the dashboard.",
							Computed:    true,
						},
						"title": schema.StringAttribute{
							Description: "Title of the dashboard.",
							Computed:    true,
						},
						"slug": schema.StringAttribute{
							Description: "Slug of the dashboard; null when it has none.",
							Computed:    true,
						},
						"published": schema.BoolAttribute{
							Description: "Whether the dashboard is published.",
							Computed:    true,
						},
						"status": schema.StringAttribute{
							Description: "`published` or `draft`.",
							Computed:    true,
						},
						"url": schema.StringAttribute{
							Description: "Path of the dashboard in Superset.",
							Computed:    true,
						},
					},
				},
			},
		},
	}
}

func (d *dashboardsDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var config dashboardsDataSourceModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &config)...)
	if resp.Diagnostics.HasError() {
		return
	}

	dashboards, err := d.client.ListDashboards(client.DashboardFilters{
		OwnerID:   config.OwnerID.ValueInt64Pointer(),
		Tag:       config.Tag.ValueString(),
		Published: config.Published.ValueBoolPointer(),
	})
	if err != nil {
		resp.Diagnostics.AddError("Unable to List Superset Dashboards", fmt.Sprintf("Could not list the dashboards: %s", err))
		return
	}

	ids := make([]int64, 0, len(dashboards))
	config.Dashboards = make([]dashboardItem, 0, len(dashboards))
	for _, dashboard := range dashboards {
		ids = append(ids, dashboard.ID)
		config.Dashboards = append(config.Dashboards, dashboardItem{
			ID:        types.Int64Value(dashboard.ID),
			Title:     types.StringValue(dashboard.Title),
			Slug:      optionalString(dashboard.Slug),
			Published: types.BoolValue(dashboard.Published),
			Status:    types.StringValue(dashboard.Status),
			URL:       optionalString(dashboard.URL),
		})
	}
	var diags diag.Diagnostics
	config.IDs, diags = idsValue(ctx, ids)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &config)...)
}

func (d *dashboardsDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	d.client = dataSourceClient(req, &resp.Diagnostics)
}
