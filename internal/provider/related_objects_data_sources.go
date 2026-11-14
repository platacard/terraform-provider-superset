package provider

import (
	"context"
	"fmt"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/platacard/terraform-provider-superset/internal/client"
)

// The related objects data sources tell which charts and dashboards depend on a dataset or
// on a database connection, for example to check what a replacement would break or to act
// on every chart of a dataset.

type relatedChartItem struct {
	ID      types.Int64  `tfsdk:"id"`
	Name    types.String `tfsdk:"name"`
	VizType types.String `tfsdk:"viz_type"`
}

type relatedDashboardItem struct {
	ID    types.Int64  `tfsdk:"id"`
	Title types.String `tfsdk:"title"`
	Slug  types.String `tfsdk:"slug"`
}

// relatedObjectsAttributes are the computed attributes both data sources share.
func relatedObjectsAttributes() map[string]schema.Attribute {
	return map[string]schema.Attribute{
		"chart_ids": schema.ListAttribute{
			Description: "Identifiers of the charts.",
			Computed:    true,
			ElementType: types.Int64Type,
		},
		"dashboard_ids": schema.ListAttribute{
			Description: "Identifiers of the dashboards.",
			Computed:    true,
			ElementType: types.Int64Type,
		},
		"charts": schema.ListNestedAttribute{
			Description: "The charts, in the order Superset lists them.",
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
				},
			},
		},
		"dashboards": schema.ListNestedAttribute{
			Description: "The dashboards with at least one of the charts, in the order Superset lists them.",
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
				},
			},
		},
	}
}

// relatedObjectsValues maps the related objects to the shared attributes.
func relatedObjectsValues(ctx context.Context, objects *client.RelatedObjects) (charts []relatedChartItem, dashboards []relatedDashboardItem, chartIDs, dashboardIDs types.List, diags diag.Diagnostics) {
	charts = make([]relatedChartItem, 0, len(objects.Charts))
	ids := make([]int64, 0, len(objects.Charts))
	for _, chart := range objects.Charts {
		ids = append(ids, chart.ID)
		charts = append(charts, relatedChartItem{ID: types.Int64Value(chart.ID), Name: types.StringValue(chart.Name), VizType: types.StringValue(chart.VizType)})
	}
	chartIDs, d := idsValue(ctx, ids)
	diags.Append(d...)

	dashboards = make([]relatedDashboardItem, 0, len(objects.Dashboards))
	ids = make([]int64, 0, len(objects.Dashboards))
	for _, dashboard := range objects.Dashboards {
		ids = append(ids, dashboard.ID)
		dashboards = append(dashboards, relatedDashboardItem{ID: types.Int64Value(dashboard.ID), Title: types.StringValue(dashboard.Title), Slug: optionalString(dashboard.Slug)})
	}
	dashboardIDs, d = idsValue(ctx, ids)
	diags.Append(d...)
	return charts, dashboards, chartIDs, dashboardIDs, diags
}

// Dataset

var (
	_ datasource.DataSource              = &datasetRelatedObjectsDataSource{}
	_ datasource.DataSourceWithConfigure = &datasetRelatedObjectsDataSource{}
)

// NewDatasetRelatedObjectsDataSource is a helper function to simplify the provider implementation.
func NewDatasetRelatedObjectsDataSource() datasource.DataSource {
	return &datasetRelatedObjectsDataSource{}
}

// datasetRelatedObjectsDataSource lists the charts and dashboards built on a dataset.
type datasetRelatedObjectsDataSource struct {
	client *client.Client
}

type datasetRelatedObjectsModel struct {
	DatasetID    types.Int64            `tfsdk:"dataset_id"`
	ChartIDs     types.List             `tfsdk:"chart_ids"`
	DashboardIDs types.List             `tfsdk:"dashboard_ids"`
	Charts       []relatedChartItem     `tfsdk:"charts"`
	Dashboards   []relatedDashboardItem `tfsdk:"dashboards"`
}

func (d *datasetRelatedObjectsDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_dataset_related_objects"
}

func (d *datasetRelatedObjectsDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	attributes := relatedObjectsAttributes()
	attributes["dataset_id"] = schema.Int64Attribute{
		Description: "Numeric identifier of the dataset.",
		Required:    true,
	}
	resp.Schema = schema.Schema{
		Description: "Lists the charts built on a dataset and the dashboards those charts are placed on, for example to check what depends on a dataset before replacing it, or to send every chart of a dataset with `superset_report`. The lookup fails when the dataset does not exist.",
		Attributes:  attributes,
	}
}

func (d *datasetRelatedObjectsDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var config datasetRelatedObjectsModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &config)...)
	if resp.Diagnostics.HasError() {
		return
	}

	objects, err := d.client.GetDatasetRelatedObjects(config.DatasetID.ValueInt64())
	if err != nil {
		resp.Diagnostics.AddError("Unable to Read the Related Objects of the Superset Dataset", fmt.Sprintf("Could not read the related objects of dataset %d: %s", config.DatasetID.ValueInt64(), err))
		return
	}

	var diags diag.Diagnostics
	config.Charts, config.Dashboards, config.ChartIDs, config.DashboardIDs, diags = relatedObjectsValues(ctx, objects)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &config)...)
}

func (d *datasetRelatedObjectsDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	d.client = dataSourceClient(req, &resp.Diagnostics)
}

// Database

var (
	_ datasource.DataSource              = &databaseRelatedObjectsDataSource{}
	_ datasource.DataSourceWithConfigure = &databaseRelatedObjectsDataSource{}
)

// NewDatabaseRelatedObjectsDataSource is a helper function to simplify the provider implementation.
func NewDatabaseRelatedObjectsDataSource() datasource.DataSource {
	return &databaseRelatedObjectsDataSource{}
}

// databaseRelatedObjectsDataSource lists the charts and dashboards that depend on a connection.
type databaseRelatedObjectsDataSource struct {
	client *client.Client
}

type databaseRelatedObjectsModel struct {
	DatabaseName types.String           `tfsdk:"database_name"`
	DatabaseID   types.Int64            `tfsdk:"database_id"`
	ChartIDs     types.List             `tfsdk:"chart_ids"`
	DashboardIDs types.List             `tfsdk:"dashboard_ids"`
	Charts       []relatedChartItem     `tfsdk:"charts"`
	Dashboards   []relatedDashboardItem `tfsdk:"dashboards"`
}

func (d *databaseRelatedObjectsDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_database_related_objects"
}

func (d *databaseRelatedObjectsDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	attributes := relatedObjectsAttributes()
	attributes["database_name"] = schema.StringAttribute{
		Description: "Name of the database connection. Exactly one of `database_name` and `database_id` must be set.",
		Optional:    true,
	}
	attributes["database_id"] = schema.Int64Attribute{
		Description: "Numeric identifier of the connection, looked up from `database_name` when that is set.",
		Optional:    true,
		Computed:    true,
	}
	resp.Schema = schema.Schema{
		Description: "Lists the charts whose datasets use a database connection and the dashboards those charts are placed on, for example to check what depends on a connection before replacing it. The lookup fails when the connection does not exist.",
		Attributes:  attributes,
	}
}

func (d *databaseRelatedObjectsDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var config databaseRelatedObjectsModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &config)...)
	if resp.Diagnostics.HasError() {
		return
	}

	name := config.DatabaseName.ValueString()
	if (name == "") == config.DatabaseID.IsNull() {
		resp.Diagnostics.AddAttributeError(path.Root("database_name"), "Invalid Database Reference", "Set exactly one of database_name and database_id.")
		return
	}
	if name != "" {
		database, err := d.client.FindDatabaseByName(name)
		if err != nil {
			resp.Diagnostics.AddAttributeError(path.Root("database_name"), "Unable to Find the Superset Database", fmt.Sprintf("Could not look up database %q: %s", name, err))
			return
		}
		config.DatabaseID = types.Int64Value(database.ID)
	}

	objects, err := d.client.GetDatabaseRelatedObjects(config.DatabaseID.ValueInt64())
	if err != nil {
		resp.Diagnostics.AddError("Unable to Read the Related Objects of the Superset Database", fmt.Sprintf("Could not read the related objects of database %d: %s", config.DatabaseID.ValueInt64(), err))
		return
	}

	var diags diag.Diagnostics
	config.Charts, config.Dashboards, config.ChartIDs, config.DashboardIDs, diags = relatedObjectsValues(ctx, objects)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &config)...)
}

func (d *databaseRelatedObjectsDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	d.client = dataSourceClient(req, &resp.Diagnostics)
}
