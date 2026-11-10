package provider

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/platacard/terraform-provider-superset/internal/client"
)

// The lookup data sources find one object by the name a person knows it by, so a
// configuration can reference objects that are not managed by Terraform. Each lookup is exact
// and fails when nothing matches, or when several objects match and the attributes do not
// tell them apart.

// dataSourceClient takes the client out of the provider data, for the data sources.
func dataSourceClient(req datasource.ConfigureRequest, diags *diag.Diagnostics) *client.Client {
	if req.ProviderData == nil {
		return nil
	}
	c, ok := req.ProviderData.(*client.Client)
	if !ok {
		diags.AddError(
			"Unexpected Data Source Configure Type",
			fmt.Sprintf("Expected *client.Client, got: %T. Please report this issue to the provider developers.", req.ProviderData),
		)
		return nil
	}
	return c
}

// idList renders ids for an error message about several matches.
func idList(ids []int64) string {
	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, strconv.FormatInt(id, 10))
	}
	return strings.Join(parts, ", ")
}

// Database

var (
	_ datasource.DataSource              = &databaseDataSource{}
	_ datasource.DataSourceWithConfigure = &databaseDataSource{}
)

// NewDatabaseDataSource is a helper function to simplify the provider implementation.
func NewDatabaseDataSource() datasource.DataSource {
	return &databaseDataSource{}
}

// databaseDataSource looks up one database connection by its name.
type databaseDataSource struct {
	client *client.Client
}

type databaseDataSourceModel struct {
	ID             types.Int64  `tfsdk:"id"`
	DatabaseName   types.String `tfsdk:"database_name"`
	Backend        types.String `tfsdk:"backend"`
	UUID           types.String `tfsdk:"uuid"`
	ExposeInSQLLab types.Bool   `tfsdk:"expose_in_sqllab"`
	AllowDML       types.Bool   `tfsdk:"allow_dml"`
	AllowRunAsync  types.Bool   `tfsdk:"allow_run_async"`
}

func (d *databaseDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_database"
}

func (d *databaseDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Looks up a database connection by its name, for example to reference its id or to check its settings. The lookup fails when no connection has exactly that name.",
		Attributes: map[string]schema.Attribute{
			"database_name": schema.StringAttribute{
				Description: "Name of the connection. The comparison is exact.",
				Required:    true,
			},
			"id": schema.Int64Attribute{
				Description: "Numeric identifier of the connection.",
				Computed:    true,
			},
			"backend": schema.StringAttribute{
				Description: "SQLAlchemy dialect of the connection, for example `postgresql`.",
				Computed:    true,
			},
			"uuid": schema.StringAttribute{
				Description: "UUID of the connection, as export bundles reference it.",
				Computed:    true,
			},
			"expose_in_sqllab": schema.BoolAttribute{
				Description: "Whether the connection is available in SQL Lab.",
				Computed:    true,
			},
			"allow_dml": schema.BoolAttribute{
				Description: "Whether SQL Lab may run statements that modify data.",
				Computed:    true,
			},
			"allow_run_async": schema.BoolAttribute{
				Description: "Whether queries run asynchronously.",
				Computed:    true,
			},
		},
	}
}

func (d *databaseDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var config databaseDataSourceModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &config)...)
	if resp.Diagnostics.HasError() {
		return
	}

	name := config.DatabaseName.ValueString()
	if name == "" {
		resp.Diagnostics.AddAttributeError(path.Root("database_name"), "Missing Database Name", "The database name must not be empty.")
		return
	}

	database, err := d.client.FindDatabaseByName(name)
	if err != nil {
		resp.Diagnostics.AddError("Unable to Read Superset Database", fmt.Sprintf("Could not look up database %q: %s", name, err))
		return
	}

	config.ID = types.Int64Value(database.ID)
	config.Backend = types.StringValue(database.Backend)
	config.UUID = optionalString(database.UUID)
	config.ExposeInSQLLab = types.BoolValue(database.ExposeInSQLLab)
	config.AllowDML = types.BoolValue(database.AllowDML)
	config.AllowRunAsync = types.BoolValue(database.AllowRunAsync)
	resp.Diagnostics.Append(resp.State.Set(ctx, &config)...)
}

func (d *databaseDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	d.client = dataSourceClient(req, &resp.Diagnostics)
}

// Dataset

var (
	_ datasource.DataSource              = &datasetDataSource{}
	_ datasource.DataSourceWithConfigure = &datasetDataSource{}
)

// NewDatasetDataSource is a helper function to simplify the provider implementation.
func NewDatasetDataSource() datasource.DataSource {
	return &datasetDataSource{}
}

// datasetDataSource looks up one dataset by its table name, schema and database.
type datasetDataSource struct {
	client *client.Client
}

type datasetDataSourceModel struct {
	ID           types.Int64  `tfsdk:"id"`
	TableName    types.String `tfsdk:"table_name"`
	Schema       types.String `tfsdk:"schema"`
	DatabaseName types.String `tfsdk:"database_name"`
	DatabaseID   types.Int64  `tfsdk:"database_id"`
	SQL          types.String `tfsdk:"sql"`
	Kind         types.String `tfsdk:"kind"`
	Perm         types.String `tfsdk:"perm"`
	SchemaPerm   types.String `tfsdk:"schema_perm"`
}

func (d *datasetDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_dataset"
}

func (d *datasetDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Looks up a dataset by its table name, narrowed down by its schema and database connection, for example to reference its id in a chart or its permission in `superset_role_permissions`. The lookup fails when no dataset matches, or when several do.",
		Attributes: map[string]schema.Attribute{
			"table_name": schema.StringAttribute{
				Description: "Name of the table, or name of the virtual dataset. The comparison is exact.",
				Required:    true,
			},
			"schema": schema.StringAttribute{
				Description: "Schema of the table. Set it when several datasets share the table name.",
				Optional:    true,
				Computed:    true,
			},
			"database_name": schema.StringAttribute{
				Description: "Name of the database connection. Set it when several datasets share the table name.",
				Optional:    true,
				Computed:    true,
			},
			"id": schema.Int64Attribute{
				Description: "Numeric identifier of the dataset.",
				Computed:    true,
			},
			"database_id": schema.Int64Attribute{
				Description: "Numeric identifier of the database connection.",
				Computed:    true,
			},
			"sql": schema.StringAttribute{
				Description: "SQL query of a virtual dataset; null for a table.",
				Computed:    true,
			},
			"kind": schema.StringAttribute{
				Description: "`physical` for a table, `virtual` for a query.",
				Computed:    true,
			},
			"perm": schema.StringAttribute{
				Description: "View menu of the `datasource_access` permission of the dataset, for `superset_role_permissions`.",
				Computed:    true,
			},
			"schema_perm": schema.StringAttribute{
				Description: "View menu of the `schema_access` permission of the schema of the dataset; null without a schema.",
				Computed:    true,
			},
		},
	}
}

func (d *datasetDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var config datasetDataSourceModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &config)...)
	if resp.Diagnostics.HasError() {
		return
	}

	table := config.TableName.ValueString()
	if table == "" {
		resp.Diagnostics.AddAttributeError(path.Root("table_name"), "Missing Table Name", "The table name must not be empty.")
		return
	}

	var databaseID *int64
	if name := config.DatabaseName.ValueString(); name != "" {
		database, err := d.client.FindDatabaseByName(name)
		if err != nil {
			resp.Diagnostics.AddAttributeError(path.Root("database_name"), "Unable to Find the Database of the Dataset", err.Error())
			return
		}
		databaseID = &database.ID
	}

	datasets, err := d.client.FindDatasets(table, config.Schema.ValueString(), databaseID)
	if err != nil {
		resp.Diagnostics.AddError("Unable to Read Superset Dataset", fmt.Sprintf("Could not look up dataset %q: %s", table, err))
		return
	}
	switch len(datasets) {
	case 0:
		resp.Diagnostics.AddError("Unable to Read Superset Dataset", fmt.Sprintf("Could not look up dataset %q: %s", table, client.ErrNotFound))
		return
	case 1:
	default:
		ids := make([]int64, 0, len(datasets))
		for _, dataset := range datasets {
			ids = append(ids, dataset.ID)
		}
		resp.Diagnostics.AddError("Ambiguous Superset Dataset", fmt.Sprintf("%d datasets are named %q (ids %s); set schema and database_name to tell them apart.", len(datasets), table, idList(ids)))
		return
	}

	dataset := datasets[0]
	config.ID = types.Int64Value(dataset.ID)
	config.Schema = optionalString(dataset.Schema)
	config.DatabaseName = types.StringValue(dataset.Database.Name)
	config.DatabaseID = types.Int64Value(dataset.Database.ID)
	config.SQL = optionalString(dataset.SQL)
	config.Kind = types.StringValue(dataset.Kind)
	config.Perm, config.SchemaPerm = datasetPermissions(dataset.Database.Name, dataset.TableName, dataset.Schema, dataset.ID)
	resp.Diagnostics.Append(resp.State.Set(ctx, &config)...)
}

func (d *datasetDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	d.client = dataSourceClient(req, &resp.Diagnostics)
}

// Chart

var (
	_ datasource.DataSource              = &chartDataSource{}
	_ datasource.DataSourceWithConfigure = &chartDataSource{}
)

// NewChartDataSource is a helper function to simplify the provider implementation.
func NewChartDataSource() datasource.DataSource {
	return &chartDataSource{}
}

// chartDataSource looks up one chart by its name.
type chartDataSource struct {
	client *client.Client
}

type chartDataSourceModel struct {
	ID             types.Int64  `tfsdk:"id"`
	Name           types.String `tfsdk:"name"`
	DatasourceID   types.Int64  `tfsdk:"datasource_id"`
	DatasourceType types.String `tfsdk:"datasource_type"`
	VizType        types.String `tfsdk:"viz_type"`
	Description    types.String `tfsdk:"description"`
	URL            types.String `tfsdk:"url"`
}

func (d *chartDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_chart"
}

func (d *chartDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Looks up a chart by its name, for example to place it on a dashboard or to send it with `superset_report`. Chart names are not unique in Superset: the lookup fails when no chart matches, or when several do.",
		Attributes: map[string]schema.Attribute{
			"name": schema.StringAttribute{
				Description: "Name of the chart. The comparison is exact.",
				Required:    true,
			},
			"datasource_id": schema.Int64Attribute{
				Description: "Identifier of the dataset of the chart. Set it when several charts share the name.",
				Optional:    true,
				Computed:    true,
			},
			"id": schema.Int64Attribute{
				Description: "Numeric identifier of the chart.",
				Computed:    true,
			},
			"datasource_type": schema.StringAttribute{
				Description: "Type of the dataset of the chart, usually `table`.",
				Computed:    true,
			},
			"viz_type": schema.StringAttribute{
				Description: "Visualization type of the chart, for example `echarts_timeseries_line`.",
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
	}
}

func (d *chartDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var config chartDataSourceModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &config)...)
	if resp.Diagnostics.HasError() {
		return
	}

	name := config.Name.ValueString()
	if name == "" {
		resp.Diagnostics.AddAttributeError(path.Root("name"), "Missing Chart Name", "The chart name must not be empty.")
		return
	}

	charts, err := d.client.FindCharts(name, config.DatasourceID.ValueInt64Pointer())
	if err != nil {
		resp.Diagnostics.AddError("Unable to Read Superset Chart", fmt.Sprintf("Could not look up chart %q: %s", name, err))
		return
	}
	switch len(charts) {
	case 0:
		resp.Diagnostics.AddError("Unable to Read Superset Chart", fmt.Sprintf("Could not look up chart %q: %s", name, client.ErrNotFound))
		return
	case 1:
	default:
		ids := make([]int64, 0, len(charts))
		for _, chart := range charts {
			ids = append(ids, chart.ID)
		}
		resp.Diagnostics.AddError("Ambiguous Superset Chart", fmt.Sprintf("%d charts are named %q (ids %s); set datasource_id to tell them apart, or rename them.", len(charts), name, idList(ids)))
		return
	}

	chart := charts[0]
	config.ID = types.Int64Value(chart.ID)
	config.DatasourceID = types.Int64Value(chart.DatasourceID)
	config.DatasourceType = types.StringValue(chart.DatasourceType)
	config.VizType = types.StringValue(chart.VizType)
	config.Description = optionalString(chart.Description)
	config.URL = optionalString(chart.URL)
	resp.Diagnostics.Append(resp.State.Set(ctx, &config)...)
}

func (d *chartDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	d.client = dataSourceClient(req, &resp.Diagnostics)
}

// Dashboard

var (
	_ datasource.DataSource              = &dashboardDataSource{}
	_ datasource.DataSourceWithConfigure = &dashboardDataSource{}
)

// NewDashboardDataSource is a helper function to simplify the provider implementation.
func NewDashboardDataSource() datasource.DataSource {
	return &dashboardDataSource{}
}

// dashboardDataSource looks up one dashboard by its slug or its title.
type dashboardDataSource struct {
	client *client.Client
}

type dashboardDataSourceModel struct {
	ID        types.Int64  `tfsdk:"id"`
	Slug      types.String `tfsdk:"slug"`
	Title     types.String `tfsdk:"title"`
	Published types.Bool   `tfsdk:"published"`
	Status    types.String `tfsdk:"status"`
	URL       types.String `tfsdk:"url"`
}

func (d *dashboardDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_dashboard"
}

func (d *dashboardDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Looks up a dashboard by its slug or its title, for example to embed it with `superset_dashboard_embedding` or to send it with `superset_report`. Slugs are unique; titles are not, so a lookup by title fails when several dashboards share it.",
		Attributes: map[string]schema.Attribute{
			"slug": schema.StringAttribute{
				Description: "Slug of the dashboard, the last part of its URL. Set either `slug` or `title`.",
				Optional:    true,
				Computed:    true,
			},
			"title": schema.StringAttribute{
				Description: "Title of the dashboard. The comparison is exact. Set either `slug` or `title`.",
				Optional:    true,
				Computed:    true,
			},
			"id": schema.Int64Attribute{
				Description: "Numeric identifier of the dashboard.",
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
	}
}

func (d *dashboardDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var config dashboardDataSourceModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &config)...)
	if resp.Diagnostics.HasError() {
		return
	}

	slug, title := config.Slug.ValueString(), config.Title.ValueString()
	if (slug == "") == (title == "") {
		resp.Diagnostics.AddAttributeError(path.Root("slug"), "Invalid Dashboard Lookup", "Set either slug or title.")
		return
	}

	dashboards, err := d.client.FindDashboards(slug, title)
	if err != nil {
		resp.Diagnostics.AddError("Unable to Read Superset Dashboard", fmt.Sprintf("Could not look up dashboard %q: %s", slug+title, err))
		return
	}
	switch len(dashboards) {
	case 0:
		resp.Diagnostics.AddError("Unable to Read Superset Dashboard", fmt.Sprintf("Could not look up dashboard %q: %s", slug+title, client.ErrNotFound))
		return
	case 1:
	default:
		ids := make([]int64, 0, len(dashboards))
		for _, dashboard := range dashboards {
			ids = append(ids, dashboard.ID)
		}
		resp.Diagnostics.AddError("Ambiguous Superset Dashboard", fmt.Sprintf("%d dashboards are titled %q (ids %s); look the dashboard up by its slug.", len(dashboards), title, idList(ids)))
		return
	}

	dashboard := dashboards[0]
	config.ID = types.Int64Value(dashboard.ID)
	config.Slug = optionalString(dashboard.Slug)
	config.Title = types.StringValue(dashboard.Title)
	config.Published = types.BoolValue(dashboard.Published)
	config.Status = types.StringValue(dashboard.Status)
	config.URL = optionalString(dashboard.URL)
	resp.Diagnostics.Append(resp.State.Set(ctx, &config)...)
}

func (d *dashboardDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	d.client = dataSourceClient(req, &resp.Diagnostics)
}
