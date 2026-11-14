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

// lookupKind names a kind of object in the diagnostics of a lookup.
type lookupKind struct {
	title string // "Saved Query", for the summary
	one   string // "saved query", for the detail
	many  string // "saved queries", for the count of an ambiguous lookup
}

// oneMatch returns the only match of a lookup. It reports an error when nothing matched, or
// when several objects matched; the advice then tells how to tell them apart.
func oneMatch[T any](diags *diag.Diagnostics, matches []T, id func(T) int64, kind lookupKind, name, advice string) (T, bool) {
	var none T
	switch len(matches) {
	case 0:
		diags.AddError("Unable to Read Superset "+kind.title, fmt.Sprintf("Could not look up %s %q: %s", kind.one, name, client.ErrNotFound))
		return none, false
	case 1:
		return matches[0], true
	default:
		ids := make([]int64, 0, len(matches))
		for _, match := range matches {
			ids = append(ids, id(match))
		}
		diags.AddError("Ambiguous Superset "+kind.title, fmt.Sprintf("%d %s are named %q (ids %s); %s", len(matches), kind.many, name, idList(ids), advice))
		return none, false
	}
}

// Saved query

var (
	_ datasource.DataSource              = &savedQueryDataSource{}
	_ datasource.DataSourceWithConfigure = &savedQueryDataSource{}
)

// NewSavedQueryDataSource is a helper function to simplify the provider implementation.
func NewSavedQueryDataSource() datasource.DataSource {
	return &savedQueryDataSource{}
}

// savedQueryDataSource looks up one saved query by its label.
type savedQueryDataSource struct {
	client *client.Client
}

type savedQueryDataSourceModel struct {
	ID           types.Int64  `tfsdk:"id"`
	Label        types.String `tfsdk:"label"`
	DatabaseID   types.Int64  `tfsdk:"database_id"`
	DatabaseName types.String `tfsdk:"database_name"`
	Schema       types.String `tfsdk:"schema"`
	Catalog      types.String `tfsdk:"catalog"`
	SQL          types.String `tfsdk:"sql"`
	Description  types.String `tfsdk:"description"`
}

func (d *savedQueryDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_saved_query"
}

func (d *savedQueryDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Looks up a saved query of SQL Lab by its label, for example to reuse its SQL in a virtual dataset or in `superset_sql_query`. Labels are not unique in Superset: the lookup fails when no saved query matches, or when several do.",
		Attributes: map[string]schema.Attribute{
			"label": schema.StringAttribute{
				Description: "Label of the saved query. The comparison is exact.",
				Required:    true,
			},
			"database_id": schema.Int64Attribute{
				Description: "Identifier of the database connection of the query. Set it when several saved queries share the label.",
				Optional:    true,
				Computed:    true,
			},
			"id": schema.Int64Attribute{
				Description: "Numeric identifier of the saved query.",
				Computed:    true,
			},
			"database_name": schema.StringAttribute{
				Description: "Name of the database connection of the query.",
				Computed:    true,
			},
			"schema": schema.StringAttribute{
				Description: "Schema the query runs in; null when none is set.",
				Computed:    true,
			},
			"catalog": schema.StringAttribute{
				Description: "Catalog the query runs in, on Superset 4.1 and later; null when none is set.",
				Computed:    true,
			},
			"sql": schema.StringAttribute{
				Description: "SQL of the query.",
				Computed:    true,
			},
			"description": schema.StringAttribute{
				Description: "Description of the query.",
				Computed:    true,
			},
		},
	}
}

func (d *savedQueryDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var config savedQueryDataSourceModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &config)...)
	if resp.Diagnostics.HasError() {
		return
	}

	label := config.Label.ValueString()
	if label == "" {
		resp.Diagnostics.AddAttributeError(path.Root("label"), "Missing Saved Query Label", "The label must not be empty.")
		return
	}

	queries, err := d.client.FindSavedQueries(label, config.DatabaseID.ValueInt64Pointer())
	if err != nil {
		resp.Diagnostics.AddError("Unable to Read Superset Saved Query", fmt.Sprintf("Could not look up saved query %q: %s", label, err))
		return
	}
	query, ok := oneMatch(&resp.Diagnostics, queries, func(q client.SavedQuerySummary) int64 { return q.ID },
		lookupKind{"Saved Query", "saved query", "saved queries"}, label, "set database_id to tell them apart, or rename them.")
	if !ok {
		return
	}

	config.ID = types.Int64Value(query.ID)
	config.DatabaseID = types.Int64Value(query.Database.ID)
	config.DatabaseName = types.StringValue(query.Database.Name)
	config.Schema = optionalString(query.Schema)
	config.Catalog = optionalString(query.Catalog)
	config.SQL = types.StringValue(query.SQL)
	config.Description = optionalString(query.Description)
	resp.Diagnostics.Append(resp.State.Set(ctx, &config)...)
}

func (d *savedQueryDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	d.client = dataSourceClient(req, &resp.Diagnostics)
}

// Annotation layer

var (
	_ datasource.DataSource              = &annotationLayerDataSource{}
	_ datasource.DataSourceWithConfigure = &annotationLayerDataSource{}
)

// NewAnnotationLayerDataSource is a helper function to simplify the provider implementation.
func NewAnnotationLayerDataSource() datasource.DataSource {
	return &annotationLayerDataSource{}
}

// annotationLayerDataSource looks up one annotation layer by its name.
type annotationLayerDataSource struct {
	client *client.Client
}

type annotationLayerDataSourceModel struct {
	ID          types.Int64  `tfsdk:"id"`
	Name        types.String `tfsdk:"name"`
	Description types.String `tfsdk:"description"`
}

func (d *annotationLayerDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_annotation_layer"
}

func (d *annotationLayerDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Looks up an annotation layer by its name, for example to add annotations to a layer that is not managed by Terraform. Layer names are not unique in Superset: the lookup fails when no layer matches, or when several do.",
		Attributes: map[string]schema.Attribute{
			"name": schema.StringAttribute{
				Description: "Name of the layer. The comparison is exact.",
				Required:    true,
			},
			"id": schema.Int64Attribute{
				Description: "Numeric identifier of the layer, for `superset_annotation.layer_id`.",
				Computed:    true,
			},
			"description": schema.StringAttribute{
				Description: "Description of the layer.",
				Computed:    true,
			},
		},
	}
}

func (d *annotationLayerDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var config annotationLayerDataSourceModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &config)...)
	if resp.Diagnostics.HasError() {
		return
	}

	name := config.Name.ValueString()
	if name == "" {
		resp.Diagnostics.AddAttributeError(path.Root("name"), "Missing Annotation Layer Name", "The layer name must not be empty.")
		return
	}

	layers, err := d.client.FindAnnotationLayers(name)
	if err != nil {
		resp.Diagnostics.AddError("Unable to Read Superset Annotation Layer", fmt.Sprintf("Could not look up annotation layer %q: %s", name, err))
		return
	}
	layer, ok := oneMatch(&resp.Diagnostics, layers, func(l client.AnnotationLayer) int64 { return l.ID },
		lookupKind{"Annotation Layer", "annotation layer", "annotation layers"}, name, "rename them, or import the layer by its id.")
	if !ok {
		return
	}

	config.ID = types.Int64Value(layer.ID)
	config.Description = optionalString(layer.Description)
	resp.Diagnostics.Append(resp.State.Set(ctx, &config)...)
}

func (d *annotationLayerDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	d.client = dataSourceClient(req, &resp.Diagnostics)
}

// Tag

var (
	_ datasource.DataSource              = &tagDataSource{}
	_ datasource.DataSourceWithConfigure = &tagDataSource{}
)

// NewTagDataSource is a helper function to simplify the provider implementation.
func NewTagDataSource() datasource.DataSource {
	return &tagDataSource{}
}

// tagDataSource looks up one tag by its name.
type tagDataSource struct {
	client *client.Client
}

type tagDataSourceModel struct {
	ID          types.Int64  `tfsdk:"id"`
	Name        types.String `tfsdk:"name"`
	Description types.String `tfsdk:"description"`
}

func (d *tagDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_tag"
}

func (d *tagDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Looks up a tag by its name, for example to reference its id or to check that it exists. Tag names are unique; the lookup fails when no tag has exactly that name. The tag API needs the `TAGGING_SYSTEM` feature flag.",
		Attributes: map[string]schema.Attribute{
			"name": schema.StringAttribute{
				Description: "Name of the tag. The comparison is exact.",
				Required:    true,
			},
			"id": schema.Int64Attribute{
				Description: "Numeric identifier of the tag.",
				Computed:    true,
			},
			"description": schema.StringAttribute{
				Description: "Description of the tag.",
				Computed:    true,
			},
		},
	}
}

func (d *tagDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var config tagDataSourceModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &config)...)
	if resp.Diagnostics.HasError() {
		return
	}

	name := config.Name.ValueString()
	if name == "" {
		resp.Diagnostics.AddAttributeError(path.Root("name"), "Missing Tag Name", "The tag name must not be empty.")
		return
	}

	tag, err := d.client.FindTagByName(name)
	if err != nil {
		resp.Diagnostics.AddError("Unable to Read Superset Tag", fmt.Sprintf("Could not look up tag %q: %s", name, err))
		return
	}

	config.ID = types.Int64Value(tag.ID)
	config.Description = optionalString(tag.Description)
	resp.Diagnostics.Append(resp.State.Set(ctx, &config)...)
}

func (d *tagDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	d.client = dataSourceClient(req, &resp.Diagnostics)
}

// Report

var (
	_ datasource.DataSource              = &reportDataSource{}
	_ datasource.DataSourceWithConfigure = &reportDataSource{}
)

// NewReportDataSource is a helper function to simplify the provider implementation.
func NewReportDataSource() datasource.DataSource {
	return &reportDataSource{}
}

// reportDataSource looks up one report or alert by its name.
type reportDataSource struct {
	client *client.Client
}

type reportDataSourceModel struct {
	ID          types.Int64  `tfsdk:"id"`
	Name        types.String `tfsdk:"name"`
	Type        types.String `tfsdk:"type"`
	Description types.String `tfsdk:"description"`
	Crontab     types.String `tfsdk:"crontab"`
	Timezone    types.String `tfsdk:"timezone"`
	Active      types.Bool   `tfsdk:"active"`
	ChartID     types.Int64  `tfsdk:"chart_id"`
	DashboardID types.Int64  `tfsdk:"dashboard_id"`
	LastState   types.String `tfsdk:"last_state"`
}

func (d *reportDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_report"
}

func (d *reportDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Looks up a report or an alert by its name, for example to check its schedule or its last state. A name is unique per type, so an alert and a report can share it: set `type` to tell them apart. The report API needs the `ALERT_REPORTS` feature flag.",
		Attributes: map[string]schema.Attribute{
			"name": schema.StringAttribute{
				Description: "Name of the report or alert. The comparison is exact.",
				Required:    true,
			},
			"type": schema.StringAttribute{
				Description: "`Report` or `Alert`. Set it when an alert and a report share the name.",
				Optional:    true,
				Computed:    true,
			},
			"id": schema.Int64Attribute{
				Description: "Numeric identifier of the report.",
				Computed:    true,
			},
			"description": schema.StringAttribute{
				Description: "Description of the report.",
				Computed:    true,
			},
			"crontab": schema.StringAttribute{
				Description: "Schedule of the report, as a cron expression.",
				Computed:    true,
			},
			"timezone": schema.StringAttribute{
				Description: "Time zone of the schedule.",
				Computed:    true,
			},
			"active": schema.BoolAttribute{
				Description: "Whether the report runs on its schedule.",
				Computed:    true,
			},
			"chart_id": schema.Int64Attribute{
				Description: "Identifier of the chart the report sends; null for a dashboard report.",
				Computed:    true,
			},
			"dashboard_id": schema.Int64Attribute{
				Description: "Identifier of the dashboard the report sends; null for a chart report.",
				Computed:    true,
			},
			"last_state": schema.StringAttribute{
				Description: "State of the last run, for example `Success`, `Error` or `Not triggered`.",
				Computed:    true,
			},
		},
	}
}

func (d *reportDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var config reportDataSourceModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &config)...)
	if resp.Diagnostics.HasError() {
		return
	}

	name := config.Name.ValueString()
	if name == "" {
		resp.Diagnostics.AddAttributeError(path.Root("name"), "Missing Report Name", "The report name must not be empty.")
		return
	}
	reportType := config.Type.ValueString()
	if reportType != "" && reportType != "Report" && reportType != "Alert" {
		resp.Diagnostics.AddAttributeError(path.Root("type"), "Invalid Report Type", fmt.Sprintf("The type must be Report or Alert, not %q.", reportType))
		return
	}

	reports, err := d.client.FindReports(name, reportType)
	if err != nil {
		resp.Diagnostics.AddError("Unable to Read Superset Report", fmt.Sprintf("Could not look up report %q: %s", name, err))
		return
	}
	report, ok := oneMatch(&resp.Diagnostics, reports, func(r client.ReportSummary) int64 { return r.ID },
		lookupKind{"Report", "report", "reports"}, name, "set type to Report or Alert to tell them apart.")
	if !ok {
		return
	}

	config.ID = types.Int64Value(report.ID)
	config.Type = types.StringValue(report.Type)
	config.Description = optionalString(report.Description)
	config.Crontab = types.StringValue(report.Crontab)
	config.Timezone = optionalString(report.Timezone)
	config.Active = types.BoolValue(report.Active)
	config.ChartID = types.Int64PointerValue(report.ChartID)
	config.DashboardID = types.Int64PointerValue(report.DashboardID)
	config.LastState = optionalString(report.LastState)
	resp.Diagnostics.Append(resp.State.Set(ctx, &config)...)
}

func (d *reportDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	d.client = dataSourceClient(req, &resp.Diagnostics)
}

// Theme

var (
	_ datasource.DataSource              = &themeDataSource{}
	_ datasource.DataSourceWithConfigure = &themeDataSource{}
)

// NewThemeDataSource is a helper function to simplify the provider implementation.
func NewThemeDataSource() datasource.DataSource {
	return &themeDataSource{}
}

// themeDataSource looks up one theme by its name.
type themeDataSource struct {
	client *client.Client
}

type themeDataSourceModel struct {
	ID            types.Int64  `tfsdk:"id"`
	Name          types.String `tfsdk:"name"`
	UUID          types.String `tfsdk:"uuid"`
	JSONData      types.String `tfsdk:"json_data"`
	IsSystem      types.Bool   `tfsdk:"is_system"`
	SystemDefault types.Bool   `tfsdk:"system_default"`
	SystemDark    types.Bool   `tfsdk:"system_dark"`
}

func (d *themeDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_theme"
}

func (d *themeDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Looks up a theme by its name, on Superset 6, for example to apply one of the built-in themes (`THEME_DEFAULT`, `THEME_DARK`) to a dashboard or to read its tokens. Theme names are not unique: the lookup fails when no theme matches, or when several do.",
		Attributes: map[string]schema.Attribute{
			"name": schema.StringAttribute{
				Description: "Name of the theme. The comparison is exact.",
				Required:    true,
			},
			"id": schema.Int64Attribute{
				Description: "Numeric identifier of the theme.",
				Computed:    true,
			},
			"uuid": schema.StringAttribute{
				Description: "UUID of the theme, as export bundles reference it.",
				Computed:    true,
			},
			"json_data": schema.StringAttribute{
				Description: "Design tokens of the theme, as a JSON document.",
				Computed:    true,
			},
			"is_system": schema.BoolAttribute{
				Description: "Whether the theme is one of the built-in themes of Superset, which cannot be edited.",
				Computed:    true,
			},
			"system_default": schema.BoolAttribute{
				Description: "Whether the theme is the default theme of the instance.",
				Computed:    true,
			},
			"system_dark": schema.BoolAttribute{
				Description: "Whether the theme is the dark theme of the instance.",
				Computed:    true,
			},
		},
	}
}

func (d *themeDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var config themeDataSourceModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &config)...)
	if resp.Diagnostics.HasError() {
		return
	}

	name := config.Name.ValueString()
	if name == "" {
		resp.Diagnostics.AddAttributeError(path.Root("name"), "Missing Theme Name", "The theme name must not be empty.")
		return
	}

	themes, err := d.client.FindThemes(name)
	if err != nil {
		resp.Diagnostics.AddError("Unable to Read Superset Theme", fmt.Sprintf("Could not look up theme %q: %s", name, err))
		return
	}
	theme, ok := oneMatch(&resp.Diagnostics, themes, func(t client.Theme) int64 { return t.ID },
		lookupKind{"Theme", "theme", "themes"}, name, "rename them, or import the theme by its id.")
	if !ok {
		return
	}

	config.ID = types.Int64Value(theme.ID)
	config.UUID = optionalString(theme.UUID)
	config.JSONData = types.StringValue(theme.JSONData)
	config.IsSystem = types.BoolValue(theme.IsSystem)
	config.SystemDefault = types.BoolValue(theme.SystemDefault)
	config.SystemDark = types.BoolValue(theme.SystemDark)
	resp.Diagnostics.Append(resp.State.Set(ctx, &config)...)
}

func (d *themeDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	d.client = dataSourceClient(req, &resp.Diagnostics)
}
