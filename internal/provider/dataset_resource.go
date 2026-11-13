package provider

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/hashicorp/terraform-plugin-framework/attr"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/booldefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/int64planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/setplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/tfsdk"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
	"terraform-provider-superset/internal/client"
)

// Ensure the implementation satisfies the expected interfaces.
var (
	_ resource.Resource                = &datasetResource{}
	_ resource.ResourceWithConfigure   = &datasetResource{}
	_ resource.ResourceWithImportState = &datasetResource{}
	_ resource.ResourceWithModifyPlan  = &datasetResource{}
)

// NewDatasetResource is a helper function to simplify the provider implementation.
func NewDatasetResource() resource.Resource {
	return &datasetResource{}
}

// datasetResource manages a dataset.
type datasetResource struct {
	client *client.Client
}

// datasetResourceModel maps the resource schema data.
type datasetResourceModel struct {
	ID                   types.Int64  `tfsdk:"id"`
	TableName            types.String `tfsdk:"table_name"`
	DatabaseName         types.String `tfsdk:"database_name"`
	Schema               types.String `tfsdk:"schema"`
	SQL                  types.String `tfsdk:"sql"`
	Description          types.String `tfsdk:"description"`
	Owners               types.Set    `tfsdk:"owners"`
	CacheTimeout         types.Int64  `tfsdk:"cache_timeout"`
	MainDttmCol          types.String `tfsdk:"main_dttm_col"`
	TemplateParams       jsonValue    `tfsdk:"template_params"`
	Extra                jsonValue    `tfsdk:"extra"`
	FetchValuesPredicate types.String `tfsdk:"fetch_values_predicate"`
	FilterSelectEnabled  types.Bool   `tfsdk:"filter_select_enabled"`
	NormalizeColumns     types.Bool   `tfsdk:"normalize_columns"`
	AlwaysFilterMainDttm types.Bool   `tfsdk:"always_filter_main_dttm"`
	DefaultEndpoint      types.String `tfsdk:"default_endpoint"`
	Metrics              types.Set    `tfsdk:"metrics"`
	Columns              types.Set    `tfsdk:"columns"`
	Perm                 types.String `tfsdk:"perm"`
	SchemaPerm           types.String `tfsdk:"schema_perm"`
	IsManagedExternally  types.Bool   `tfsdk:"is_managed_externally"`
	ExternalURL          types.String `tfsdk:"external_url"`
	UUID                 types.String `tfsdk:"uuid"`
}

// datasetMetricModel maps one element of the metrics attribute.
type datasetMetricModel struct {
	Name        types.String `tfsdk:"metric_name"`
	Expression  types.String `tfsdk:"expression"`
	VerboseName types.String `tfsdk:"verbose_name"`
	Description types.String `tfsdk:"description"`
	D3Format    types.String `tfsdk:"d3format"`
	WarningText types.String `tfsdk:"warning_text"`
	MetricType  types.String `tfsdk:"metric_type"`
}

var datasetMetricAttributeTypes = map[string]attr.Type{
	"metric_name":  types.StringType,
	"expression":   types.StringType,
	"verbose_name": types.StringType,
	"description":  types.StringType,
	"d3format":     types.StringType,
	"warning_text": types.StringType,
	"metric_type":  types.StringType,
}

// datasetColumnModel maps one element of the columns attribute.
type datasetColumnModel struct {
	Name             types.String `tfsdk:"column_name"`
	Type             types.String `tfsdk:"type"`
	Expression       types.String `tfsdk:"expression"`
	VerboseName      types.String `tfsdk:"verbose_name"`
	Description      types.String `tfsdk:"description"`
	PythonDateFormat types.String `tfsdk:"python_date_format"`
	IsDttm           types.Bool   `tfsdk:"is_dttm"`
	Groupby          types.Bool   `tfsdk:"groupby"`
	Filterable       types.Bool   `tfsdk:"filterable"`
}

var datasetColumnAttributeTypes = map[string]attr.Type{
	"column_name":        types.StringType,
	"type":               types.StringType,
	"expression":         types.StringType,
	"verbose_name":       types.StringType,
	"description":        types.StringType,
	"python_date_format": types.StringType,
	"is_dttm":            types.BoolType,
	"groupby":            types.BoolType,
	"filterable":         types.BoolType,
}

// Metadata returns the resource type name.
func (r *datasetResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_dataset"
}

// Schema defines the schema for the resource.
func (r *datasetResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Manages a dataset: a table of a database, or a virtual dataset defined by a SQL query. Superset detects the columns of the dataset and creates its `count` metric itself; `columns` and `metrics` take them over when set.",
		Attributes: map[string]schema.Attribute{
			"id": schema.Int64Attribute{
				Description: "Numeric identifier of the dataset.",
				Computed:    true,
				PlanModifiers: []planmodifier.Int64{
					int64planmodifier.UseStateForUnknown(),
				},
			},
			"table_name": schema.StringAttribute{
				Description: "Name of the table, or name of the virtual dataset.",
				Required:    true,
			},
			"database_name": schema.StringAttribute{
				Description: "Name of the database connection the dataset belongs to. Changing it replaces the dataset.",
				Required:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"schema": schema.StringAttribute{
				Description: "Schema of the table.",
				Optional:    true,
			},
			"sql": schema.StringAttribute{
				Description: "SQL query of a virtual dataset. Superset runs it when the dataset is created to detect the columns, so it must be valid at that time: Jinja templates need the `ENABLE_TEMPLATE_PROCESSING` feature flag, and on Superset 3.1, whose create endpoint does not take `template_params`, their parameters need a default such as `{{ region | default('eu') }}`.",
				Optional:    true,
			},
			"description": schema.StringAttribute{
				Description: "Free-text description of the dataset.",
				Optional:    true,
			},
			"owners": schema.SetAttribute{
				Description: "Identifiers of the users who own the dataset. Defaults to the user the provider authenticates as.",
				ElementType: types.Int64Type,
				Optional:    true,
				Computed:    true,
				PlanModifiers: []planmodifier.Set{
					setplanmodifier.UseStateForUnknown(),
				},
			},
			"cache_timeout": schema.Int64Attribute{
				Description: "Duration in seconds for which the charts of the dataset are cached. Unset to use the timeout of the database.",
				Optional:    true,
			},
			"main_dttm_col": schema.StringAttribute{
				Description: "Column used as the default time column of the dataset. Superset picks the first datetime column when unset.",
				Optional:    true,
				Computed:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"template_params": schema.StringAttribute{
				Description: "JSON object with the default values of the Jinja template parameters of the SQL query. Recent versions of Superset apply them when the dataset is created; Superset 3.1 only after.",
				Optional:    true,
				CustomType:  jsonDocument,
			},
			"extra": schema.StringAttribute{
				Description: "JSON document with extra settings of the dataset, such as `certification` or `warning_markdown`.",
				Optional:    true,
				CustomType:  jsonDocument,
			},
			"fetch_values_predicate": schema.StringAttribute{
				Description: "SQL predicate applied when Superset fetches the distinct values of a column for a filter.",
				Optional:    true,
			},
			"filter_select_enabled": schema.BoolAttribute{
				Description: "Whether filters of the dataset offer the distinct values of a column. Defaults to `true`.",
				Optional:    true,
				Computed:    true,
				Default:     booldefault.StaticBool(true),
			},
			"normalize_columns": schema.BoolAttribute{
				Description: "Whether the names of the columns are normalized to lower case when they are fetched from the source. Defaults to `false`.",
				Optional:    true,
				Computed:    true,
				Default:     booldefault.StaticBool(false),
			},
			"always_filter_main_dttm": schema.BoolAttribute{
				Description: "Whether the time range of a chart is always applied to the main datetime column, even when the chart filters another one. Defaults to `false`.",
				Optional:    true,
				Computed:    true,
				Default:     booldefault.StaticBool(false),
			},
			"default_endpoint": schema.StringAttribute{
				Description: "URL Superset redirects to when the dataset is opened.",
				Optional:    true,
			},
			"metrics": schema.SetNestedAttribute{
				Description: "Metrics of the dataset. When unset, the metrics are left to Superset, which creates `count` with every dataset. When set, the dataset has exactly these metrics: declare `count` too if it should stay, and metrics not listed here are deleted.",
				Optional:    true,
				Computed:    true,
				PlanModifiers: []planmodifier.Set{
					setplanmodifier.UseStateForUnknown(),
				},
				NestedObject: schema.NestedAttributeObject{
					Attributes: map[string]schema.Attribute{
						"metric_name": schema.StringAttribute{
							Description: "Name of the metric, unique within the dataset.",
							Required:    true,
						},
						"expression": schema.StringAttribute{
							Description: "SQL aggregate expression of the metric, for example `SUM(amount)`.",
							Required:    true,
						},
						"verbose_name": schema.StringAttribute{
							Description: "Label shown instead of the name.",
							Optional:    true,
						},
						"description": schema.StringAttribute{
							Description: "Free-text description of the metric.",
							Optional:    true,
						},
						"d3format": schema.StringAttribute{
							Description: "D3 format of the values, for example `,.2f`.",
							Optional:    true,
						},
						"warning_text": schema.StringAttribute{
							Description: "Warning shown next to the metric.",
							Optional:    true,
						},
						"metric_type": schema.StringAttribute{
							Description: "Type of the aggregate, for example `sum` or `count`; informational.",
							Optional:    true,
						},
					},
				},
			},
			"columns": schema.SetNestedAttribute{
				Description: "Columns of the dataset. When unset, the columns are left to Superset, which detects them from the source. When set, the dataset has exactly these columns, and columns not listed here are deleted. Within a column, only the attributes that are set are written, so the others keep the value Superset has, such as the detected `type`; an empty string clears an attribute.",
				Optional:    true,
				Computed:    true,
				PlanModifiers: []planmodifier.Set{
					setplanmodifier.UseStateForUnknown(),
				},
				NestedObject: schema.NestedAttributeObject{
					Attributes: map[string]schema.Attribute{
						"column_name": schema.StringAttribute{
							Description: "Name of the column, unique within the dataset.",
							Required:    true,
						},
						"type": schema.StringAttribute{
							Description: "Data type of the column, for example `NUMERIC`. Superset detects it for the columns of the source; set it for a calculated column.",
							Optional:    true,
						},
						"expression": schema.StringAttribute{
							Description: "SQL expression of a calculated column, for example `amount * 100`.",
							Optional:    true,
						},
						"verbose_name": schema.StringAttribute{
							Description: "Label shown instead of the name.",
							Optional:    true,
						},
						"description": schema.StringAttribute{
							Description: "Free-text description of the column.",
							Optional:    true,
						},
						"python_date_format": schema.StringAttribute{
							Description: "Format of the values of a datetime column stored as text, for example `%Y-%m-%d`, or `epoch_s`.",
							Optional:    true,
						},
						"is_dttm": schema.BoolAttribute{
							Description: "Whether the column is a datetime column, which can be used as the time column of a chart. Superset detects it for the columns of the source.",
							Optional:    true,
						},
						"groupby": schema.BoolAttribute{
							Description: "Whether the column can be used as a dimension. Defaults to `true` in Superset.",
							Optional:    true,
						},
						"filterable": schema.BoolAttribute{
							Description: "Whether the column can be used in a filter. Defaults to `true` in Superset.",
							Optional:    true,
						},
					},
				},
			},
			"perm": schema.StringAttribute{
				Description: "View menu of the `datasource_access` permission of the dataset, `[<database>].[<table>](id:<id>)`, for `superset_role_permissions`.",
				Computed:    true,
			},
			"schema_perm": schema.StringAttribute{
				Description: "View menu of the `schema_access` permission of the schema of the dataset, `[<database>].[<schema>]`; null without a schema.",
				Computed:    true,
			},
			"is_managed_externally": schema.BoolAttribute{
				Description: "Whether the dataset is managed outside Superset, for example by Terraform. Superset then shows a banner on the dataset and disables its editing in the UI. Defaults to `false`.",
				Optional:    true,
				Computed:    true,
				Default:     booldefault.StaticBool(false),
			},
			"external_url": schema.StringAttribute{
				Description: "URL of the place where the dataset is managed, shown as a link in the banner of a dataset that is managed externally. Superset does not return it, so a change made in Superset is not detected.",
				Optional:    true,
			},
			"uuid": schema.StringAttribute{
				Description: "UUID of the dataset, the identifier used in export files. Superset 3.1 does not return it, so it is null there.",
				Computed:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
		},
	}
}

// ModifyPlan fills in the view menus of the permissions of an existing dataset from the planned
// values, so an update of the dataset does not leave them unknown and does not ripple into the
// resources that reference them.
func (r *datasetResource) ModifyPlan(ctx context.Context, req resource.ModifyPlanRequest, resp *resource.ModifyPlanResponse) {
	if req.Plan.Raw.IsNull() || req.State.Raw.IsNull() {
		return
	}

	var plan datasetResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() || plan.ID.IsUnknown() || plan.DatabaseName.IsUnknown() || plan.TableName.IsUnknown() || plan.Schema.IsUnknown() {
		return
	}
	setDatasetPermissions(&plan)
	resp.Diagnostics.Append(resp.Plan.SetAttribute(ctx, path.Root("perm"), plan.Perm)...)
	resp.Diagnostics.Append(resp.Plan.SetAttribute(ctx, path.Root("schema_perm"), plan.SchemaPerm)...)
}

// Create creates the dataset and sets the initial Terraform state. A dataset whose settings
// could not be written is recorded in the state with an error, so Terraform replaces it.
func (r *datasetResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan datasetResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	dataset, diags := r.datasetFromModel(ctx, plan, req.Config)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	id, err := r.client.CreateDataset(dataset)
	if err != nil {
		resp.Diagnostics.AddError("Unable to Create Superset Dataset", err.Error())
		if id != 0 {
			resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("id"), id)...)
		}
		return
	}

	r.readInto(ctx, id, plan, &resp.State, &resp.Diagnostics, "Unable to Read Superset Dataset After Creation")
}

// Read refreshes the Terraform state with the dataset as it is in Superset.
func (r *datasetResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state datasetResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	dataset, err := r.client.GetDataset(state.ID.ValueInt64())
	if errors.Is(err, client.ErrNotFound) {
		tflog.Warn(ctx, "dataset no longer exists in Superset, removing it from the state", map[string]any{"id": state.ID.ValueInt64()})
		resp.State.RemoveResource(ctx)
		return
	}
	if err != nil {
		resp.Diagnostics.AddError("Unable to Read Superset Dataset", err.Error())
		return
	}

	model, diags := datasetModel(ctx, dataset, state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &model)...)
}

// Update replaces the settings of the dataset.
func (r *datasetResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state datasetResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	dataset, diags := r.datasetFromModel(ctx, plan, req.Config)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	if err := r.client.UpdateDataset(state.ID.ValueInt64(), dataset); err != nil {
		resp.Diagnostics.AddError("Unable to Update Superset Dataset", err.Error())
		return
	}

	r.readInto(ctx, state.ID.ValueInt64(), plan, &resp.State, &resp.Diagnostics, "Unable to Read Superset Dataset After Update")
}

// Delete deletes the dataset; a dataset that is already gone is not an error.
func (r *datasetResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state datasetResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	err := r.client.DeleteDataset(state.ID.ValueInt64())
	if err != nil && !errors.Is(err, client.ErrNotFound) {
		resp.Diagnostics.AddError("Unable to Delete Superset Dataset", err.Error())
		return
	}
}

// ImportState imports a dataset by its numeric id.
func (r *datasetResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	id, err := strconv.ParseInt(req.ID, 10, 64)
	if err != nil {
		resp.Diagnostics.AddError("Invalid Import ID", fmt.Sprintf("The import ID %q is not a numeric dataset id.", req.ID))
		return
	}
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("id"), id)...)
}

// Configure adds the provider configured client to the resource.
func (r *datasetResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}

	client, ok := req.ProviderData.(*client.Client)
	if !ok {
		resp.Diagnostics.AddError(
			"Unexpected Resource Configure Type",
			fmt.Sprintf("Expected *client.Client, got: %T. Please report this issue to the provider developers.", req.ProviderData),
		)
		return
	}

	r.client = client
}

// readInto fetches the dataset and stores it in the state after a create or an update, so the
// state carries the values Superset computed, such as the owners or the main datetime column.
// The planned columns tell which of their attributes the configuration sets.
func (r *datasetResource) readInto(ctx context.Context, id int64, prior datasetResourceModel, state stateSetter, diags *diag.Diagnostics, summary string) {
	dataset, err := r.client.GetDataset(id)
	if err != nil {
		diags.AddError(summary, err.Error())
		return
	}

	model, d := datasetModel(ctx, dataset, prior)
	diags.Append(d...)
	if diags.HasError() {
		return
	}
	diags.Append(state.Set(ctx, &model)...)
}

// datasetFromModel builds the dataset of the plan, looking the database up by its name. The
// metrics and the columns are only part of it when the configuration sets them.
func (r *datasetResource) datasetFromModel(ctx context.Context, model datasetResourceModel, config tfsdk.Config) (client.Dataset, diag.Diagnostics) {
	var diags diag.Diagnostics
	databaseID, err := r.client.GetDatabaseIDByName(model.DatabaseName.ValueString())
	if err != nil {
		diags.AddAttributeError(path.Root("database_name"), "Unable to Find the Database of the Dataset", err.Error())
		return client.Dataset{}, diags
	}

	dataset := client.Dataset{
		DatabaseID:           databaseID,
		TableName:            model.TableName.ValueString(),
		Schema:               model.Schema.ValueString(),
		SQL:                  model.SQL.ValueString(),
		Description:          model.Description.ValueString(),
		CacheTimeout:         model.CacheTimeout.ValueInt64Pointer(),
		MainDttmCol:          model.MainDttmCol.ValueString(),
		TemplateParams:       model.TemplateParams.ValueString(),
		Extra:                model.Extra.ValueString(),
		FetchValuesPredicate: model.FetchValuesPredicate.ValueString(),
		FilterSelectEnabled:  model.FilterSelectEnabled.ValueBool(),
		NormalizeColumns:     model.NormalizeColumns.ValueBool(),
		AlwaysFilterMainDttm: model.AlwaysFilterMainDttm.ValueBool(),
		DefaultEndpoint:      model.DefaultEndpoint.ValueString(),
		IsManagedExternally:  model.IsManagedExternally.ValueBool(),
		ExternalURL:          model.ExternalURL.ValueString(),
	}
	if !model.Owners.IsNull() && !model.Owners.IsUnknown() {
		diags.Append(model.Owners.ElementsAs(ctx, &dataset.Owners, false)...)
	}

	var configured types.Set
	diags.Append(config.GetAttribute(ctx, path.Root("metrics"), &configured)...)
	if !configured.IsNull() && !model.Metrics.IsUnknown() {
		var metrics []datasetMetricModel
		diags.Append(model.Metrics.ElementsAs(ctx, &metrics, false)...)
		dataset.Metrics = make([]client.DatasetMetric, 0, len(metrics))
		for _, metric := range metrics {
			dataset.Metrics = append(dataset.Metrics, client.DatasetMetric{
				Name:        metric.Name.ValueString(),
				Expression:  metric.Expression.ValueString(),
				VerboseName: metric.VerboseName.ValueString(),
				Description: metric.Description.ValueString(),
				D3Format:    metric.D3Format.ValueString(),
				WarningText: metric.WarningText.ValueString(),
				MetricType:  metric.MetricType.ValueString(),
			})
		}
	}

	diags.Append(config.GetAttribute(ctx, path.Root("columns"), &configured)...)
	if !configured.IsNull() && !model.Columns.IsUnknown() {
		var columns []datasetColumnModel
		diags.Append(model.Columns.ElementsAs(ctx, &columns, false)...)
		dataset.Columns = make([]client.DatasetColumn, 0, len(columns))
		for _, column := range columns {
			dataset.Columns = append(dataset.Columns, client.DatasetColumn{
				Name:             column.Name.ValueString(),
				Type:             column.Type.ValueStringPointer(),
				Expression:       column.Expression.ValueStringPointer(),
				VerboseName:      column.VerboseName.ValueStringPointer(),
				Description:      column.Description.ValueStringPointer(),
				PythonDateFormat: column.PythonDateFormat.ValueStringPointer(),
				IsDttm:           column.IsDttm.ValueBoolPointer(),
				Groupby:          column.Groupby.ValueBoolPointer(),
				Filterable:       column.Filterable.ValueBoolPointer(),
			})
		}
	}
	return dataset, diags
}

// datasetModel maps a dataset to the state, with the view menus of its permissions. The prior
// model, the plan or the prior state, tells which attributes of a column the configuration
// sets, so the others stay null and the values Superset keeps do not show up as changes; it also
// supplies the cache timeout and the external url, which Superset does not return as configured.
func datasetModel(ctx context.Context, dataset *client.Dataset, prior datasetResourceModel) (datasetResourceModel, diag.Diagnostics) {
	owners, diags := types.SetValueFrom(ctx, types.Int64Type, dataset.Owners)
	metrics := make([]datasetMetricModel, 0, len(dataset.Metrics))
	for _, metric := range dataset.Metrics {
		metrics = append(metrics, datasetMetricModel{
			Name:        types.StringValue(metric.Name),
			Expression:  types.StringValue(metric.Expression),
			VerboseName: optionalString(metric.VerboseName),
			Description: optionalString(metric.Description),
			D3Format:    optionalString(metric.D3Format),
			WarningText: optionalString(metric.WarningText),
			MetricType:  optionalString(metric.MetricType),
		})
	}
	metricsValue, d := types.SetValueFrom(ctx, types.ObjectType{AttrTypes: datasetMetricAttributeTypes}, metrics)
	diags.Append(d...)
	columnsValue, d := datasetColumnsValue(ctx, dataset.Columns, prior.Columns)
	diags.Append(d...)
	model := datasetResourceModel{
		ID:                   types.Int64Value(dataset.ID),
		TableName:            types.StringValue(dataset.TableName),
		DatabaseName:         types.StringValue(dataset.DatabaseName),
		Schema:               optionalString(dataset.Schema),
		SQL:                  optionalString(dataset.SQL),
		Description:          optionalString(dataset.Description),
		Owners:               owners,
		CacheTimeout:         datasetCacheTimeout(dataset.CacheTimeout, prior.CacheTimeout),
		MainDttmCol:          optionalString(dataset.MainDttmCol),
		TemplateParams:       jsonDocument.value(dataset.TemplateParams),
		Extra:                jsonDocument.value(dataset.Extra),
		FetchValuesPredicate: optionalString(dataset.FetchValuesPredicate),
		FilterSelectEnabled:  types.BoolValue(dataset.FilterSelectEnabled),
		NormalizeColumns:     types.BoolValue(dataset.NormalizeColumns),
		AlwaysFilterMainDttm: types.BoolValue(dataset.AlwaysFilterMainDttm),
		DefaultEndpoint:      optionalString(dataset.DefaultEndpoint),
		Metrics:              metricsValue,
		Columns:              columnsValue,
		IsManagedExternally:  types.BoolValue(dataset.IsManagedExternally),
		ExternalURL:          prior.ExternalURL,
		UUID:                 optionalString(dataset.UUID),
	}
	setDatasetPermissions(&model)
	return model, diags
}

// datasetCacheTimeout maps the cache timeout of a dataset to the state. Superset reports 0, its
// default, for a dataset whose cache timeout was never set, so 0 stays null when the
// configuration does not set the attribute.
func datasetCacheTimeout(value *int64, prior types.Int64) types.Int64 {
	if prior.IsNull() && (value == nil || *value == 0) {
		return types.Int64Null()
	}
	return types.Int64PointerValue(value)
}

// datasetColumnsValue maps the columns of a dataset to the state. A column that the prior
// columns know carries only the attributes they set; the other columns, and all of them when
// the prior columns are unknown, as after an import, carry every attribute.
func datasetColumnsValue(ctx context.Context, columns []client.DatasetColumn, prior types.Set) (types.Set, diag.Diagnostics) {
	var diags diag.Diagnostics
	configured := map[string]datasetColumnModel{}
	if !prior.IsNull() && !prior.IsUnknown() {
		var elements []datasetColumnModel
		diags.Append(prior.ElementsAs(ctx, &elements, false)...)
		for _, element := range elements {
			configured[element.Name.ValueString()] = element
		}
	}

	models := make([]datasetColumnModel, 0, len(columns))
	for _, column := range columns {
		model := datasetColumnModel{
			Name:             types.StringValue(column.Name),
			Type:             optionalString(*column.Type),
			Expression:       optionalString(*column.Expression),
			VerboseName:      optionalString(*column.VerboseName),
			Description:      optionalString(*column.Description),
			PythonDateFormat: optionalString(*column.PythonDateFormat),
			IsDttm:           types.BoolValue(*column.IsDttm),
			Groupby:          types.BoolValue(*column.Groupby),
			Filterable:       types.BoolValue(*column.Filterable),
		}
		if element, ok := configured[column.Name]; ok {
			model.Type = configuredString(element.Type, *column.Type)
			model.Expression = configuredString(element.Expression, *column.Expression)
			model.VerboseName = configuredString(element.VerboseName, *column.VerboseName)
			model.Description = configuredString(element.Description, *column.Description)
			model.PythonDateFormat = configuredString(element.PythonDateFormat, *column.PythonDateFormat)
			model.IsDttm = configuredBool(element.IsDttm, *column.IsDttm)
			model.Groupby = configuredBool(element.Groupby, *column.Groupby)
			model.Filterable = configuredBool(element.Filterable, *column.Filterable)
		}
		models = append(models, model)
	}
	value, d := types.SetValueFrom(ctx, types.ObjectType{AttrTypes: datasetColumnAttributeTypes}, models)
	diags.Append(d...)
	return value, diags
}

// configuredString returns the value Superset has when the configuration sets the attribute,
// and null when it does not.
func configuredString(configured types.String, value string) types.String {
	if configured.IsNull() {
		return types.StringNull()
	}
	return types.StringValue(value)
}

func configuredBool(configured types.Bool, value bool) types.Bool {
	if configured.IsNull() {
		return types.BoolNull()
	}
	return types.BoolValue(value)
}

// setDatasetPermissions fills in the view menus of the dataset permissions.
func setDatasetPermissions(model *datasetResourceModel) {
	model.Perm, model.SchemaPerm = datasetPermissions(model.DatabaseName.ValueString(), model.TableName.ValueString(), model.Schema.ValueString(), model.ID.ValueInt64())
}

// datasetPermissions derives the view menus of the dataset permissions the way Superset names
// them: `[database].[table](id:N)` for the dataset and `[database].[schema]` for its schema,
// null without a schema.
func datasetPermissions(database, table, schema string, id int64) (types.String, types.String) {
	perm := types.StringValue(fmt.Sprintf("[%s].[%s](id:%d)", database, table, id))
	if schema == "" {
		return perm, types.StringNull()
	}
	return perm, types.StringValue(fmt.Sprintf("[%s].[%s]", database, schema))
}
