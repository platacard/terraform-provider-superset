package provider

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/booldefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/int64planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/setplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringdefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
	"terraform-provider-superset/internal/client"
)

// Ensure the implementation satisfies the expected interfaces.
var (
	_ resource.Resource                = &chartResource{}
	_ resource.ResourceWithConfigure   = &chartResource{}
	_ resource.ResourceWithImportState = &chartResource{}
)

// NewChartResource is a helper function to simplify the provider implementation.
func NewChartResource() resource.Resource {
	return &chartResource{}
}

// chartResource manages a chart.
type chartResource struct {
	client *client.Client
}

// chartResourceModel maps the resource schema data.
type chartResourceModel struct {
	ID                   types.Int64  `tfsdk:"id"`
	Name                 types.String `tfsdk:"name"`
	DatasourceID         types.Int64  `tfsdk:"datasource_id"`
	DatasourceType       types.String `tfsdk:"datasource_type"`
	VizType              types.String `tfsdk:"viz_type"`
	Params               jsonValue    `tfsdk:"params"`
	QueryContext         jsonValue    `tfsdk:"query_context"`
	Description          types.String `tfsdk:"description"`
	CacheTimeout         types.Int64  `tfsdk:"cache_timeout"`
	CertifiedBy          types.String `tfsdk:"certified_by"`
	CertificationDetails types.String `tfsdk:"certification_details"`
	Owners               types.Set    `tfsdk:"owners"`
	Tags                 types.Set    `tfsdk:"tags"`
	IsManagedExternally  types.Bool   `tfsdk:"is_managed_externally"`
	ExternalURL          types.String `tfsdk:"external_url"`
	UUID                 types.String `tfsdk:"uuid"`
}

// Metadata returns the resource type name.
func (r *chartResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_chart"
}

// Schema defines the schema for the resource.
func (r *chartResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Manages a chart. Place a chart on a dashboard through the `position_json` of the dashboard.",
		Attributes: map[string]schema.Attribute{
			"id": schema.Int64Attribute{
				Description: "Numeric identifier of the chart.",
				Computed:    true,
				PlanModifiers: []planmodifier.Int64{
					int64planmodifier.UseStateForUnknown(),
				},
			},
			"name": schema.StringAttribute{
				Description: "Name of the chart.",
				Required:    true,
			},
			"datasource_id": schema.Int64Attribute{
				Description: "Identifier of the dataset the chart is built on.",
				Required:    true,
			},
			"datasource_type": schema.StringAttribute{
				Description: "Type of the datasource: `table` (a dataset, the default), `query`, `saved_query` or `view`.",
				Optional:    true,
				Computed:    true,
				Default:     stringdefault.StaticString("table"),
				Validators:  []validator.String{oneOf{"table", "dataset", "query", "saved_query", "view"}},
			},
			"viz_type": schema.StringAttribute{
				Description: "Visualization type, for example `table` or `echarts_timeseries_line`.",
				Required:    true,
			},
			"params": schema.StringAttribute{
				Description: "JSON document with the form data of the chart, as shown in the chart's `View query` and `Edit chart properties` dialogs. Formatting differences are not reported as drift.",
				Required:    true,
				CustomType:  jsonDocument,
			},
			"query_context": schema.StringAttribute{
				Description: "JSON document with the query context of the chart, which Superset uses for alerts, reports and cache warm-up. When unset, the query context Superset generates on its own is left alone.",
				Optional:    true,
				Computed:    true,
				CustomType:  jsonDocument,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"description": schema.StringAttribute{
				Description: "Free-text description of the chart.",
				Optional:    true,
			},
			"cache_timeout": schema.Int64Attribute{
				Description: "Duration in seconds for which the chart data is cached. Unset to use the timeout of the dataset or database.",
				Optional:    true,
			},
			"certified_by": schema.StringAttribute{
				Description: "Person or group that certified the chart.",
				Optional:    true,
			},
			"certification_details": schema.StringAttribute{
				Description: "Details of the certification.",
				Optional:    true,
			},
			"owners": schema.SetAttribute{
				Description: "Identifiers of the users who own the chart. Defaults to the user the provider authenticates as.",
				ElementType: types.Int64Type,
				Optional:    true,
				Computed:    true,
				PlanModifiers: []planmodifier.Set{
					setplanmodifier.UseStateForUnknown(),
				},
			},
			"tags": schema.SetAttribute{
				Description: "Names of the tags attached to the chart, with the `TAGGING_SYSTEM` feature flag. When unset, the tags are left as they are; when set, the chart has exactly these tags. Tags that do not exist yet are created, and stay when the chart is destroyed.",
				ElementType: types.StringType,
				Optional:    true,
				Computed:    true,
				PlanModifiers: []planmodifier.Set{
					setplanmodifier.UseStateForUnknown(),
				},
			},
			"is_managed_externally": schema.BoolAttribute{
				Description: "Whether the chart is managed outside Superset, for example by Terraform. Superset then shows a banner on the chart and disables its editing in the UI. Defaults to `false`.",
				Optional:    true,
				Computed:    true,
				Default:     booldefault.StaticBool(false),
			},
			"external_url": schema.StringAttribute{
				Description: "URL of the place where the chart is managed, shown as a link in the banner of a chart that is managed externally. Superset does not return it, so a change made in Superset is not detected.",
				Optional:    true,
			},
			"uuid": schema.StringAttribute{
				Description: "UUID of the chart, the identifier used in export files. Superset 3.1 does not return it, so it is null there.",
				Computed:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
		},
	}
}

// Create creates the chart and sets the initial Terraform state.
func (r *chartResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan chartResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	chart, diags := chartFromModel(ctx, plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	id, err := r.client.CreateChart(chart)
	if err != nil {
		resp.Diagnostics.AddError("Unable to Create Superset Chart", err.Error())
		return
	}

	resp.Diagnostics.Append(r.applyTags(ctx, id, plan.Tags, types.SetNull(types.StringType))...)
	r.readInto(ctx, id, plan, &resp.State, &resp.Diagnostics, "Unable to Read Superset Chart After Creation")
}

// Read refreshes the Terraform state with the chart as it is in Superset.
func (r *chartResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state chartResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	chart, err := r.client.GetChart(state.ID.ValueInt64())
	if errors.Is(err, client.ErrNotFound) {
		tflog.Warn(ctx, "chart no longer exists in Superset, removing it from the state", map[string]any{"id": state.ID.ValueInt64()})
		resp.State.RemoveResource(ctx)
		return
	}
	if err != nil {
		resp.Diagnostics.AddError("Unable to Read Superset Chart", err.Error())
		return
	}

	model, diags := chartModel(ctx, chart, state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &model)...)
}

// Update replaces the attributes of the chart.
func (r *chartResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state chartResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	chart, diags := chartFromModel(ctx, plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	if err := r.client.UpdateChart(state.ID.ValueInt64(), chart); err != nil {
		resp.Diagnostics.AddError("Unable to Update Superset Chart", err.Error())
		return
	}

	resp.Diagnostics.Append(r.applyTags(ctx, state.ID.ValueInt64(), plan.Tags, state.Tags)...)

	r.readInto(ctx, state.ID.ValueInt64(), plan, &resp.State, &resp.Diagnostics, "Unable to Read Superset Chart After Update")
}

// Delete deletes the chart; a chart that is already gone is not an error.
func (r *chartResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state chartResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	err := r.client.DeleteChart(state.ID.ValueInt64())
	if err != nil && !errors.Is(err, client.ErrNotFound) {
		resp.Diagnostics.AddError("Unable to Delete Superset Chart", err.Error())
		return
	}
}

// ImportState imports a chart by its numeric id.
func (r *chartResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	id, err := strconv.ParseInt(req.ID, 10, 64)
	if err != nil {
		resp.Diagnostics.AddError("Invalid Import ID", fmt.Sprintf("The import ID %q is not a numeric chart id.", req.ID))
		return
	}
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("id"), id)...)
}

// Configure adds the provider configured client to the resource.
func (r *chartResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

// applyTags attaches and detaches tags so the chart has exactly the planned ones, when the
// configuration sets them. The tags the chart has are read from Superset rather than taken
// from the state, so tags attached elsewhere are detached too.
func (r *chartResource) applyTags(ctx context.Context, id int64, planned, prior types.Set) diag.Diagnostics {
	var diags diag.Diagnostics
	if planned.IsNull() || planned.IsUnknown() || planned.Equal(prior) {
		return diags
	}
	var desired []string
	diags.Append(planned.ElementsAs(ctx, &desired, false)...)
	if diags.HasError() {
		return diags
	}
	chart, err := r.client.GetChart(id)
	if err != nil {
		diags.AddError("Unable to Read the Tags of the Superset Chart", err.Error())
		return diags
	}
	if err := r.client.SetObjectTags(client.ChartObject, id, chart.Tags, desired); err != nil {
		diags.AddError("Unable to Tag the Superset Chart", err.Error())
	}
	return diags
}

// readInto fetches the chart and stores it in the state after a create or an update, so the
// state carries the values Superset computed, such as the default owners. The plan supplies
// the values Superset does not return.
func (r *chartResource) readInto(ctx context.Context, id int64, plan chartResourceModel, state stateSetter, diags *diag.Diagnostics, summary string) {
	chart, err := r.client.GetChart(id)
	if err != nil {
		diags.AddError(summary, err.Error())
		return
	}

	model, d := chartModel(ctx, chart, plan)
	diags.Append(d...)
	if diags.HasError() {
		return
	}
	diags.Append(state.Set(ctx, &model)...)
}

func chartFromModel(ctx context.Context, model chartResourceModel) (client.Chart, diag.Diagnostics) {
	chart := client.Chart{
		Name:                 model.Name.ValueString(),
		VizType:              model.VizType.ValueString(),
		DatasourceID:         model.DatasourceID.ValueInt64(),
		DatasourceType:       model.DatasourceType.ValueString(),
		Params:               model.Params.ValueString(),
		QueryContext:         model.QueryContext.ValueString(),
		Description:          model.Description.ValueString(),
		CacheTimeout:         model.CacheTimeout.ValueInt64Pointer(),
		CertifiedBy:          model.CertifiedBy.ValueString(),
		CertificationDetails: model.CertificationDetails.ValueString(),
		IsManagedExternally:  model.IsManagedExternally.ValueBool(),
		ExternalURL:          model.ExternalURL.ValueString(),
	}

	var diags diag.Diagnostics
	if !model.Owners.IsNull() && !model.Owners.IsUnknown() {
		diags.Append(model.Owners.ElementsAs(ctx, &chart.Owners, false)...)
	}
	return chart, diags
}

// chartModel maps a chart to the state. The external url, which Superset does not return, is
// taken from the prior model.
func chartModel(ctx context.Context, chart *client.Chart, prior chartResourceModel) (chartResourceModel, diag.Diagnostics) {
	owners, diags := types.SetValueFrom(ctx, types.Int64Type, chart.Owners)
	tags, d := types.SetValueFrom(ctx, types.StringType, nonNilStrings(chart.Tags))
	diags.Append(d...)
	model := chartResourceModel{
		ID:                   types.Int64Value(chart.ID),
		Name:                 types.StringValue(chart.Name),
		DatasourceID:         types.Int64Value(chart.DatasourceID),
		DatasourceType:       types.StringValue(chart.DatasourceType),
		VizType:              types.StringValue(chart.VizType),
		Params:               jsonDocument.value(chart.Params),
		QueryContext:         jsonDocument.value(chart.QueryContext),
		Description:          optionalString(chart.Description),
		CacheTimeout:         types.Int64PointerValue(chart.CacheTimeout),
		CertifiedBy:          optionalString(chart.CertifiedBy),
		CertificationDetails: optionalString(chart.CertificationDetails),
		Owners:               owners,
		Tags:                 tags,
		IsManagedExternally:  types.BoolValue(chart.IsManagedExternally),
		ExternalURL:          prior.ExternalURL,
		UUID:                 optionalString(chart.UUID),
	}
	return model, diags
}

// nonNilStrings returns an empty slice instead of nil, so a set is never null.
func nonNilStrings(values []string) []string {
	if values == nil {
		return []string{}
	}
	return values
}
