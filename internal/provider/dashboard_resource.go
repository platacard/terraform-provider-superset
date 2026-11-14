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
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
	"github.com/platacard/terraform-provider-superset/internal/client"
)

// Ensure the implementation satisfies the expected interfaces.
var (
	_ resource.Resource                = &dashboardResource{}
	_ resource.ResourceWithConfigure   = &dashboardResource{}
	_ resource.ResourceWithImportState = &dashboardResource{}
)

// NewDashboardResource is a helper function to simplify the provider implementation.
func NewDashboardResource() resource.Resource {
	return &dashboardResource{}
}

// dashboardResource manages a dashboard.
type dashboardResource struct {
	client *client.Client
}

// dashboardResourceModel maps the resource schema data.
type dashboardResourceModel struct {
	ID                   types.Int64  `tfsdk:"id"`
	Title                types.String `tfsdk:"title"`
	Slug                 types.String `tfsdk:"slug"`
	CSS                  types.String `tfsdk:"css"`
	JSONMetadata         jsonValue    `tfsdk:"json_metadata"`
	PositionJSON         jsonValue    `tfsdk:"position_json"`
	Published            types.Bool   `tfsdk:"published"`
	CertifiedBy          types.String `tfsdk:"certified_by"`
	CertificationDetails types.String `tfsdk:"certification_details"`
	Owners               types.Set    `tfsdk:"owners"`
	Roles                types.Set    `tfsdk:"roles"`
	Tags                 types.Set    `tfsdk:"tags"`
	ThemeID              types.Int64  `tfsdk:"theme_id"`
	IsManagedExternally  types.Bool   `tfsdk:"is_managed_externally"`
	ExternalURL          types.String `tfsdk:"external_url"`
	UUID                 types.String `tfsdk:"uuid"`
}

// stateSetter is the part of a state that stores a model.
type stateSetter interface {
	Set(ctx context.Context, val any) diag.Diagnostics
}

// Metadata returns the resource type name.
func (r *dashboardResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_dashboard"
}

// Schema defines the schema for the resource.
func (r *dashboardResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Manages a dashboard. The charts shown on the dashboard are the charts placed in its `position_json`.",
		Attributes: map[string]schema.Attribute{
			"id": schema.Int64Attribute{
				Description: "Numeric identifier of the dashboard.",
				Computed:    true,
				PlanModifiers: []planmodifier.Int64{
					int64planmodifier.UseStateForUnknown(),
				},
			},
			"title": schema.StringAttribute{
				Description: "Title of the dashboard.",
				Required:    true,
			},
			"slug": schema.StringAttribute{
				Description: "Unique slug used in the URL of the dashboard instead of its id.",
				Optional:    true,
			},
			"css": schema.StringAttribute{
				Description: "CSS applied to the dashboard.",
				Optional:    true,
			},
			"json_metadata": schema.StringAttribute{
				Description: "JSON document with the settings of the dashboard, such as `refresh_frequency`, `color_scheme` or `native_filter_configuration`. Only the keys set here are managed: keys Superset adds or that are changed in the UI keep their value and are not reported as drift. When unset, the settings are left to Superset.",
				Optional:    true,
				Computed:    true,
				CustomType:  jsonPartialDocument,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"position_json": schema.StringAttribute{
				Description: "JSON document with the layout of the dashboard. The charts referenced by its `CHART` elements are linked to the dashboard. Superset adds the `uuid` of each chart to the layout, which is not reported as drift. When unset, the layout is left to Superset.",
				Optional:    true,
				Computed:    true,
				CustomType:  jsonPartialDocument,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"published": schema.BoolAttribute{
				Description: "Whether the dashboard is published; an unpublished dashboard is a draft visible to its owners only. Defaults to `false`.",
				Optional:    true,
				Computed:    true,
				Default:     booldefault.StaticBool(false),
			},
			"certified_by": schema.StringAttribute{
				Description: "Person or group that certified the dashboard.",
				Optional:    true,
			},
			"certification_details": schema.StringAttribute{
				Description: "Details of the certification.",
				Optional:    true,
			},
			"owners": schema.SetAttribute{
				Description: "Identifiers of the users who own the dashboard. Defaults to the user the provider authenticates as.",
				ElementType: types.Int64Type,
				Optional:    true,
				Computed:    true,
				PlanModifiers: []planmodifier.Set{
					setplanmodifier.UseStateForUnknown(),
				},
			},
			"roles": schema.SetAttribute{
				Description: "Identifiers of the roles granted access to the dashboard when dashboard role-based access control is enabled.",
				ElementType: types.Int64Type,
				Optional:    true,
			},
			"theme_id": schema.Int64Attribute{
				Description: "Identifier of the `superset_theme` the dashboard applies, on Superset 6. Unset, the dashboard uses the theme of the instance.",
				Optional:    true,
			},
			"tags": schema.SetAttribute{
				Description: "Names of the tags attached to the dashboard, with the `TAGGING_SYSTEM` feature flag. When unset, the tags are left as they are; when set, the dashboard has exactly these tags. Tags that do not exist yet are created, and stay when the dashboard is destroyed.",
				ElementType: types.StringType,
				Optional:    true,
				Computed:    true,
				PlanModifiers: []planmodifier.Set{
					setplanmodifier.UseStateForUnknown(),
				},
			},
			"is_managed_externally": schema.BoolAttribute{
				Description: "Whether the dashboard is managed outside Superset, for example by Terraform. Superset then shows a banner on the dashboard and disables its editing in the UI. Defaults to `false`.",
				Optional:    true,
				Computed:    true,
				Default:     booldefault.StaticBool(false),
			},
			"external_url": schema.StringAttribute{
				Description: "URL of the place where the dashboard is managed, shown as a link in the banner of a dashboard that is managed externally. Superset does not return it, so a change made in Superset is not detected.",
				Optional:    true,
			},
			"uuid": schema.StringAttribute{
				Description: "UUID of the dashboard, the identifier used in export files and by the embedded SDK. Superset 3.1 does not return it, so it is null there.",
				Computed:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
		},
	}
}

// Create creates the dashboard and sets the initial Terraform state. A dashboard whose charts
// could not be linked is recorded in the state with an error, so Terraform replaces it.
func (r *dashboardResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan dashboardResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	dashboard, diags := dashboardFromModel(ctx, plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	id, err := r.client.CreateDashboard(dashboard)
	if err != nil {
		resp.Diagnostics.AddError("Unable to Create Superset Dashboard", err.Error())
		if id != 0 {
			resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("id"), id)...)
		}
		return
	}

	resp.Diagnostics.Append(r.applyTags(ctx, id, plan.Tags, types.SetNull(types.StringType))...)
	r.readInto(ctx, id, plan, &resp.State, &resp.Diagnostics, "Unable to Read Superset Dashboard After Creation")
}

// Read refreshes the Terraform state with the dashboard as it is in Superset.
func (r *dashboardResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state dashboardResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	dashboard, err := r.client.GetDashboard(state.ID.ValueInt64())
	if errors.Is(err, client.ErrNotFound) {
		tflog.Warn(ctx, "dashboard no longer exists in Superset, removing it from the state", map[string]any{"id": state.ID.ValueInt64()})
		resp.State.RemoveResource(ctx)
		return
	}
	if err != nil {
		resp.Diagnostics.AddError("Unable to Read Superset Dashboard", err.Error())
		return
	}

	model, diags := dashboardModel(ctx, dashboard, state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &model)...)
}

// Update replaces the attributes of the dashboard.
func (r *dashboardResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state dashboardResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	dashboard, diags := dashboardFromModel(ctx, plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
	dashboard.ClearTheme = plan.ThemeID.IsNull() && !state.ThemeID.IsNull()

	if err := r.client.UpdateDashboard(state.ID.ValueInt64(), dashboard); err != nil {
		resp.Diagnostics.AddError("Unable to Update Superset Dashboard", err.Error())
		return
	}

	resp.Diagnostics.Append(r.applyTags(ctx, state.ID.ValueInt64(), plan.Tags, state.Tags)...)

	r.readInto(ctx, state.ID.ValueInt64(), plan, &resp.State, &resp.Diagnostics, "Unable to Read Superset Dashboard After Update")
}

// Delete deletes the dashboard; a dashboard that is already gone is not an error.
func (r *dashboardResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state dashboardResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	err := r.client.DeleteDashboard(state.ID.ValueInt64())
	if err != nil && !errors.Is(err, client.ErrNotFound) {
		resp.Diagnostics.AddError("Unable to Delete Superset Dashboard", err.Error())
		return
	}
}

// ImportState imports a dashboard by its numeric id.
func (r *dashboardResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	id, err := strconv.ParseInt(req.ID, 10, 64)
	if err != nil {
		resp.Diagnostics.AddError("Invalid Import ID", fmt.Sprintf("The import ID %q is not a numeric dashboard id.", req.ID))
		return
	}
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("id"), id)...)
}

// Configure adds the provider configured client to the resource.
func (r *dashboardResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

// applyTags attaches and detaches tags so the dashboard has exactly the planned ones, when
// the configuration sets them.
func (r *dashboardResource) applyTags(ctx context.Context, id int64, planned, prior types.Set) diag.Diagnostics {
	var diags diag.Diagnostics
	if planned.IsNull() || planned.IsUnknown() || planned.Equal(prior) {
		return diags
	}
	var desired []string
	diags.Append(planned.ElementsAs(ctx, &desired, false)...)
	if diags.HasError() {
		return diags
	}
	dashboard, err := r.client.GetDashboard(id)
	if err != nil {
		diags.AddError("Unable to Read the Tags of the Superset Dashboard", err.Error())
		return diags
	}
	if err := r.client.SetObjectTags(client.DashboardObject, id, dashboard.Tags, desired); err != nil {
		diags.AddError("Unable to Tag the Superset Dashboard", err.Error())
	}
	return diags
}

// readInto fetches the dashboard and stores it in the state after a create or an update, so the
// state carries the values Superset computed, such as the default owners or the completed metadata.
func (r *dashboardResource) readInto(ctx context.Context, id int64, plan dashboardResourceModel, state stateSetter, diags *diag.Diagnostics, summary string) {
	dashboard, err := r.client.GetDashboard(id)
	if err != nil {
		diags.AddError(summary, err.Error())
		return
	}

	model, d := dashboardModel(ctx, dashboard, plan)
	diags.Append(d...)
	if diags.HasError() {
		return
	}
	diags.Append(state.Set(ctx, &model)...)
}

func dashboardFromModel(ctx context.Context, model dashboardResourceModel) (client.Dashboard, diag.Diagnostics) {
	dashboard := client.Dashboard{
		Title:                model.Title.ValueString(),
		Slug:                 model.Slug.ValueString(),
		CSS:                  model.CSS.ValueString(),
		Metadata:             model.JSONMetadata.ValueString(),
		Layout:               model.PositionJSON.ValueString(),
		Published:            model.Published.ValueBool(),
		CertifiedBy:          model.CertifiedBy.ValueString(),
		CertificationDetails: model.CertificationDetails.ValueString(),
		ThemeID:              model.ThemeID.ValueInt64Pointer(),
		IsManagedExternally:  model.IsManagedExternally.ValueBool(),
		ExternalURL:          model.ExternalURL.ValueString(),
	}

	var diags diag.Diagnostics
	if !model.Owners.IsNull() && !model.Owners.IsUnknown() {
		diags.Append(model.Owners.ElementsAs(ctx, &dashboard.Owners, false)...)
	}
	if !model.Roles.IsNull() && !model.Roles.IsUnknown() {
		diags.Append(model.Roles.ElementsAs(ctx, &dashboard.Roles, false)...)
	}
	return dashboard, diags
}

// dashboardModel maps a dashboard to the state. Roles stay null when the dashboard has none and
// the prior model has none either, so a configuration without roles does not drift. The external
// url, which Superset does not return, is taken from the prior model.
func dashboardModel(ctx context.Context, dashboard *client.Dashboard, prior dashboardResourceModel) (dashboardResourceModel, diag.Diagnostics) {
	owners, diags := types.SetValueFrom(ctx, types.Int64Type, dashboard.Owners)
	tags, d := types.SetValueFrom(ctx, types.StringType, nonNilStrings(dashboard.Tags))
	diags.Append(d...)
	roles := types.SetNull(types.Int64Type)
	if len(dashboard.Roles) > 0 || !prior.Roles.IsNull() {
		var d diag.Diagnostics
		roles, d = types.SetValueFrom(ctx, types.Int64Type, nonNilIDs(dashboard.Roles))
		diags.Append(d...)
	}

	model := dashboardResourceModel{
		ID:                   types.Int64Value(dashboard.ID),
		Title:                types.StringValue(dashboard.Title),
		Slug:                 optionalString(dashboard.Slug),
		CSS:                  optionalString(dashboard.CSS),
		JSONMetadata:         jsonPartialDocument.value(dashboard.Metadata),
		PositionJSON:         jsonPartialDocument.value(dashboard.Layout),
		Published:            types.BoolValue(dashboard.Published),
		CertifiedBy:          optionalString(dashboard.CertifiedBy),
		CertificationDetails: optionalString(dashboard.CertificationDetails),
		Owners:               owners,
		Roles:                roles,
		Tags:                 tags,
		ThemeID:              types.Int64PointerValue(dashboard.ThemeID),
		IsManagedExternally:  types.BoolValue(dashboard.IsManagedExternally),
		ExternalURL:          prior.ExternalURL,
		UUID:                 optionalString(dashboard.UUID),
	}
	return model, diags
}

func nonNilIDs(values []int64) []int64 {
	if values == nil {
		return []int64{}
	}
	return values
}
