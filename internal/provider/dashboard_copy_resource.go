package provider

import (
	"context"
	"errors"
	"fmt"

	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/booldefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/boolplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/int64planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/listplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
	"github.com/platacard/terraform-provider-superset/internal/client"
)

// Ensure the implementation satisfies the expected interfaces.
var (
	_ resource.Resource              = &dashboardCopyResource{}
	_ resource.ResourceWithConfigure = &dashboardCopyResource{}
)

// NewDashboardCopyResource is a helper function to simplify the provider implementation.
func NewDashboardCopyResource() resource.Resource {
	return &dashboardCopyResource{}
}

// dashboardCopyResource manages a dashboard copied from another one.
type dashboardCopyResource struct {
	client *client.Client
}

// dashboardCopyResourceModel maps the resource schema data.
type dashboardCopyResourceModel struct {
	ID                types.Int64  `tfsdk:"id"`
	SourceDashboardID types.Int64  `tfsdk:"source_dashboard_id"`
	Title             types.String `tfsdk:"title"`
	CSS               types.String `tfsdk:"css"`
	JSONMetadata      jsonValue    `tfsdk:"json_metadata"`
	DuplicateSlices   types.Bool   `tfsdk:"duplicate_slices"`
	ChartIDs          types.List   `tfsdk:"chart_ids"`
	UUID              types.String `tfsdk:"uuid"`
}

// Metadata returns the resource type name.
func (r *dashboardCopyResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_dashboard_copy"
}

// Schema defines the schema for the resource.
func (r *dashboardCopyResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Manages a dashboard copied from another one, for example a dashboard per region made from a template. The copy is made once, through the copy endpoint of Superset, with the layout and the settings of the source; afterwards it is a dashboard of its own, whose title, CSS and settings this resource manages, and the source can change without affecting it. The copy shows the same charts as the source, or, with `duplicate_slices`, duplicates of them that belong to the copy. Destroying the resource deletes the copy; duplicated charts stay.",
		Attributes: map[string]schema.Attribute{
			"id": schema.Int64Attribute{
				Description: "Numeric identifier of the copy.",
				Computed:    true,
				PlanModifiers: []planmodifier.Int64{
					int64planmodifier.UseStateForUnknown(),
				},
			},
			"source_dashboard_id": schema.Int64Attribute{
				Description: "Identifier of the dashboard to copy. A change makes a new copy.",
				Required:    true,
				PlanModifiers: []planmodifier.Int64{
					int64planmodifier.RequiresReplace(),
				},
			},
			"title": schema.StringAttribute{
				Description: "Title of the copy.",
				Required:    true,
			},
			"css": schema.StringAttribute{
				Description: "CSS applied to the copy. The CSS of the source is not copied.",
				Optional:    true,
			},
			"json_metadata": schema.StringAttribute{
				Description: "JSON document with the settings of the copy, such as `refresh_frequency` or `color_scheme`, over the settings of the source. Only the keys set here are managed: keys Superset adds or that are changed in the UI keep their value and are not reported as drift. When unset, the settings of the source are kept.",
				Optional:    true,
				Computed:    true,
				CustomType:  jsonPartialDocument,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"duplicate_slices": schema.BoolAttribute{
				Description: "Whether to duplicate the charts of the source, so the copy shows charts of its own that can be changed without touching the source. Defaults to `false`: the copy shows the charts of the source. A change makes a new copy.",
				Optional:    true,
				Computed:    true,
				Default:     booldefault.StaticBool(false),
				PlanModifiers: []planmodifier.Bool{
					boolplanmodifier.RequiresReplace(),
				},
			},
			"chart_ids": schema.ListAttribute{
				Description: "Identifiers of the charts shown on the copy: the charts of the source, or their duplicates.",
				Computed:    true,
				ElementType: types.Int64Type,
				PlanModifiers: []planmodifier.List{
					listplanmodifier.UseStateForUnknown(),
				},
			},
			"uuid": schema.StringAttribute{
				Description: "UUID of the copy, as export bundles and the embedded SDK reference it (null on Superset 3.1, which does not return it).",
				Computed:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
		},
	}
}

// Create copies the source dashboard.
func (r *dashboardCopyResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan dashboardCopyResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	source := plan.SourceDashboardID.ValueInt64()
	id, err := r.client.CopyDashboard(source, plan.Title.ValueString(), plan.CSS.ValueString(), plan.JSONMetadata.ValueString(), plan.DuplicateSlices.ValueBool())
	if err != nil {
		resp.Diagnostics.AddError("Unable to Copy Superset Dashboard", fmt.Sprintf("Could not copy dashboard %d: %s", source, err))
		return
	}
	tflog.Debug(ctx, "Copied dashboard", map[string]any{"source": source, "id": id})

	model, diags := r.model(ctx, id, plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &model)...)
}

// Read refreshes the copy from Superset.
func (r *dashboardCopyResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state dashboardCopyResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	model, diags := r.model(ctx, state.ID.ValueInt64(), state)
	if diags.HasError() && r.gone(state.ID.ValueInt64()) {
		tflog.Warn(ctx, "The copied dashboard no longer exists; it will be made again", map[string]any{"id": state.ID.ValueInt64()})
		resp.State.RemoveResource(ctx)
		return
	}
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &model)...)
}

// Update changes the title, the CSS and the settings of the copy, keeping everything else
// the copy has.
func (r *dashboardCopyResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state dashboardCopyResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	id := state.ID.ValueInt64()
	current, err := r.client.GetDashboard(id)
	if err != nil {
		resp.Diagnostics.AddError("Unable to Update Superset Dashboard Copy", fmt.Sprintf("Could not read dashboard %d: %s", id, err))
		return
	}
	current.Title = plan.Title.ValueString()
	current.CSS = plan.CSS.ValueString()
	current.Metadata = plan.JSONMetadata.ValueString()
	current.Layout = ""
	if err := r.client.UpdateDashboard(id, *current); err != nil {
		resp.Diagnostics.AddError("Unable to Update Superset Dashboard Copy", fmt.Sprintf("Could not update dashboard %d: %s", id, err))
		return
	}

	model, diags := r.model(ctx, id, plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &model)...)
}

// Delete deletes the copy; the duplicated charts, if any, stay.
func (r *dashboardCopyResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state dashboardCopyResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	id := state.ID.ValueInt64()
	if err := r.client.DeleteDashboard(id); err != nil && !errors.Is(err, client.ErrNotFound) {
		resp.Diagnostics.AddError("Unable to Delete Superset Dashboard Copy", fmt.Sprintf("Could not delete dashboard %d: %s", id, err))
	}
}

// model reads the copy and maps it to the state, keeping the source and the duplication
// choice of the prior model, which Superset does not keep.
func (r *dashboardCopyResource) model(ctx context.Context, id int64, prior dashboardCopyResourceModel) (dashboardCopyResourceModel, diag.Diagnostics) {
	var diags diag.Diagnostics
	dashboard, err := r.client.GetDashboard(id)
	if err != nil {
		diags.AddError("Unable to Read Superset Dashboard Copy", fmt.Sprintf("Could not read dashboard %d: %s", id, err))
		return prior, diags
	}
	chartIDs, err := r.client.DashboardChartIDs(id)
	if err != nil {
		diags.AddError("Unable to Read Superset Dashboard Copy", fmt.Sprintf("Could not read the charts of dashboard %d: %s", id, err))
		return prior, diags
	}
	charts, d := idsValue(ctx, chartIDs)
	diags.Append(d...)

	return dashboardCopyResourceModel{
		ID:                types.Int64Value(id),
		SourceDashboardID: prior.SourceDashboardID,
		Title:             types.StringValue(dashboard.Title),
		CSS:               optionalString(dashboard.CSS),
		JSONMetadata:      jsonPartialDocument.value(dashboard.Metadata),
		DuplicateSlices:   prior.DuplicateSlices,
		ChartIDs:          charts,
		UUID:              optionalString(dashboard.UUID),
	}, diags
}

// gone reports whether the copy no longer exists.
func (r *dashboardCopyResource) gone(id int64) bool {
	_, err := r.client.GetDashboard(id)
	return errors.Is(err, client.ErrNotFound)
}

// Configure adds the provider configured client to the resource.
func (r *dashboardCopyResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}

	c, ok := req.ProviderData.(*client.Client)
	if !ok {
		resp.Diagnostics.AddError(
			"Unexpected Resource Configure Type",
			fmt.Sprintf("Expected *client.Client, got: %T. Please report this issue to the provider developers.", req.ProviderData),
		)
		return
	}
	r.client = c
}
