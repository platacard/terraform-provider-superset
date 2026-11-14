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
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/int64planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
	"github.com/platacard/terraform-provider-superset/internal/client"
)

// Ensure the implementation satisfies the expected interfaces.
var (
	_ resource.Resource                = &dashboardEmbeddingResource{}
	_ resource.ResourceWithConfigure   = &dashboardEmbeddingResource{}
	_ resource.ResourceWithImportState = &dashboardEmbeddingResource{}
)

// NewDashboardEmbeddingResource is a helper function to simplify the provider implementation.
func NewDashboardEmbeddingResource() resource.Resource {
	return &dashboardEmbeddingResource{}
}

// dashboardEmbeddingResource manages the embedding configuration of a dashboard.
type dashboardEmbeddingResource struct {
	client *client.Client
}

// dashboardEmbeddingResourceModel maps the resource schema data.
type dashboardEmbeddingResourceModel struct {
	ID             types.Int64  `tfsdk:"id"`
	DashboardID    types.Int64  `tfsdk:"dashboard_id"`
	AllowedDomains types.Set    `tfsdk:"allowed_domains"`
	UUID           types.String `tfsdk:"uuid"`
}

// Metadata returns the resource type name.
func (r *dashboardEmbeddingResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_dashboard_embedding"
}

// Schema defines the schema for the resource.
func (r *dashboardEmbeddingResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Makes a dashboard embeddable in other applications with the Superset embedded SDK. Guest tokens for the embedded dashboard require the `EMBEDDED_SUPERSET` feature flag in Superset.",
		Attributes: map[string]schema.Attribute{
			"id": schema.Int64Attribute{
				Description: "Identifier of the embedding configuration, which is the identifier of the dashboard.",
				Computed:    true,
				PlanModifiers: []planmodifier.Int64{
					int64planmodifier.UseStateForUnknown(),
				},
			},
			"dashboard_id": schema.Int64Attribute{
				Description: "Identifier of the dashboard to embed. Changing it replaces the configuration, which gives the embedded dashboard a new uuid.",
				Required:    true,
				PlanModifiers: []planmodifier.Int64{
					int64planmodifier.RequiresReplace(),
				},
			},
			"allowed_domains": schema.SetAttribute{
				Description: "Origins allowed to embed the dashboard, for example `https://app.example.com`. An empty set allows every origin.",
				ElementType: types.StringType,
				Required:    true,
			},
			"uuid": schema.StringAttribute{
				Description: "Identifier under which the embedded SDK loads the dashboard.",
				Computed:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
		},
	}
}

// Create enables the embedding of the dashboard and sets the initial Terraform state.
func (r *dashboardEmbeddingResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan dashboardEmbeddingResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	r.set(ctx, plan, &resp.State, &resp.Diagnostics, "Unable to Enable Superset Dashboard Embedding")
}

// Read refreshes the Terraform state with the embedding configuration as it is in Superset.
func (r *dashboardEmbeddingResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state dashboardEmbeddingResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	embedding, err := r.client.GetDashboardEmbedding(state.DashboardID.ValueInt64())
	if errors.Is(err, client.ErrNotFound) {
		tflog.Warn(ctx, "dashboard embedding no longer exists in Superset, removing it from the state", map[string]any{"dashboard_id": state.DashboardID.ValueInt64()})
		resp.State.RemoveResource(ctx)
		return
	}
	if err != nil {
		resp.Diagnostics.AddError("Unable to Read Superset Dashboard Embedding", err.Error())
		return
	}

	model, diags := dashboardEmbeddingModel(ctx, embedding)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &model)...)
}

// Update replaces the allowed domains.
func (r *dashboardEmbeddingResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan dashboardEmbeddingResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	r.set(ctx, plan, &resp.State, &resp.Diagnostics, "Unable to Update Superset Dashboard Embedding")
}

// Delete disables the embedding; a configuration that is already gone is not an error.
func (r *dashboardEmbeddingResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state dashboardEmbeddingResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	err := r.client.DeleteDashboardEmbedding(state.DashboardID.ValueInt64())
	if err != nil && !errors.Is(err, client.ErrNotFound) {
		resp.Diagnostics.AddError("Unable to Disable Superset Dashboard Embedding", err.Error())
		return
	}
}

// ImportState imports the embedding configuration by the numeric id of its dashboard.
func (r *dashboardEmbeddingResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	id, err := strconv.ParseInt(req.ID, 10, 64)
	if err != nil {
		resp.Diagnostics.AddError("Invalid Import ID", fmt.Sprintf("The import ID %q is not a numeric dashboard id.", req.ID))
		return
	}
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("dashboard_id"), id)...)
}

// Configure adds the provider configured client to the resource.
func (r *dashboardEmbeddingResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

// set writes the planned allowed domains to Superset and stores the configuration it returns.
func (r *dashboardEmbeddingResource) set(ctx context.Context, plan dashboardEmbeddingResourceModel, state stateSetter, diags *diag.Diagnostics, summary string) {
	var domains []string
	diags.Append(plan.AllowedDomains.ElementsAs(ctx, &domains, false)...)
	if diags.HasError() {
		return
	}

	dashboardID := plan.DashboardID.ValueInt64()
	if _, err := r.client.SetDashboardEmbedding(dashboardID, domains); err != nil {
		diags.AddError(summary, err.Error())
		return
	}

	embedding, err := r.client.GetDashboardEmbedding(dashboardID)
	if err != nil {
		diags.AddError("Unable to Read Superset Dashboard Embedding After Writing It", err.Error())
		return
	}

	model, d := dashboardEmbeddingModel(ctx, embedding)
	diags.Append(d...)
	if diags.HasError() {
		return
	}
	diags.Append(state.Set(ctx, &model)...)
}

func dashboardEmbeddingModel(ctx context.Context, embedding *client.DashboardEmbedding) (dashboardEmbeddingResourceModel, diag.Diagnostics) {
	domains := embedding.AllowedDomains
	if domains == nil {
		domains = []string{}
	}
	allowed, diags := types.SetValueFrom(ctx, types.StringType, domains)
	model := dashboardEmbeddingResourceModel{
		ID:             types.Int64Value(embedding.DashboardID),
		DashboardID:    types.Int64Value(embedding.DashboardID),
		AllowedDomains: allowed,
		UUID:           types.StringValue(embedding.UUID),
	}
	return model, diags
}
