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
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
	"github.com/platacard/terraform-provider-superset/internal/client"
)

// Ensure the implementation satisfies the expected interfaces.
var (
	_ resource.Resource                = &tagResource{}
	_ resource.ResourceWithConfigure   = &tagResource{}
	_ resource.ResourceWithImportState = &tagResource{}
)

// NewTagResource is a helper function to simplify the provider implementation.
func NewTagResource() resource.Resource {
	return &tagResource{}
}

// tagResource manages a tag.
type tagResource struct {
	client *client.Client
}

// tagResourceModel maps the resource schema data.
type tagResourceModel struct {
	ID          types.Int64  `tfsdk:"id"`
	Name        types.String `tfsdk:"name"`
	Description types.String `tfsdk:"description"`
}

// Metadata returns the resource type name.
func (r *tagResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_tag"
}

// Schema defines the schema for the resource.
func (r *tagResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Manages a tag, a label attached to charts, dashboards and saved queries with the `TAGGING_SYSTEM` feature flag. The objects a tag is attached to are managed by the `tags` of `superset_chart` and `superset_dashboard`; the tag keeps them when it is renamed, and is detached from them when it is destroyed.",
		Attributes: map[string]schema.Attribute{
			"id": schema.Int64Attribute{
				Description: "Numeric identifier of the tag.",
				Computed:    true,
				PlanModifiers: []planmodifier.Int64{
					int64planmodifier.UseStateForUnknown(),
				},
			},
			"name": schema.StringAttribute{
				Description: "Name of the tag, unique in Superset.",
				Required:    true,
			},
			"description": schema.StringAttribute{
				Description: "Free-text description of the tag.",
				Optional:    true,
			},
		},
	}
}

// Create creates the tag and sets the initial Terraform state.
func (r *tagResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan tagResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	id, err := r.client.CreateTag(client.Tag{Name: plan.Name.ValueString(), Description: plan.Description.ValueString()})
	if err != nil {
		resp.Diagnostics.AddError("Unable to Create Superset Tag", err.Error())
		return
	}

	r.readInto(ctx, id, &resp.State, &resp.Diagnostics, "Unable to Read Superset Tag After Creation")
}

// Read refreshes the Terraform state with the tag as it is in Superset.
func (r *tagResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state tagResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	tag, err := r.client.GetTag(state.ID.ValueInt64())
	if errors.Is(err, client.ErrNotFound) {
		tflog.Warn(ctx, "tag no longer exists in Superset, removing it from the state", map[string]any{"id": state.ID.ValueInt64()})
		resp.State.RemoveResource(ctx)
		return
	}
	if err != nil {
		resp.Diagnostics.AddError("Unable to Read Superset Tag", err.Error())
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, tagModel(tag))...)
}

// Update replaces the name and the description of the tag.
func (r *tagResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state tagResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	current := client.Tag{ID: state.ID.ValueInt64(), Name: state.Name.ValueString()}
	tag := client.Tag{Name: plan.Name.ValueString(), Description: plan.Description.ValueString()}
	if err := r.client.UpdateTag(state.ID.ValueInt64(), current, tag); err != nil {
		resp.Diagnostics.AddError("Unable to Update Superset Tag", err.Error())
		return
	}

	r.readInto(ctx, state.ID.ValueInt64(), &resp.State, &resp.Diagnostics, "Unable to Read Superset Tag After Update")
}

// Delete detaches the tag from its objects and deletes it; a tag that is already gone is not an error.
func (r *tagResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state tagResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	err := r.client.DeleteTag(state.ID.ValueInt64(), state.Name.ValueString())
	if err != nil && !errors.Is(err, client.ErrNotFound) {
		resp.Diagnostics.AddError("Unable to Delete Superset Tag", err.Error())
		return
	}
}

// ImportState imports a tag by its numeric id.
func (r *tagResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	id, err := strconv.ParseInt(req.ID, 10, 64)
	if err != nil {
		resp.Diagnostics.AddError("Invalid Import ID", fmt.Sprintf("The import ID %q is not a numeric tag id.", req.ID))
		return
	}
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("id"), id)...)
}

// Configure adds the provider configured client to the resource.
func (r *tagResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

// readInto fetches the tag and stores it in the state after a create or an update.
func (r *tagResource) readInto(ctx context.Context, id int64, state stateSetter, diags *diag.Diagnostics, summary string) {
	tag, err := r.client.GetTag(id)
	if err != nil {
		diags.AddError(summary, err.Error())
		return
	}
	diags.Append(state.Set(ctx, tagModel(tag))...)
}

func tagModel(tag *client.Tag) *tagResourceModel {
	return &tagResourceModel{
		ID:          types.Int64Value(tag.ID),
		Name:        types.StringValue(tag.Name),
		Description: optionalString(tag.Description),
	}
}
