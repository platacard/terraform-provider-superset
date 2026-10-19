package provider

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/int64planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/listplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
	"terraform-provider-superset/internal/client"
)

// Ensure the implementation satisfies the expected interfaces.
var (
	_ resource.Resource                = &groupResource{}
	_ resource.ResourceWithConfigure   = &groupResource{}
	_ resource.ResourceWithImportState = &groupResource{}
)

// NewGroupResource is a helper function to simplify the provider implementation.
func NewGroupResource() resource.Resource {
	return &groupResource{}
}

// groupResource is the resource implementation.
type groupResource struct {
	client *client.Client
}

// groupResourceModel maps the resource schema data.
type groupResourceModel struct {
	ID          types.Int64  `tfsdk:"id"`
	Name        types.String `tfsdk:"name"`
	Roles       types.List   `tfsdk:"roles"`
	Users       types.List   `tfsdk:"users"`
	LastUpdated types.String `tfsdk:"last_updated"`
}

// Metadata returns the resource type name.
func (r *groupResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_group"
}

// Schema defines the schema for the resource.
func (r *groupResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Manages a group in Superset.",
		Attributes: map[string]schema.Attribute{
			"id": schema.Int64Attribute{
				Description: "Numeric identifier of the group.",
				Computed:    true,
				PlanModifiers: []planmodifier.Int64{
					int64planmodifier.UseStateForUnknown(),
				},
			},
			"name": schema.StringAttribute{
				Description: "Name of the group.",
				Required:    true,
			},
			"roles": schema.ListAttribute{
				Description: "List of role IDs assigned to the group.",
				Required:    true,
				ElementType: types.Int64Type,
				PlanModifiers: []planmodifier.List{
					listplanmodifier.UseStateForUnknown(),
				},
			},
			"users": schema.ListAttribute{
				Description: "List of user IDs assigned to the group.",
				Required:    true,
				ElementType: types.Int64Type,
				PlanModifiers: []planmodifier.List{
					listplanmodifier.UseStateForUnknown(),
				},
			},
			"last_updated": schema.StringAttribute{
				Description: "Timestamp of the last update.",
				Computed:    true,
			},
		},
	}
}

// Create creates the resource and sets the initial Terraform state.
func (r *groupResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	tflog.Debug(ctx, "Starting Create method")
	var plan groupResourceModel
	diags := req.Plan.Get(ctx, &plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	var roles []int64
	diags = plan.Roles.ElementsAs(ctx, &roles, false)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	var users []int64
	diags = plan.Users.ElementsAs(ctx, &users, false)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	id, err := r.client.CreateGroup(plan.Name.ValueString(), roles, users)
	if err != nil {
		resp.Diagnostics.AddError(
			"Unable to Create Superset Group",
			fmt.Sprintf("CreateGroup failed: %s", err.Error()),
		)
		return
	}

	plan.ID = types.Int64Value(id)
	plan.LastUpdated = types.StringValue(time.Now().Format(time.RFC3339))

	diags = resp.State.Set(ctx, &plan)
	resp.Diagnostics.Append(diags...)
	tflog.Debug(ctx, fmt.Sprintf("Created group: ID=%d, Name=%s", plan.ID.ValueInt64(), plan.Name.ValueString()))
}

// Read refreshes the Terraform state with the latest data from Superset.
func (r *groupResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	tflog.Debug(ctx, "Starting Read method")
	var state groupResourceModel
	diags := req.State.Get(ctx, &state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	group, err := r.client.GetGroup(state.ID.ValueInt64())
	if err != nil {
		resp.Diagnostics.AddError(
			"Error reading group",
			fmt.Sprintf("Could not read group ID %d: %s", state.ID.ValueInt64(), err.Error()),
		)
		return
	}

	tflog.Debug(ctx, "API returned group", map[string]interface{}{
		"id":   group.ID,
		"name": group.Name,
	})

	state.Name = types.StringValue(group.Name)

	rolesList, diags := types.ListValueFrom(ctx, types.Int64Type, group.Roles)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
	state.Roles = rolesList

	usersList, diags := types.ListValueFrom(ctx, types.Int64Type, group.Users)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
	state.Users = usersList

	diags = resp.State.Set(ctx, &state)
	resp.Diagnostics.Append(diags...)
}

// Update updates the resource and sets the updated Terraform state on success.
func (r *groupResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	tflog.Debug(ctx, "Starting Update method")
	var plan groupResourceModel
	var state groupResourceModel

	req.Plan.Get(ctx, &plan)
	req.State.Get(ctx, &state)

	var roles []int64
	diags := plan.Roles.ElementsAs(ctx, &roles, false)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	var users []int64
	diags = plan.Users.ElementsAs(ctx, &users, false)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	err := r.client.UpdateGroup(
		state.ID.ValueInt64(),
		plan.Name.ValueString(),
		roles,
		users,
	)
	if err != nil {
		resp.Diagnostics.AddError("Failed to update group", "Error: "+err.Error())
		return
	}

	plan.ID = state.ID
	plan.LastUpdated = types.StringValue(time.Now().Format(time.RFC3339))

	resp.State.Set(ctx, &plan)
	tflog.Debug(ctx, fmt.Sprintf("Updated group: ID=%d, Name=%s", plan.ID.ValueInt64(), plan.Name.ValueString()))
}

// Delete deletes the resource and removes the Terraform state on success.
func (r *groupResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	tflog.Debug(ctx, "Starting Delete method")
	var state groupResourceModel
	diags := req.State.Get(ctx, &state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	err := r.client.DeleteGroup(state.ID.ValueInt64())
	if err != nil {
		if err.Error() == "failed to delete group, status code: 404" {
			resp.State.RemoveResource(ctx)
			tflog.Debug(ctx, fmt.Sprintf("Group ID %d not found, removing from state", state.ID.ValueInt64()))
			return
		}
		resp.Diagnostics.AddError(
			"Unable to Delete Superset Group",
			fmt.Sprintf("DeleteGroup failed: %s", err.Error()),
		)
		return
	}

	resp.State.RemoveResource(ctx)
	tflog.Debug(ctx, fmt.Sprintf("Deleted group: ID=%d", state.ID.ValueInt64()))
}

// ImportState imports an existing resource.
func (r *groupResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	tflog.Debug(ctx, "Starting ImportState method", map[string]interface{}{
		"import_id": req.ID,
	})

	id, err := strconv.ParseInt(req.ID, 10, 64)
	if err != nil {
		resp.Diagnostics.AddError(
			"Invalid Import ID",
			fmt.Sprintf("The provided import ID '%s' is not a valid int64: %s", req.ID, err.Error()),
		)
		return
	}

	resp.State.SetAttribute(ctx, path.Root("id"), id)

	tflog.Debug(ctx, "ImportState completed successfully", map[string]interface{}{
		"import_id": req.ID,
	})
}

// Configure adds the provider configured client to the resource.
func (r *groupResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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
