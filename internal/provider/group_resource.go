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
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/setplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/tfsdk"
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

// groupResource manages a group of users of Superset 6.
type groupResource struct {
	client *client.Client
}

// groupResourceModel maps the resource schema data.
type groupResourceModel struct {
	ID          types.Int64  `tfsdk:"id"`
	Name        types.String `tfsdk:"name"`
	Label       types.String `tfsdk:"label"`
	Description types.String `tfsdk:"description"`
	Roles       types.Set    `tfsdk:"roles"`
	Users       types.Set    `tfsdk:"users"`
}

// Metadata returns the resource type name.
func (r *groupResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_group"
}

// Schema defines the schema for the resource.
func (r *groupResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Manages a group of users, on Superset 6: its members get the roles of the group. The group endpoints belong to the Flask-AppBuilder security API, which Superset only enables with `FAB_ADD_SECURITY_API = True`.",
		Attributes: map[string]schema.Attribute{
			"id": schema.Int64Attribute{
				Description: "Numeric identifier of the group.",
				Computed:    true,
				PlanModifiers: []planmodifier.Int64{
					int64planmodifier.UseStateForUnknown(),
				},
			},
			"name": schema.StringAttribute{
				Description: "Name of the group, unique in Superset.",
				Required:    true,
			},
			"label": schema.StringAttribute{
				Description: "Label shown instead of the name.",
				Optional:    true,
			},
			"description": schema.StringAttribute{
				Description: "Free-text description of the group.",
				Optional:    true,
			},
			"roles": schema.SetAttribute{
				Description: "Identifiers of the roles the members of the group get, for example from `superset_role`.",
				ElementType: types.Int64Type,
				Optional:    true,
			},
			"users": schema.SetAttribute{
				Description: "Identifiers of the members of the group, for example from `superset_user`. When unset, the members are left as they are, as made by the `groups` of the users; when set, the group has exactly these members, and `[]` removes them all. Manage a membership from one side only, here or in `superset_user`.",
				ElementType: types.Int64Type,
				Optional:    true,
				Computed:    true,
				PlanModifiers: []planmodifier.Set{
					setplanmodifier.UseStateForUnknown(),
				},
			},
		},
	}
}

// Create creates the group and sets the initial Terraform state.
func (r *groupResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan groupResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	group, diags := groupFromModel(ctx, plan, req.Config)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	id, err := r.client.CreateGroup(group)
	if err != nil {
		resp.Diagnostics.AddError("Unable to Create Superset Group", err.Error())
		return
	}

	r.readInto(ctx, id, plan, &resp.State, &resp.Diagnostics, "Unable to Read Superset Group After Creation")
}

// Read refreshes the Terraform state with the group as it is in Superset.
func (r *groupResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state groupResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	group, err := r.client.GetGroup(state.ID.ValueInt64())
	if errors.Is(err, client.ErrNotFound) {
		tflog.Warn(ctx, "group no longer exists in Superset, removing it from the state", map[string]any{"id": state.ID.ValueInt64()})
		resp.State.RemoveResource(ctx)
		return
	}
	if err != nil {
		resp.Diagnostics.AddError("Unable to Read Superset Group", err.Error())
		return
	}

	model, diags := groupModel(ctx, group, state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &model)...)
}

// Update replaces the attributes and roles of the group, and its members when they are configured.
func (r *groupResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state groupResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	group, diags := groupFromModel(ctx, plan, req.Config)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	if err := r.client.UpdateGroup(state.ID.ValueInt64(), group); err != nil {
		resp.Diagnostics.AddError("Unable to Update Superset Group", err.Error())
		return
	}

	r.readInto(ctx, state.ID.ValueInt64(), plan, &resp.State, &resp.Diagnostics, "Unable to Read Superset Group After Update")
}

// Delete deletes the group; a group that is already gone is not an error.
func (r *groupResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state groupResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	err := r.client.DeleteGroup(state.ID.ValueInt64())
	if err != nil && !errors.Is(err, client.ErrNotFound) {
		resp.Diagnostics.AddError("Unable to Delete Superset Group", err.Error())
		return
	}
}

// ImportState imports a group by its numeric id.
func (r *groupResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	id, err := strconv.ParseInt(req.ID, 10, 64)
	if err != nil {
		resp.Diagnostics.AddError("Invalid Import ID", fmt.Sprintf("The import ID %q is not a numeric group id.", req.ID))
		return
	}
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("id"), id)...)
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

// readInto fetches the group and stores it in the state after a create or an update.
func (r *groupResource) readInto(ctx context.Context, id int64, prior groupResourceModel, state stateSetter, diags *diag.Diagnostics, summary string) {
	group, err := r.client.GetGroup(id)
	if err != nil {
		diags.AddError(summary, err.Error())
		return
	}

	model, d := groupModel(ctx, group, prior)
	diags.Append(d...)
	if diags.HasError() {
		return
	}
	diags.Append(state.Set(ctx, &model)...)
}

// groupFromModel builds the group of the plan. The users are only part of it when the
// configuration sets them: the plan carries the ones of the state otherwise.
func groupFromModel(ctx context.Context, model groupResourceModel, config tfsdk.Config) (client.Group, diag.Diagnostics) {
	group := client.Group{
		Name:        model.Name.ValueString(),
		Label:       model.Label.ValueString(),
		Description: model.Description.ValueString(),
	}
	var diags diag.Diagnostics
	if !model.Roles.IsNull() && !model.Roles.IsUnknown() {
		diags.Append(model.Roles.ElementsAs(ctx, &group.Roles, false)...)
	}
	var configured types.Set
	diags.Append(config.GetAttribute(ctx, path.Root("users"), &configured)...)
	if !configured.IsNull() && !model.Users.IsUnknown() {
		group.Users = []int64{}
		diags.Append(model.Users.ElementsAs(ctx, &group.Users, false)...)
	}
	return group, diags
}

// groupModel maps a group to the state. Empty roles stay null when the prior model has none
// either, so a configuration without them does not drift; the users are always the members
// Superset reports, since the attribute is computed when unset.
func groupModel(ctx context.Context, group *client.Group, prior groupResourceModel) (groupResourceModel, diag.Diagnostics) {
	var diags diag.Diagnostics
	roles := types.SetNull(types.Int64Type)
	if len(group.Roles) > 0 || !prior.Roles.IsNull() {
		var d diag.Diagnostics
		roles, d = types.SetValueFrom(ctx, types.Int64Type, nonNilIDs(group.Roles))
		diags.Append(d...)
	}
	users, d := types.SetValueFrom(ctx, types.Int64Type, nonNilIDs(group.Users))
	diags.Append(d...)
	return groupResourceModel{
		ID:          types.Int64Value(group.ID),
		Name:        types.StringValue(group.Name),
		Label:       optionalString(group.Label),
		Description: optionalString(group.Description),
		Roles:       roles,
		Users:       users,
	}, diags
}
