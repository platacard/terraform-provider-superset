package provider

import (
	"context"
	"errors"
	"fmt"

	"github.com/hashicorp/terraform-plugin-framework/diag"
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
	_ resource.Resource              = &roleMembersResource{}
	_ resource.ResourceWithConfigure = &roleMembersResource{}
)

// NewRoleMembersResource is a helper function to simplify the provider implementation.
func NewRoleMembersResource() resource.Resource {
	return &roleMembersResource{}
}

// roleMembersResource manages the users and the groups of a role, on Superset 6.
type roleMembersResource struct {
	client *client.Client
}

// roleMembersResourceModel maps the resource schema data.
type roleMembersResourceModel struct {
	ID       types.Int64 `tfsdk:"id"`
	RoleID   types.Int64 `tfsdk:"role_id"`
	UserIDs  types.Set   `tfsdk:"user_ids"`
	GroupIDs types.Set   `tfsdk:"group_ids"`
}

// Metadata returns the resource type name.
func (r *roleMembersResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_role_members"
}

// Schema defines the schema for the resource.
func (r *roleMembersResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Manages the members of a role from the side of the role, on Superset 6: the users that have the role and the groups that carry it. Each list that is set is authoritative: Superset makes exactly these users or groups members, so members added elsewhere are removed on the next apply, and an empty list removes them all. A list that is unset is left alone. Destroying the resource removes the members of the lists it managed. Do not manage the same membership through `superset_user.roles` or `superset_group.roles` as well, since each side would undo the other. The role endpoints belong to the Flask-AppBuilder security API, which Superset only enables with `FAB_ADD_SECURITY_API = True`.",
		Attributes: map[string]schema.Attribute{
			"id": schema.Int64Attribute{
				Description: "Identifier of the role, which identifies the resource as well.",
				Computed:    true,
				PlanModifiers: []planmodifier.Int64{
					int64planmodifier.UseStateForUnknown(),
				},
			},
			"role_id": schema.Int64Attribute{
				Description: "Identifier of the role.",
				Required:    true,
				PlanModifiers: []planmodifier.Int64{
					int64planmodifier.RequiresReplace(),
				},
			},
			"user_ids": schema.SetAttribute{
				Description: "Identifiers of the users that have the role; exactly these users when set.",
				Optional:    true,
				ElementType: types.Int64Type,
			},
			"group_ids": schema.SetAttribute{
				Description: "Identifiers of the groups that carry the role; exactly these groups when set.",
				Optional:    true,
				ElementType: types.Int64Type,
			},
		},
	}
}

// Create sets the members of the role.
func (r *roleMembersResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan roleMembersResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	resp.Diagnostics.Append(r.apply(ctx, plan, roleMembersResourceModel{})...)
	if resp.Diagnostics.HasError() {
		return
	}
	model, diags := r.model(ctx, plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &model)...)
}

// Read refreshes the members of the lists the resource manages.
func (r *roleMembersResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state roleMembersResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	if _, err := r.client.GetRole(state.RoleID.ValueInt64()); err != nil {
		if errors.Is(err, client.ErrNotFound) {
			tflog.Warn(ctx, "The role no longer exists; its members will be set again once it does", map[string]any{"role_id": state.RoleID.ValueInt64()})
			resp.State.RemoveResource(ctx)
			return
		}
		resp.Diagnostics.AddError("Unable to Read Superset Role Members", fmt.Sprintf("Could not read role %d: %s", state.RoleID.ValueInt64(), err))
		return
	}
	model, diags := r.model(ctx, state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &model)...)
}

// Update sets the lists that changed; a list that is no longer set is left as it is.
func (r *roleMembersResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state roleMembersResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	resp.Diagnostics.Append(r.apply(ctx, plan, state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	model, diags := r.model(ctx, plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &model)...)
}

// Delete removes the members of the lists the resource managed; a role that is already gone
// has no members to remove.
func (r *roleMembersResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state roleMembersResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	roleID := state.RoleID.ValueInt64()
	if !state.UserIDs.IsNull() {
		if err := r.client.SetRoleUsers(roleID, nil); err != nil && !errors.Is(err, client.ErrNotFound) {
			resp.Diagnostics.AddError("Unable to Remove the Users of the Superset Role", fmt.Sprintf("Could not remove the users of role %d: %s", roleID, err))
		}
	}
	if !state.GroupIDs.IsNull() {
		if err := r.client.SetRoleGroups(roleID, nil); err != nil && !errors.Is(err, client.ErrNotFound) {
			resp.Diagnostics.AddError("Unable to Remove the Groups of the Superset Role", fmt.Sprintf("Could not remove the groups of role %d: %s", roleID, err))
		}
	}
}

// apply sends the lists of the plan that are set and differ from the prior model.
func (r *roleMembersResource) apply(ctx context.Context, plan, prior roleMembersResourceModel) diag.Diagnostics {
	var diags diag.Diagnostics
	roleID := plan.RoleID.ValueInt64()
	if !plan.UserIDs.IsNull() && !plan.UserIDs.Equal(prior.UserIDs) {
		var users []int64
		diags.Append(plan.UserIDs.ElementsAs(ctx, &users, false)...)
		if diags.HasError() {
			return diags
		}
		if err := r.client.SetRoleUsers(roleID, users); err != nil {
			diags.AddError("Unable to Set the Users of the Superset Role", fmt.Sprintf("Could not set the users of role %d: %s", roleID, err))
			return diags
		}
	}
	if !plan.GroupIDs.IsNull() && !plan.GroupIDs.Equal(prior.GroupIDs) {
		var groups []int64
		diags.Append(plan.GroupIDs.ElementsAs(ctx, &groups, false)...)
		if diags.HasError() {
			return diags
		}
		if err := r.client.SetRoleGroups(roleID, groups); err != nil {
			diags.AddError("Unable to Set the Groups of the Superset Role", fmt.Sprintf("Could not set the groups of role %d: %s", roleID, err))
			return diags
		}
	}
	return diags
}

// model reads the members of the lists the given model manages; an unset list stays null.
func (r *roleMembersResource) model(ctx context.Context, managed roleMembersResourceModel) (roleMembersResourceModel, diag.Diagnostics) {
	var diags diag.Diagnostics
	roleID := managed.RoleID.ValueInt64()
	model := roleMembersResourceModel{
		ID:       types.Int64Value(roleID),
		RoleID:   managed.RoleID,
		UserIDs:  types.SetNull(types.Int64Type),
		GroupIDs: types.SetNull(types.Int64Type),
	}
	if !managed.UserIDs.IsNull() {
		users, err := r.client.RoleUserIDs(roleID)
		if err != nil {
			diags.AddError("Unable to Read the Users of the Superset Role", fmt.Sprintf("Could not read the users of role %d: %s", roleID, err))
			return model, diags
		}
		var d diag.Diagnostics
		model.UserIDs, d = types.SetValueFrom(ctx, types.Int64Type, users)
		diags.Append(d...)
	}
	if !managed.GroupIDs.IsNull() {
		groups, err := r.client.RoleGroupIDs(roleID)
		if err != nil {
			diags.AddError("Unable to Read the Groups of the Superset Role", fmt.Sprintf("Could not read the groups of role %d: %s", roleID, err))
			return model, diags
		}
		var d diag.Diagnostics
		model.GroupIDs, d = types.SetValueFrom(ctx, types.Int64Type, groups)
		diags.Append(d...)
	}
	return model, diags
}

// Configure adds the provider configured client to the resource.
func (r *roleMembersResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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
