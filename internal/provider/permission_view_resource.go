package provider

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/boolplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/int64planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
	"github.com/platacard/terraform-provider-superset/internal/client"
)

// Ensure the implementation satisfies the expected interfaces.
var (
	_ resource.Resource                = &permissionViewResource{}
	_ resource.ResourceWithConfigure   = &permissionViewResource{}
	_ resource.ResourceWithImportState = &permissionViewResource{}
)

// NewPermissionViewResource is a helper function to simplify the provider implementation.
func NewPermissionViewResource() resource.Resource {
	return &permissionViewResource{}
}

// permissionViewResource manages a permission granted on a view menu.
type permissionViewResource struct {
	client *client.Client
}

// permissionViewResourceModel maps the resource schema data.
type permissionViewResourceModel struct {
	ID              types.Int64  `tfsdk:"id"`
	PermissionName  types.String `tfsdk:"permission_name"`
	ViewMenuName    types.String `tfsdk:"view_menu_name"`
	PermissionID    types.Int64  `tfsdk:"permission_id"`
	ViewMenuID      types.Int64  `tfsdk:"view_menu_id"`
	CreatedViewMenu types.Bool   `tfsdk:"created_view_menu"`
}

// Metadata returns the resource type name.
func (r *permissionViewResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_permission_view"
}

// Schema defines the schema for the resource.
func (r *permissionViewResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Manages a permission granted on a view menu: the pair that `superset_role_permissions` gives to roles, for example `can_read` on a view menu of a custom security manager. Superset creates the pairs of its own views and of its datasets and databases itself; this resource is for the view menus of custom code. The permission must exist, since the API cannot create permissions; the view menu is created when it does not exist and deleted again with the resource, unless other permissions were granted on it meanwhile. A pair that exists already is not taken over: import it. The endpoints belong to the Flask-AppBuilder security API, which Superset only enables with `FAB_ADD_SECURITY_API = True`.",
		Attributes: map[string]schema.Attribute{
			"id": schema.Int64Attribute{
				Description: "Numeric identifier of the pair.",
				Computed:    true,
				PlanModifiers: []planmodifier.Int64{
					int64planmodifier.UseStateForUnknown(),
				},
			},
			"permission_name": schema.StringAttribute{
				Description: "Name of the permission, for example `can_read` or `menu_access`. It must exist.",
				Required:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"view_menu_name": schema.StringAttribute{
				Description: "Name of the view menu. It is created when it does not exist.",
				Required:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"permission_id": schema.Int64Attribute{
				Description: "Numeric identifier of the permission.",
				Computed:    true,
				PlanModifiers: []planmodifier.Int64{
					int64planmodifier.UseStateForUnknown(),
				},
			},
			"view_menu_id": schema.Int64Attribute{
				Description: "Numeric identifier of the view menu.",
				Computed:    true,
				PlanModifiers: []planmodifier.Int64{
					int64planmodifier.UseStateForUnknown(),
				},
			},
			"created_view_menu": schema.BoolAttribute{
				Description: "Whether the resource created the view menu, in which case the destroy deletes it as well, unless other permissions are granted on it by then.",
				Computed:    true,
				PlanModifiers: []planmodifier.Bool{
					boolplanmodifier.UseStateForUnknown(),
				},
			},
		},
	}
}

// Create grants the permission on the view menu, creating the view menu when needed.
func (r *permissionViewResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan permissionViewResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	permission, viewMenu := plan.PermissionName.ValueString(), plan.ViewMenuName.ValueString()
	permissionID, err := r.client.FindPermissionID(permission)
	if err != nil {
		if errors.Is(err, client.ErrNotFound) {
			resp.Diagnostics.AddAttributeError(path.Root("permission_name"), "Unknown Superset Permission", fmt.Sprintf("Permission %q does not exist. Superset creates permissions from its code and from its security manager; the API cannot create them.", permission))
			return
		}
		resp.Diagnostics.AddError("Unable to Find the Superset Permission", fmt.Sprintf("Could not look up permission %q: %s", permission, err))
		return
	}

	created := false
	viewMenuID, err := r.client.FindViewMenuID(viewMenu)
	switch {
	case errors.Is(err, client.ErrNotFound):
		viewMenuID, err = r.client.CreateViewMenu(viewMenu)
		if err != nil {
			resp.Diagnostics.AddError("Unable to Create the Superset View Menu", fmt.Sprintf("Could not create view menu %q: %s", viewMenu, err))
			return
		}
		created = true
		tflog.Debug(ctx, "Created view menu", map[string]any{"name": viewMenu, "id": viewMenuID})
	case err != nil:
		resp.Diagnostics.AddError("Unable to Find the Superset View Menu", fmt.Sprintf("Could not look up view menu %q: %s", viewMenu, err))
		return
	default:
		pairs, err := r.client.ViewMenuPermissionViews(viewMenuID)
		if err != nil {
			resp.Diagnostics.AddError("Unable to Read the Permissions of the Superset View Menu", fmt.Sprintf("Could not read the permissions of view menu %q: %s", viewMenu, err))
			return
		}
		if id, ok := pairs[permission]; ok {
			resp.Diagnostics.AddError("Superset Permission View Already Exists", fmt.Sprintf("Permission %q on view menu %q already exists with id %d: import it instead.", permission, viewMenu, id))
			return
		}
	}

	id, err := r.client.CreatePermissionView(permissionID, viewMenuID, viewMenu)
	if err != nil {
		resp.Diagnostics.AddError("Unable to Create Superset Permission View", fmt.Sprintf("Could not grant permission %q on view menu %q: %s", permission, viewMenu, err))
		return
	}

	plan.ID = types.Int64Value(id)
	plan.PermissionID = types.Int64Value(permissionID)
	plan.ViewMenuID = types.Int64Value(viewMenuID)
	plan.CreatedViewMenu = types.BoolValue(created)
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

// Read refreshes the pair; one that no longer exists is removed from the state.
func (r *permissionViewResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state permissionViewResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	id := state.ID.ValueInt64()
	pair, err := r.client.GetPermissionView(id)
	if err != nil {
		if errors.Is(err, client.ErrNotFound) {
			tflog.Warn(ctx, "The permission view no longer exists; it will be created again", map[string]any{"id": id})
			resp.State.RemoveResource(ctx)
			return
		}
		resp.Diagnostics.AddError("Unable to Read Superset Permission View", fmt.Sprintf("Could not read permission view %d: %s", id, err))
		return
	}

	state.PermissionName = types.StringValue(pair.PermissionName)
	state.ViewMenuName = types.StringValue(pair.ViewMenuName)
	if state.PermissionID.IsNull() || state.ViewMenuID.IsNull() {
		permissionID, err := r.client.FindPermissionID(pair.PermissionName)
		if err != nil {
			resp.Diagnostics.AddError("Unable to Read Superset Permission View", fmt.Sprintf("Could not look up permission %q: %s", pair.PermissionName, err))
			return
		}
		viewMenuID, err := r.client.FindViewMenuID(pair.ViewMenuName)
		if err != nil {
			resp.Diagnostics.AddError("Unable to Read Superset Permission View", fmt.Sprintf("Could not look up view menu %q: %s", pair.ViewMenuName, err))
			return
		}
		state.PermissionID = types.Int64Value(permissionID)
		state.ViewMenuID = types.Int64Value(viewMenuID)
	}
	if state.CreatedViewMenu.IsNull() {
		state.CreatedViewMenu = types.BoolValue(false)
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &state)...)
}

// Update never runs: every configurable attribute requires a replacement.
func (r *permissionViewResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan permissionViewResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

// Delete removes the pair, and the view menu when the resource created it and nothing else
// is granted on it.
func (r *permissionViewResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state permissionViewResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	id, viewMenu := state.ID.ValueInt64(), state.ViewMenuName.ValueString()
	if err := r.client.DeletePermissionView(id, viewMenu); err != nil && !errors.Is(err, client.ErrNotFound) {
		resp.Diagnostics.AddError("Unable to Delete Superset Permission View", fmt.Sprintf("Could not delete permission view %d: %s", id, err))
		return
	}
	if !state.CreatedViewMenu.ValueBool() {
		return
	}

	viewMenuID := state.ViewMenuID.ValueInt64()
	pairs, err := r.client.ViewMenuPermissionViews(viewMenuID)
	if err != nil {
		resp.Diagnostics.AddWarning("Superset View Menu Kept", fmt.Sprintf("Could not check whether view menu %q still carries permissions, so it was kept: %s", viewMenu, err))
		return
	}
	if len(pairs) > 0 {
		tflog.Info(ctx, "The view menu carries other permissions and is kept", map[string]any{"name": viewMenu, "permissions": len(pairs)})
		return
	}
	if err := r.client.DeleteViewMenu(viewMenuID); err != nil && !errors.Is(err, client.ErrNotFound) {
		resp.Diagnostics.AddWarning("Superset View Menu Kept", fmt.Sprintf("Could not delete view menu %q, which the resource had created: %s", viewMenu, err))
	}
}

// ImportState imports a pair by its id; the view menu is then not deleted with the resource.
func (r *permissionViewResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	id, err := strconv.ParseInt(req.ID, 10, 64)
	if err != nil {
		resp.Diagnostics.AddError("Invalid Import ID", fmt.Sprintf("The import ID must be the numeric identifier of the permission view, not %q.", req.ID))
		return
	}
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("id"), id)...)
}

// Configure adds the provider configured client to the resource.
func (r *permissionViewResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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
