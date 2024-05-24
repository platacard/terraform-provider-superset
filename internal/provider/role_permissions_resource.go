package provider

import (
	"context"
	"fmt"

	"strconv"
	"terraform-provider-superset/internal/client"
	"time"

	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
)

// Ensure the implementation satisfies the expected interfaces.
var (
	_ resource.Resource                = &rolePermissionsResource{}
	_ resource.ResourceWithConfigure   = &rolePermissionsResource{}
	_ resource.ResourceWithImportState = &rolePermissionsResource{}
)

// NewRolePermissionsResource is a helper function to simplify the provider implementation.
func NewRolePermissionsResource() resource.Resource {
	return &rolePermissionsResource{}
}

// rolePermissionsResource is the resource implementation.
type rolePermissionsResource struct {
	client *client.Client
}

// rolePermissionsResourceModel maps the resource schema data.
type rolePermissionsResourceModel struct {
	ID                  types.String              `tfsdk:"id"`
	RoleName            types.String              `tfsdk:"role_name"`
	ResourcePermissions []resourcePermissionModel `tfsdk:"resource_permissions"`
	LastUpdated         types.String              `tfsdk:"last_updated"`
}

type resourcePermissionModel struct {
	ID         types.Int64  `tfsdk:"id"`
	Permission types.String `tfsdk:"permission"`
	ViewMenu   types.String `tfsdk:"view_menu"`
}

// Metadata returns the resource type name.
func (r *rolePermissionsResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_role_permissions"
}

// Schema defines the schema for the resource.
func (r *rolePermissionsResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Manages the permissions associated with a role in Superset.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Description: "The unique identifier for the role permissions resource.",
				Computed:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"last_updated": schema.StringAttribute{
				Description: "The timestamp of the last update to the role permissions.",
				Computed:    true,
			},
			"role_name": schema.StringAttribute{
				Description: "The name of the role to which the permissions are assigned.",
				Required:    true,
			},
			"resource_permissions": schema.ListNestedAttribute{
				Description: "A list of permissions associated with the role.",
				Required:    true,
				NestedObject: schema.NestedAttributeObject{
					Attributes: map[string]schema.Attribute{
						"id": schema.Int64Attribute{
							Description: "The unique identifier of the permission.",
							Computed:    true,
						},
						"permission": schema.StringAttribute{
							Description: "The name of the permission.",
							Required:    true,
						},
						"view_menu": schema.StringAttribute{
							Description: "The name of the view menu associated with the permission.",
							Required:    true,
						},
					},
				},
			},
		},
	}
}

// Create creates the resource and sets the initial Terraform state.
func (r *rolePermissionsResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	tflog.Debug(ctx, "Starting Create method")
	// Retrieve values from plan
	var plan rolePermissionsResourceModel
	diags := req.Plan.Get(ctx, &plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		tflog.Debug(ctx, "Exiting Create due to error in retrieving plan", map[string]interface{}{
			"diagnostics": resp.Diagnostics,
		})
		return
	}

	tflog.Debug(ctx, "Plan obtained", map[string]interface{}{
		"roleName": plan.RoleName.ValueString(),
	})

	// Get the role ID based on role name
	roleID, err := r.client.GetRoleIDByName(plan.RoleName.ValueString())
	if err != nil {
		resp.Diagnostics.AddError(
			"Error finding role",
			fmt.Sprintf("Could not find role '%s': %s", plan.RoleName.ValueString(), err),
		)
		return
	}

	tflog.Debug(ctx, "Role ID obtained", map[string]interface{}{
		"roleID": roleID,
	})

	// Prepare permission IDs from plan using a map to ensure unique IDs
	var resourcePermissions []resourcePermissionModel
	permissionIDs := map[int64]bool{}
	for _, perm := range plan.ResourcePermissions {
		permID, err := r.client.GetPermissionIDByNameAndView(perm.Permission.ValueString(), perm.ViewMenu.ValueString())
		if err != nil {
			resp.Diagnostics.AddError(
				"Error finding permission ID",
				fmt.Sprintf("Could not find permission ID for '%s' and view '%s': %s", perm.Permission.ValueString(), perm.ViewMenu.ValueString(), err),
			)
			return
		}
		permissionIDs[permID] = true
		resourcePermissions = append(resourcePermissions, resourcePermissionModel{
			ID:         types.Int64Value(permID),
			Permission: perm.Permission,
			ViewMenu:   perm.ViewMenu,
		})
	}

	tflog.Debug(ctx, "Permission IDs prepared", map[string]interface{}{
		"permissionIDs": permissionIDs,
	})

	// Convert map to slice for the API call
	var permIDList []int64
	for id := range permissionIDs {
		permIDList = append(permIDList, id)
	}

	tflog.Debug(ctx, "Permission ID list for API call", map[string]interface{}{
		"permIDList": permIDList,
	})

	// Update role permissions using the client
	if err := r.client.UpdateRolePermissions(roleID, permIDList); err != nil {
		resp.Diagnostics.AddError(
			"Error updating role permissions",
			"Failed to update role permissions: "+err.Error(),
		)
		return
	}

	tflog.Debug(ctx, "Role permissions updated")

	// Set the state with the updated data
	// sort.Slice(resourcePermissions, func(i, j int) bool {
	// 	return resourcePermissions[i].ID.ValueInt64() < resourcePermissions[j].ID.ValueInt64()
	// })

	result := rolePermissionsResourceModel{
		ID:                  types.StringValue(fmt.Sprintf("%d", roleID)),
		RoleName:            plan.RoleName,
		ResourcePermissions: resourcePermissions,
		LastUpdated:         types.StringValue(time.Now().Format(time.RFC3339)),
	}

	diags = resp.State.Set(ctx, &result)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		tflog.Debug(ctx, "Exiting Create due to error in setting state", map[string]interface{}{
			"diagnostics": resp.Diagnostics,
		})
		return
	}

	tflog.Debug(ctx, "Create method completed successfully")
}

// Read refreshes the Terraform state with the latest data.
func (r *rolePermissionsResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	tflog.Debug(ctx, "Starting Read method")

	// Get current state
	var state rolePermissionsResourceModel
	diags := req.State.Get(ctx, &state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	tflog.Debug(ctx, "State obtained", map[string]interface{}{
		"roleName": state.RoleName.ValueString(),
	})

	// Get role ID
	roleID, err := r.client.GetRoleIDByName(state.RoleName.ValueString())
	if err != nil {
		resp.Diagnostics.AddError(
			"Error finding role",
			fmt.Sprintf("Could not find role '%s': %s", state.RoleName.ValueString(), err),
		)
		return
	}

	tflog.Debug(ctx, "Role ID obtained", map[string]interface{}{
		"roleID": roleID,
	})

	// Get permissions from Superset
	permissions, err := r.client.GetRolePermissions(roleID)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error reading role permissions",
			fmt.Sprintf("Could not read permissions for role ID %d: %s", roleID, err),
		)
		return
	}

	tflog.Debug(ctx, "Permissions fetched from Superset", map[string]interface{}{
		"permissions": permissions,
	})

	// Map permissions to resource model
	var resourcePermissions []resourcePermissionModel
	for _, perm := range permissions {
		tflog.Debug(ctx, "Processing fetched permission", map[string]interface{}{
			"ID":         perm.ID,
			"Permission": perm.PermissionName,
			"ViewMenu":   perm.ViewMenuName,
		})

		// Create mapped permission
		mappedPermission := resourcePermissionModel{
			ID:         types.Int64Value(perm.ID),
			Permission: types.StringValue(perm.PermissionName),
			ViewMenu:   types.StringValue(perm.ViewMenuName),
		}

		// Verify mapping immediately after setting the values
		tflog.Debug(ctx, "Mapped Permission", map[string]interface{}{
			"ID":         mappedPermission.ID.ValueInt64(),
			"Permission": mappedPermission.Permission.ValueString(),
			"ViewMenu":   mappedPermission.ViewMenu.ValueString(),
		})

		resourcePermissions = append(resourcePermissions, mappedPermission)
	}

	// Debug full content of resourcePermissions by converting to a slice of maps
	var debugResourcePermissions []map[string]interface{}
	for _, rp := range resourcePermissions {
		debugResourcePermissions = append(debugResourcePermissions, map[string]interface{}{
			"ID":         rp.ID.ValueInt64(),
			"Permission": rp.Permission.ValueString(),
			"ViewMenu":   rp.ViewMenu.ValueString(),
		})
	}

	tflog.Debug(ctx, "Full content of resourcePermissions", map[string]interface{}{
		"resourcePermissions": debugResourcePermissions,
	})

	// Verify the final mapped permissions
	// sort.Slice(resourcePermissions, func(i, j int) bool {
	// 	return resourcePermissions[i].ID.ValueInt64() < resourcePermissions[j].ID.ValueInt64()
	// })

	for _, rp := range resourcePermissions {
		tflog.Debug(ctx, "Mapped Permission in List", map[string]interface{}{
			"ID":         rp.ID.ValueInt64(),
			"Permission": rp.Permission.ValueString(),
			"ViewMenu":   rp.ViewMenu.ValueString(),
		})
	}

	tflog.Debug(ctx, "Final Permissions mapped to resource model", map[string]interface{}{
		"resourcePermissions": debugResourcePermissions,
	})

	// Overwrite state with refreshed values
	state.ResourcePermissions = resourcePermissions
	state.LastUpdated = types.StringValue(time.Now().Format(time.RFC3339))

	diags = resp.State.Set(ctx, &state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	tflog.Debug(ctx, "Read method completed successfully")
}

// Update updates the resource and sets the updated Terraform state on success.
func (r *rolePermissionsResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	tflog.Debug(ctx, "Starting Update method")
	// Retrieve values from plan
	var plan rolePermissionsResourceModel
	diags := req.Plan.Get(ctx, &plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		tflog.Debug(ctx, "Exiting Update due to error in retrieving plan", map[string]interface{}{
			"diagnostics": resp.Diagnostics,
		})
		return
	}

	tflog.Debug(ctx, "Plan obtained", map[string]interface{}{
		"roleName": plan.RoleName.ValueString(),
	})

	// Get the role ID based on role name
	roleID, err := r.client.GetRoleIDByName(plan.RoleName.ValueString())
	if err != nil {
		resp.Diagnostics.AddError(
			"Error finding role",
			fmt.Sprintf("Could not find role '%s': %s", plan.RoleName.ValueString(), err),
		)
		return
	}

	tflog.Debug(ctx, "Role ID obtained", map[string]interface{}{
		"roleID": roleID,
	})

	// Prepare permission IDs from plan using a map to ensure unique IDs
	var resourcePermissions []resourcePermissionModel
	permissionIDs := map[int64]bool{}
	for _, perm := range plan.ResourcePermissions {
		permID, err := r.client.GetPermissionIDByNameAndView(perm.Permission.ValueString(), perm.ViewMenu.ValueString())
		if err != nil {
			resp.Diagnostics.AddError(
				"Error finding permission ID",
				fmt.Sprintf("Could not find permission ID for '%s' and view '%s': %s", perm.Permission.ValueString(), perm.ViewMenu.ValueString(), err),
			)
			return
		}
		permissionIDs[permID] = true
		resourcePermissions = append(resourcePermissions, resourcePermissionModel{
			ID:         types.Int64Value(permID),
			Permission: perm.Permission,
			ViewMenu:   perm.ViewMenu,
		})
	}

	tflog.Debug(ctx, "Permission IDs prepared", map[string]interface{}{
		"permissionIDs": permissionIDs,
	})

	// Convert map to slice for the API call
	var permIDList []int64
	for id := range permissionIDs {
		permIDList = append(permIDList, id)
	}

	tflog.Debug(ctx, "Permission ID list for API call", map[string]interface{}{
		"permIDList": permIDList,
	})

	// Update role permissions using the client
	if err := r.client.UpdateRolePermissions(roleID, permIDList); err != nil {
		resp.Diagnostics.AddError(
			"Error updating role permissions",
			"Failed to update role permissions: "+err.Error(),
		)
		return
	}

	tflog.Debug(ctx, "Role permissions updated")

	// Set the state with the updated data
	// sort.Slice(resourcePermissions, func(i, j int) bool {
	// 	return resourcePermissions[i].ID.ValueInt64() < resourcePermissions[j].ID.ValueInt64()
	// })

	result := rolePermissionsResourceModel{
		ID:                  types.StringValue(fmt.Sprintf("%d", roleID)),
		RoleName:            plan.RoleName,
		ResourcePermissions: resourcePermissions,
		LastUpdated:         types.StringValue(time.Now().Format(time.RFC3339)),
	}

	diags = resp.State.Set(ctx, &result)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		tflog.Debug(ctx, "Exiting Update due to error in setting state", map[string]interface{}{
			"diagnostics": resp.Diagnostics,
		})
		return
	}

	tflog.Debug(ctx, "Update method completed successfully")
}

// Delete deletes the resource and removes the Terraform state on success.
func (r *rolePermissionsResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	tflog.Debug(ctx, "Starting Delete method")
	var state rolePermissionsResourceModel
	diags := req.State.Get(ctx, &state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		tflog.Debug(ctx, "Exiting Delete due to error in getting state", map[string]interface{}{
			"diagnostics": resp.Diagnostics,
		})
		return
	}

	tflog.Debug(ctx, "State obtained", map[string]interface{}{
		"roleName": state.RoleName.ValueString(),
	})

	roleID, err := r.client.GetRoleIDByName(state.RoleName.ValueString())
	if err != nil {
		resp.Diagnostics.AddError(
			"Error finding role",
			fmt.Sprintf("Could not find role '%s': %s", state.RoleName.ValueString(), err),
		)
		return
	}

	tflog.Debug(ctx, "Role ID obtained", map[string]interface{}{
		"roleID": roleID,
	})

	err = r.client.ClearRolePermissions(roleID)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error clearing role permissions",
			fmt.Sprintf("Could not clear permissions for role ID %d: %s", roleID, err),
		)
		return
	}

	tflog.Debug(ctx, "Role permissions cleared")

	resp.State.RemoveResource(ctx)
	tflog.Debug(ctx, "Delete method completed successfully")
}

// Configure adds the provider configured client to the resource.
func (r *rolePermissionsResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

// ImportState imports the resource state.
func (r *rolePermissionsResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	tflog.Debug(ctx, "Starting ImportState method", map[string]interface{}{
		"import_id": req.ID,
	})

	// Use the role ID from the import ID
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)

	// Fetch the role name based on the ID
	roleID, err := strconv.ParseInt(req.ID, 10, 64)
	if err != nil {
		resp.Diagnostics.AddError("Error parsing role ID", fmt.Sprintf("Could not parse role ID '%s': %s", req.ID, err))
		return
	}

	role, err := r.client.GetRole(roleID)
	if err != nil {
		resp.Diagnostics.AddError("Error fetching role", fmt.Sprintf("Could not fetch role with ID '%d': %s", roleID, err))
		return
	}

	// Manually set the role name in the state
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("role_name"), role.Name)...)
	if resp.Diagnostics.HasError() {
		return
	}

	tflog.Debug(ctx, "ImportState completed successfully", map[string]interface{}{
		"import_id": req.ID,
		"role_name": role.Name,
	})
}
