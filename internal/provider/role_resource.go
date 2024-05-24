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
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
	"terraform-provider-superset/internal/client"
)

// Ensure the implementation satisfies the expected interfaces.
var (
	_ resource.Resource                = &roleResource{}
	_ resource.ResourceWithConfigure   = &roleResource{}
	_ resource.ResourceWithImportState = &roleResource{}
)

// NewRoleResource is a helper function to simplify the provider implementation.
func NewRoleResource() resource.Resource {
	return &roleResource{}
}

// roleResource is the resource implementation.
type roleResource struct {
	client *client.Client
}

// roleResourceModel maps the resource schema data.
type roleResourceModel struct {
	ID          types.Int64  `tfsdk:"id"`
	Name        types.String `tfsdk:"name"`
	LastUpdated types.String `tfsdk:"last_updated"`
}

// Metadata returns the resource type name.
func (r *roleResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_role"
}

// Schema defines the schema for the resource.
func (r *roleResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Manages a role in Superset.",
		Attributes: map[string]schema.Attribute{
			"id": schema.Int64Attribute{
				Description: "Numeric identifier of the role.",
				Computed:    true,
				PlanModifiers: []planmodifier.Int64{
					int64planmodifier.UseStateForUnknown(),
				},
			},
			"name": schema.StringAttribute{
				Description: "Name of the role.",
				Required:    true,
			},
			"last_updated": schema.StringAttribute{
				Description: "Timestamp of the last update.",
				Computed:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
		},
	}
}

// Create creates the resource and sets the initial Terraform state.
func (r *roleResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	tflog.Debug(ctx, "Starting Create method")
	var plan roleResourceModel
	diags := req.Plan.Get(ctx, &plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		tflog.Debug(ctx, "Exiting Create due to error in retrieving plan", map[string]interface{}{
			"diagnostics": resp.Diagnostics,
		})
		return
	}

	id, err := r.client.CreateRole(plan.Name.ValueString())
	if err != nil {
		resp.Diagnostics.AddError(
			"Unable to Create Superset Role",
			fmt.Sprintf("CreateRole failed: %s", err.Error()),
		)
		return
	}

	plan.ID = types.Int64Value(id)
	plan.LastUpdated = types.StringValue(time.Now().Format(time.RFC3339))

	diags = resp.State.Set(ctx, &plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		tflog.Debug(ctx, "Exiting Create due to error in setting state", map[string]interface{}{
			"diagnostics": resp.Diagnostics,
		})
		return
	}

	tflog.Debug(ctx, fmt.Sprintf("Created role: ID=%d, Name=%s", plan.ID.ValueInt64(), plan.Name.ValueString()))
}

// Read refreshes the Terraform state with the latest data from Superset.
func (r *roleResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	tflog.Debug(ctx, "Starting Read method")
	var state roleResourceModel
	diags := req.State.Get(ctx, &state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		tflog.Debug(ctx, "Exiting Read due to error in getting state", map[string]interface{}{
			"diagnostics": resp.Diagnostics,
		})
		return
	}

	role, err := r.client.GetRole(state.ID.ValueInt64())
	if err != nil {
		resp.Diagnostics.AddError(
			"Error reading role",
			fmt.Sprintf("Could not read role ID %d: %s", state.ID.ValueInt64(), err.Error()),
		)
		return
	}

	// Correct logging using structured logging format
	tflog.Debug(ctx, "API returned role", map[string]interface{}{
		"id":   role.ID,
		"name": role.Name,
	})

	if role.Name == "" {
		tflog.Warn(ctx, "Received empty name for role", map[string]interface{}{
			"roleID": role.ID,
		})
	}

	// Assuming role.Name is a string and needs to be converted to types.String
	state.Name = types.StringValue(role.Name)

	// Save updated state
	diags = resp.State.Set(ctx, &state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		tflog.Debug(ctx, "Exiting Read due to error in setting state", map[string]interface{}{
			"diagnostics": resp.Diagnostics,
		})
		return
	}
}

// Update updates the resource and sets the updated Terraform state on success.
func (r *roleResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	tflog.Debug(ctx, "Starting Update method")
	var plan roleResourceModel
	var state roleResourceModel

	req.Plan.Get(ctx, &plan)
	req.State.Get(ctx, &state)

	if plan.Name != state.Name {
		// Only update if there is a real change
		err := r.client.UpdateRole(state.ID.ValueInt64(), plan.Name.ValueString())
		if err != nil {
			resp.Diagnostics.AddError("Failed to update role", "Error: "+err.Error())
			return
		}
		state.Name = plan.Name
		state.LastUpdated = types.StringValue(time.Now().Format(time.RFC3339))
	}

	resp.State.Set(ctx, &state)
	tflog.Debug(ctx, fmt.Sprintf("Updated role: ID=%d, Name=%s", state.ID.ValueInt64(), state.Name.ValueString()))
}

// Delete deletes the resource and removes the Terraform state on success.
func (r *roleResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	tflog.Debug(ctx, "Starting Delete method")
	var state roleResourceModel
	diags := req.State.Get(ctx, &state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		tflog.Debug(ctx, "Exiting Delete due to error in getting state", map[string]interface{}{
			"diagnostics": resp.Diagnostics,
		})
		return
	}

	err := r.client.DeleteRole(state.ID.ValueInt64())
	if err != nil {
		if err.Error() == "failed to delete role, status code: 404" {
			resp.State.RemoveResource(ctx)
			tflog.Debug(ctx, fmt.Sprintf("Role ID %d not found, removing from state", state.ID.ValueInt64()))
			return
		}
		resp.Diagnostics.AddError(
			"Unable to Delete Superset Role",
			fmt.Sprintf("DeleteRole failed: %s", err.Error()),
		)
		return
	}

	resp.State.RemoveResource(ctx)
	tflog.Debug(ctx, fmt.Sprintf("Deleted role: ID=%d", state.ID.ValueInt64()))
}

// ImportState imports an existing resource.
func (r *roleResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	tflog.Debug(ctx, "Starting ImportState method", map[string]interface{}{
		"import_id": req.ID,
	})

	// Convert import ID to int64 and set it to the state
	id, err := strconv.ParseInt(req.ID, 10, 64)
	if err != nil {
		resp.Diagnostics.AddError(
			"Invalid Import ID",
			fmt.Sprintf("The provided import ID '%s' is not a valid int64: %s", req.ID, err.Error()),
		)
		return
	}

	// Set the ID in the state and call Read
	resp.State.SetAttribute(ctx, path.Root("id"), id)

	tflog.Debug(ctx, "ImportState completed successfully", map[string]interface{}{
		"import_id": req.ID,
	})
}

// Configure adds the provider configured client to the resource.
func (r *roleResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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
