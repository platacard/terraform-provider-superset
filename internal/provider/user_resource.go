package provider

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/booldefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/int64planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/listplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
	"terraform-provider-superset/internal/client"
)

// Ensure the implementation satisfies the expected interfaces.
var (
	_ resource.Resource                = &userResource{}
	_ resource.ResourceWithConfigure   = &userResource{}
	_ resource.ResourceWithImportState = &userResource{}
)

// NewUserResource is a helper function to simplify the provider implementation.
func NewUserResource() resource.Resource {
	return &userResource{}
}

// userResource is the resource implementation.
type userResource struct {
	client *client.Client
}

// userResourceModel maps the resource schema data.
type userResourceModel struct {
	ID          types.Int64  `tfsdk:"id"`
	Username    types.String `tfsdk:"username"`
	FirstName   types.String `tfsdk:"first_name"`
	LastName    types.String `tfsdk:"last_name"`
	Email       types.String `tfsdk:"email"`
	Password    types.String `tfsdk:"password"`
	Active      types.Bool   `tfsdk:"active"`
	Roles       types.List   `tfsdk:"roles"`
	LastUpdated types.String `tfsdk:"last_updated"`
}

// Metadata returns the resource type name.
func (r *userResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_user"
}

// Schema defines the schema for the resource.
func (r *userResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Manages a user in Superset. Requires the Flask-AppBuilder security API, which Superset enables with `FAB_ADD_SECURITY_API = True` in `superset_config.py`.",
		Attributes: map[string]schema.Attribute{
			"id": schema.Int64Attribute{
				Description: "Numeric identifier of the user.",
				Computed:    true,
				PlanModifiers: []planmodifier.Int64{
					int64planmodifier.UseStateForUnknown(),
				},
			},
			"username": schema.StringAttribute{
				Description: "Username of the user.",
				Required:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"first_name": schema.StringAttribute{
				Description: "First name of the user.",
				Optional:    true,
			},
			"last_name": schema.StringAttribute{
				Description: "Last name of the user.",
				Optional:    true,
			},
			"email": schema.StringAttribute{
				Description: "Email address of the user.",
				Required:    true,
			},
			"password": schema.StringAttribute{
				Description: "Password of the user. Required for creation, optional for updates.",
				Optional:    true,
				Sensitive:   true,
			},
			"active": schema.BoolAttribute{
				Description: "Whether the user is active. Defaults to true.",
				Optional:    true,
				Computed:    true,
				Default:     booldefault.StaticBool(true),
			},
			"roles": schema.ListAttribute{
				Description: "List of role IDs assigned to the user.",
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
func (r *userResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	tflog.Debug(ctx, "Starting Create method")
	var plan userResourceModel
	diags := req.Plan.Get(ctx, &plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		tflog.Debug(ctx, "Exiting Create due to error in retrieving plan", map[string]interface{}{
			"diagnostics": resp.Diagnostics,
		})
		return
	}

	// Validate password is provided for creation
	if plan.Password.IsNull() || plan.Password.ValueString() == "" {
		resp.Diagnostics.AddError(
			"Missing Password",
			"Password is required when creating a new user",
		)
		return
	}

	// Extract roles from plan
	var roles []int64
	diags = plan.Roles.ElementsAs(ctx, &roles, false)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	id, err := r.client.CreateUser(
		plan.Username.ValueString(),
		plan.FirstName.ValueString(),
		plan.LastName.ValueString(),
		plan.Email.ValueString(),
		plan.Password.ValueString(),
		plan.Active.ValueBool(),
		roles,
	)
	if err != nil {
		resp.Diagnostics.AddError(
			"Unable to Create Superset User",
			fmt.Sprintf("CreateUser failed: %s", err.Error()),
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

	tflog.Debug(ctx, fmt.Sprintf("Created user: ID=%d, Username=%s", plan.ID.ValueInt64(), plan.Username.ValueString()))
}

// Read refreshes the Terraform state with the latest data from Superset.
func (r *userResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	tflog.Debug(ctx, "Starting Read method")
	var state userResourceModel
	diags := req.State.Get(ctx, &state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		tflog.Debug(ctx, "Exiting Read due to error in getting state", map[string]interface{}{
			"diagnostics": resp.Diagnostics,
		})
		return
	}

	user, err := r.client.GetUser(state.ID.ValueInt64())
	if err != nil {
		resp.Diagnostics.AddError(
			"Error reading user",
			fmt.Sprintf("Could not read user ID %d: %s", state.ID.ValueInt64(), err.Error()),
		)
		return
	}

	tflog.Debug(ctx, "API returned user", map[string]interface{}{
		"id":       user.ID,
		"username": user.Username,
	})

	// Update state with values from API
	state.Username = types.StringValue(user.Username)
	state.FirstName = types.StringValue(user.FirstName)
	state.LastName = types.StringValue(user.LastName)
	state.Email = types.StringValue(user.Email)
	state.Active = types.BoolValue(user.Active)

	// Superset returns the roles in its own order; keep the order of the state when the set of
	// roles is unchanged, otherwise a reordered list would show up as a change on every plan.
	rolesList, diags := types.ListValueFrom(ctx, types.Int64Type, roleIDsInStateOrder(ctx, state.Roles, user.Roles))
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
	state.Roles = rolesList

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
func (r *userResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	tflog.Debug(ctx, "Starting Update method")
	var plan userResourceModel
	var state userResourceModel

	req.Plan.Get(ctx, &plan)
	req.State.Get(ctx, &state)

	// Extract roles from plan
	var roles []int64
	diags := plan.Roles.ElementsAs(ctx, &roles, false)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	err := r.client.UpdateUser(
		state.ID.ValueInt64(),
		plan.Username.ValueString(),
		plan.FirstName.ValueString(),
		plan.LastName.ValueString(),
		plan.Email.ValueString(),
		plan.Password.ValueString(), // Can be empty string for no password change
		plan.Active.ValueBool(),
		roles,
	)
	if err != nil {
		resp.Diagnostics.AddError("Failed to update user", "Error: "+err.Error())
		return
	}

	plan.ID = state.ID
	plan.LastUpdated = types.StringValue(time.Now().Format(time.RFC3339))

	resp.State.Set(ctx, &plan)
	tflog.Debug(ctx, fmt.Sprintf("Updated user: ID=%d, Username=%s", plan.ID.ValueInt64(), plan.Username.ValueString()))
}

// Delete deletes the resource and removes the Terraform state on success.
func (r *userResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	tflog.Debug(ctx, "Starting Delete method")
	var state userResourceModel
	diags := req.State.Get(ctx, &state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		tflog.Debug(ctx, "Exiting Delete due to error in getting state", map[string]interface{}{
			"diagnostics": resp.Diagnostics,
		})
		return
	}

	err := r.client.DeleteUser(state.ID.ValueInt64())
	if err != nil {
		if err.Error() == "failed to delete user, status code: 404" {
			resp.State.RemoveResource(ctx)
			tflog.Debug(ctx, fmt.Sprintf("User ID %d not found, removing from state", state.ID.ValueInt64()))
			return
		}
		resp.Diagnostics.AddError(
			"Unable to Delete Superset User",
			fmt.Sprintf("DeleteUser failed: %s", err.Error()),
		)
		return
	}

	resp.State.RemoveResource(ctx)
	tflog.Debug(ctx, fmt.Sprintf("Deleted user: ID=%d", state.ID.ValueInt64()))
}

// ImportState imports an existing resource.
func (r *userResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
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
func (r *userResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

// roleIDsInStateOrder returns the role ids reported by Superset in the order they have in the
// state when both hold the same set of roles, and in Superset's order otherwise.
func roleIDsInStateOrder(ctx context.Context, state types.List, reported []int64) []int64 {
	if state.IsNull() || state.IsUnknown() {
		return reported
	}
	var current []int64
	if state.ElementsAs(ctx, &current, false).HasError() || len(current) != len(reported) {
		return reported
	}
	have := make(map[int64]bool, len(reported))
	for _, id := range reported {
		have[id] = true
	}
	for _, id := range current {
		if !have[id] {
			return reported
		}
	}
	return current
}
