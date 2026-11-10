package provider

import (
	"context"
	"errors"
	"fmt"

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
	_ resource.Resource              = &databasePermissionsSyncResource{}
	_ resource.ResourceWithConfigure = &databasePermissionsSyncResource{}
)

// NewDatabasePermissionsSyncResource is a helper function to simplify the provider implementation.
func NewDatabasePermissionsSyncResource() resource.Resource {
	return &databasePermissionsSyncResource{}
}

// databasePermissionsSyncResource makes Superset refresh the permissions of a database connection.
type databasePermissionsSyncResource struct {
	client *client.Client
}

// databasePermissionsSyncResourceModel maps the resource schema data.
type databasePermissionsSyncResourceModel struct {
	ID         types.Int64 `tfsdk:"id"`
	DatabaseID types.Int64 `tfsdk:"database_id"`
	Triggers   types.Map   `tfsdk:"triggers"`
}

// Metadata returns the resource type name.
func (r *databasePermissionsSyncResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_database_permissions_sync"
}

// Schema defines the schema for the resource.
func (r *databasePermissionsSyncResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Makes Superset 6 refresh the permissions of the catalogs and schemas of a database connection, as the *Sync permissions* action of the connection does, so roles can be granted `catalog_access` and `schema_access` on what the source gained. The sync runs when the resource is created and again whenever `database_id` or `triggers` change; it is not undone when the resource is destroyed.",
		Attributes: map[string]schema.Attribute{
			"id": schema.Int64Attribute{
				Description: "Numeric identifier of the database connection.",
				Computed:    true,
				PlanModifiers: []planmodifier.Int64{
					int64planmodifier.UseStateForUnknown(),
				},
			},
			"database_id": schema.Int64Attribute{
				Description: "Numeric identifier of the database connection to sync.",
				Required:    true,
				PlanModifiers: []planmodifier.Int64{
					int64planmodifier.RequiresReplace(),
				},
			},
			"triggers": schema.MapAttribute{
				Description: "Arbitrary values whose change runs the sync again, for example the list of schemas of the source.",
				ElementType: types.StringType,
				Optional:    true,
			},
		},
	}
}

// Create runs the sync.
func (r *databasePermissionsSyncResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan databasePermissionsSyncResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	if err := r.client.SyncDatabasePermissions(plan.DatabaseID.ValueInt64()); err != nil {
		resp.Diagnostics.AddError("Unable to Sync the Permissions of the Superset Database", err.Error())
		return
	}

	plan.ID = plan.DatabaseID
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

// Read checks that the database still exists, so the sync runs again for a connection that was recreated.
func (r *databasePermissionsSyncResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state databasePermissionsSyncResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	_, err := r.client.GetDatabaseConnection(state.DatabaseID.ValueInt64())
	if errors.Is(err, client.ErrNotFound) {
		tflog.Warn(ctx, "database no longer exists in Superset, removing its permission sync from the state", map[string]any{"id": state.DatabaseID.ValueInt64()})
		resp.State.RemoveResource(ctx)
		return
	}
	if err != nil {
		resp.Diagnostics.AddError("Unable to Read Superset Database", err.Error())
		return
	}
}

// Update runs the sync again: only a change of triggers updates the resource in place.
func (r *databasePermissionsSyncResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan databasePermissionsSyncResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	if err := r.client.SyncDatabasePermissions(plan.DatabaseID.ValueInt64()); err != nil {
		resp.Diagnostics.AddError("Unable to Sync the Permissions of the Superset Database", err.Error())
		return
	}

	plan.ID = plan.DatabaseID
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

// Delete only forgets the resource: a sync cannot be undone.
func (r *databasePermissionsSyncResource) Delete(_ context.Context, _ resource.DeleteRequest, _ *resource.DeleteResponse) {
}

// Configure adds the provider configured client to the resource.
func (r *databasePermissionsSyncResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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
