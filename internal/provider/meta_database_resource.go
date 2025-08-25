package provider

import (
	"context"
	"fmt"
	"strconv"

	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/boolplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/int64planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringdefault"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
	"terraform-provider-superset/internal/client"
)

// Ensure the implementation satisfies the expected interfaces.
var (
	_ resource.Resource                = &metaDatabaseResource{}
	_ resource.ResourceWithConfigure   = &metaDatabaseResource{}
	_ resource.ResourceWithImportState = &metaDatabaseResource{}
)

// NewMetaDatabaseResource is a helper function to simplify the provider implementation.
func NewMetaDatabaseResource() resource.Resource {
	return &metaDatabaseResource{}
}

// metaDatabaseResource is the resource implementation.
type metaDatabaseResource struct {
	client *client.Client
}

// metaDatabaseResourceModel maps the resource schema data.
type metaDatabaseResourceModel struct {
	ID                  types.Int64  `tfsdk:"id"`
	DatabaseName        types.String `tfsdk:"database_name"`
	SqlalchemyURI       types.String `tfsdk:"sqlalchemy_uri"`
	AllowedDatabases    types.List   `tfsdk:"allowed_databases"`
	ExposeInSqllab      types.Bool   `tfsdk:"expose_in_sqllab"`
	AllowCtas           types.Bool   `tfsdk:"allow_ctas"`
	AllowCvas           types.Bool   `tfsdk:"allow_cvas"`
	AllowDml            types.Bool   `tfsdk:"allow_dml"`
	AllowRunAsync       types.Bool   `tfsdk:"allow_run_async"`
	IsManagedExternally types.Bool   `tfsdk:"is_managed_externally"`
}

// Metadata returns the resource type name.
func (r *metaDatabaseResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_meta_database"
}

// Schema defines the schema for the resource.
func (r *metaDatabaseResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Manages a meta database connection in Superset for cross-database queries.",
		Attributes: map[string]schema.Attribute{
			"id": schema.Int64Attribute{
				Description: "Numeric identifier of the meta database connection.",
				Computed:    true,
				PlanModifiers: []planmodifier.Int64{
					int64planmodifier.UseStateForUnknown(),
				},
			},
			"database_name": schema.StringAttribute{
				Description: "Name of the meta database connection.",
				Required:    true,
			},
			"sqlalchemy_uri": schema.StringAttribute{
				Description: "SQLAlchemy URI for the meta database connection. Defaults to 'superset://' for meta databases.",
				Optional:    true,
				Computed:    true,
				Default:     stringdefault.StaticString("superset://"),
			},
			"allowed_databases": schema.ListAttribute{
				Description: "List of database names that can be accessed through this meta connection.",
				Required:    true,
				ElementType: types.StringType,
			},
			"expose_in_sqllab": schema.BoolAttribute{
				Description: "Whether to expose this connection in SQL Lab.",
				Optional:    true,
				Computed:    true,
				PlanModifiers: []planmodifier.Bool{
					boolplanmodifier.UseStateForUnknown(),
				},
			},
			"allow_ctas": schema.BoolAttribute{
				Description: "Allow CREATE TABLE AS queries.",
				Optional:    true,
				Computed:    true,
				PlanModifiers: []planmodifier.Bool{
					boolplanmodifier.UseStateForUnknown(),
				},
			},
			"allow_cvas": schema.BoolAttribute{
				Description: "Allow CREATE VIEW AS queries.",
				Optional:    true,
				Computed:    true,
				PlanModifiers: []planmodifier.Bool{
					boolplanmodifier.UseStateForUnknown(),
				},
			},
			"allow_dml": schema.BoolAttribute{
				Description: "Allow DML queries (INSERT, UPDATE, DELETE).",
				Optional:    true,
				Computed:    true,
				PlanModifiers: []planmodifier.Bool{
					boolplanmodifier.UseStateForUnknown(),
				},
			},
			"allow_run_async": schema.BoolAttribute{
				Description: "Allow asynchronous query execution.",
				Optional:    true,
				Computed:    true,
				PlanModifiers: []planmodifier.Bool{
					boolplanmodifier.UseStateForUnknown(),
				},
			},
			"is_managed_externally": schema.BoolAttribute{
				Description: "Whether this connection is managed externally.",
				Optional:    true,
				Computed:    true,
				PlanModifiers: []planmodifier.Bool{
					boolplanmodifier.UseStateForUnknown(),
				},
			},
		},
	}
}

// Configure adds the provider configured client to the resource.
func (r *metaDatabaseResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

// Create creates the resource and sets the initial Terraform state.
func (r *metaDatabaseResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	tflog.Debug(ctx, "Starting Create method for meta database")
	var plan metaDatabaseResourceModel
	diags := req.Plan.Get(ctx, &plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Extract allowed databases from plan
	var allowedDBs []string
	diags = plan.AllowedDatabases.ElementsAs(ctx, &allowedDBs, false)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Check if meta database already exists
	existingDB, err := r.client.FindMetaDatabaseByName(plan.DatabaseName.ValueString())
	if err != nil {
		resp.Diagnostics.AddError(
			"Unable to Search for Existing Meta Database",
			fmt.Sprintf("FindMetaDatabaseByName failed: %s", err.Error()),
		)
		return
	}

	var id int64
	var metaDB *client.MetaDatabase

	// Use provided sqlalchemy_uri or default to "superset://"
	sqlalchemyURI := "superset://"
	if !plan.SqlalchemyURI.IsNull() && !plan.SqlalchemyURI.IsUnknown() {
		sqlalchemyURI = plan.SqlalchemyURI.ValueString()
	}

	if existingDB != nil {
		// Found existing meta database - import it into Terraform state
		tflog.Info(ctx, "Found existing meta database, importing into Terraform state", map[string]interface{}{
			"id":   existingDB.ID,
			"name": existingDB.DatabaseName,
		})

		metaDB = &client.MetaDatabase{
			DatabaseName:         plan.DatabaseName.ValueString(),
			Engine:              "superset",
			ConfigurationMethod: "sqlalchemy_form",
			SqlalchemyURI:       sqlalchemyURI,
			ExposeInSqllab:      plan.ExposeInSqllab.ValueBoolPointer() != nil && *plan.ExposeInSqllab.ValueBoolPointer(),
			AllowCtas:           plan.AllowCtas.ValueBoolPointer() != nil && *plan.AllowCtas.ValueBoolPointer(),
			AllowCvas:           plan.AllowCvas.ValueBoolPointer() != nil && *plan.AllowCvas.ValueBoolPointer(),
			AllowDml:            plan.AllowDml.ValueBoolPointer() != nil && *plan.AllowDml.ValueBoolPointer(),
			AllowRunAsync:       plan.AllowRunAsync.ValueBoolPointer() != nil && *plan.AllowRunAsync.ValueBoolPointer(),
			ServerCert:          nil,
			IsManagedExternally: plan.IsManagedExternally.ValueBoolPointer() != nil && *plan.IsManagedExternally.ValueBoolPointer(),
			ExternalURL:         nil,
			AllowedDBs:          allowedDBs,
		}

		// Handle default values if not specified
		if plan.ExposeInSqllab.IsNull() {
			metaDB.ExposeInSqllab = true
		}
		if plan.AllowCtas.IsNull() {
			metaDB.AllowCtas = false
		}
		if plan.AllowCvas.IsNull() {
			metaDB.AllowCvas = false
		}
		if plan.AllowDml.IsNull() {
			metaDB.AllowDml = false
		}
		if plan.AllowRunAsync.IsNull() {
			metaDB.AllowRunAsync = true
		}
		if plan.IsManagedExternally.IsNull() {
			metaDB.IsManagedExternally = false
		}

		err = r.client.UpdateMetaDatabase(existingDB.ID, metaDB)
		if err != nil {
			resp.Diagnostics.AddError(
				"Unable to Update Superset Meta Database",
				fmt.Sprintf("UpdateMetaDatabase failed: %s", err.Error()),
			)
			return
		}
		
		id = existingDB.ID
	} else {
		// Create new meta database
		tflog.Debug(ctx, "Creating new meta database")

		metaDB = &client.MetaDatabase{
			DatabaseName:         plan.DatabaseName.ValueString(),
			Engine:              "superset",
			ConfigurationMethod: "sqlalchemy_form",
			SqlalchemyURI:       sqlalchemyURI,
			ExposeInSqllab:      plan.ExposeInSqllab.ValueBoolPointer() != nil && *plan.ExposeInSqllab.ValueBoolPointer(),
			AllowCtas:           plan.AllowCtas.ValueBoolPointer() != nil && *plan.AllowCtas.ValueBoolPointer(),
			AllowCvas:           plan.AllowCvas.ValueBoolPointer() != nil && *plan.AllowCvas.ValueBoolPointer(),
			AllowDml:            plan.AllowDml.ValueBoolPointer() != nil && *plan.AllowDml.ValueBoolPointer(),
			AllowRunAsync:       plan.AllowRunAsync.ValueBoolPointer() != nil && *plan.AllowRunAsync.ValueBoolPointer(),
			ServerCert:          nil,
			IsManagedExternally: plan.IsManagedExternally.ValueBoolPointer() != nil && *plan.IsManagedExternally.ValueBoolPointer(),
			ExternalURL:         nil,
			AllowedDBs:          allowedDBs,
		}

		// Handle default values if not specified
		if plan.ExposeInSqllab.IsNull() {
			metaDB.ExposeInSqllab = true
		}
		if plan.AllowCtas.IsNull() {
			metaDB.AllowCtas = false
		}
		if plan.AllowCvas.IsNull() {
			metaDB.AllowCvas = false
		}
		if plan.AllowDml.IsNull() {
			metaDB.AllowDml = false
		}
		if plan.AllowRunAsync.IsNull() {
			metaDB.AllowRunAsync = true
		}
		if plan.IsManagedExternally.IsNull() {
			metaDB.IsManagedExternally = false
		}

		id, err = r.client.CreateMetaDatabase(metaDB)
		if err != nil {
			resp.Diagnostics.AddError(
				"Unable to Create Superset Meta Database",
				fmt.Sprintf("CreateMetaDatabase failed: %s", err.Error()),
			)
			return
		}
	}

	plan.ID = types.Int64Value(id)
	plan.SqlalchemyURI = types.StringValue(metaDB.SqlalchemyURI)
	plan.ExposeInSqllab = types.BoolValue(metaDB.ExposeInSqllab)
	plan.AllowCtas = types.BoolValue(metaDB.AllowCtas)
	plan.AllowCvas = types.BoolValue(metaDB.AllowCvas)
	plan.AllowDml = types.BoolValue(metaDB.AllowDml)
	plan.AllowRunAsync = types.BoolValue(metaDB.AllowRunAsync)
	plan.IsManagedExternally = types.BoolValue(metaDB.IsManagedExternally)

	diags = resp.State.Set(ctx, plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	tflog.Debug(ctx, "Finished Create method for meta database", map[string]interface{}{
		"id": id,
	})
}

// Read refreshes the Terraform state with the latest data.
func (r *metaDatabaseResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state metaDatabaseResourceModel
	diags := req.State.Get(ctx, &state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	tflog.Info(ctx, "Reading meta database from state", map[string]interface{}{
		"state_id":   state.ID.ValueInt64(),
		"state_name": state.DatabaseName.ValueString(),
	})

	// First try to get by ID from state
	metaDB, err := r.client.GetMetaDatabase(state.ID.ValueInt64())
	if err != nil {
		tflog.Warn(ctx, "Failed to get meta database by ID, trying by name", map[string]interface{}{
			"error": err.Error(),
			"id":    state.ID.ValueInt64(),
		})
		// Fallback to search by name
		metaDB, err = r.client.FindMetaDatabaseByName(state.DatabaseName.ValueString())
		if err != nil {
			tflog.Error(ctx, "Failed to search for meta database by name", map[string]interface{}{
				"error": err.Error(),
				"name":  state.DatabaseName.ValueString(),
			})
			resp.Diagnostics.AddError(
				"Unable to Read Meta Database",
				fmt.Sprintf("Both GetMetaDatabase by ID and FindMetaDatabaseByName failed: %s", err.Error()),
			)
			return
		}
	}

	if metaDB == nil {
		// Meta database not found - it was deleted outside of Terraform
		tflog.Warn(ctx, "Meta database not found in Superset, removing from state", map[string]interface{}{
			"name": state.DatabaseName.ValueString(),
		})
		resp.State.RemoveResource(ctx)
		return
	}

	tflog.Info(ctx, "Found meta database in Superset", map[string]interface{}{
		"found_id":         metaDB.ID,
		"found_name":       metaDB.DatabaseName,
		"found_sqlalchemy": metaDB.SqlalchemyURI,
		"state_id":         state.ID.ValueInt64(),
	})

	// Update state with current values from Superset
	state.ID = types.Int64Value(metaDB.ID)
	state.DatabaseName = types.StringValue(metaDB.DatabaseName)
	
	// Only update sqlalchemy_uri if API returned a non-empty value
	if metaDB.SqlalchemyURI != "" {
		state.SqlalchemyURI = types.StringValue(metaDB.SqlalchemyURI)
	}
	// If API doesn't return sqlalchemy_uri, keep the existing value in state
	
	state.ExposeInSqllab = types.BoolValue(metaDB.ExposeInSqllab)
	state.AllowCtas = types.BoolValue(metaDB.AllowCtas)
	state.AllowCvas = types.BoolValue(metaDB.AllowCvas)
	state.AllowDml = types.BoolValue(metaDB.AllowDml)
	state.AllowRunAsync = types.BoolValue(metaDB.AllowRunAsync)
	state.IsManagedExternally = types.BoolValue(metaDB.IsManagedExternally)

	// Only update allowed_databases if API returned non-empty list
	if len(metaDB.AllowedDBs) > 0 {
		allowedDBsList, diags := types.ListValueFrom(ctx, types.StringType, metaDB.AllowedDBs)
		resp.Diagnostics.Append(diags...)
		if resp.Diagnostics.HasError() {
			return
		}
		state.AllowedDatabases = allowedDBsList
	}
	// If API doesn't return allowed_databases, keep the existing value in state

	diags = resp.State.Set(ctx, &state)
	resp.Diagnostics.Append(diags...)
}

// Update updates the resource and sets the updated Terraform state on success.
func (r *metaDatabaseResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan metaDatabaseResourceModel
	diags := req.Plan.Get(ctx, &plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Extract allowed databases from plan
	var allowedDBs []string
	diags = plan.AllowedDatabases.ElementsAs(ctx, &allowedDBs, false)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Use provided sqlalchemy_uri or default to "superset://"
	sqlalchemyURI := "superset://"
	if !plan.SqlalchemyURI.IsNull() && !plan.SqlalchemyURI.IsUnknown() {
		sqlalchemyURI = plan.SqlalchemyURI.ValueString()
	}

	metaDB := &client.MetaDatabase{
		DatabaseName:         plan.DatabaseName.ValueString(),
		Engine:              "superset",
		ConfigurationMethod: "sqlalchemy_form",
		SqlalchemyURI:       sqlalchemyURI,
		ExposeInSqllab:      plan.ExposeInSqllab.ValueBool(),
		AllowCtas:           plan.AllowCtas.ValueBool(),
		AllowCvas:           plan.AllowCvas.ValueBool(),
		AllowDml:            plan.AllowDml.ValueBool(),
		AllowRunAsync:       plan.AllowRunAsync.ValueBool(),
		ServerCert:          nil,
		IsManagedExternally: plan.IsManagedExternally.ValueBool(),
		ExternalURL:         nil,
		AllowedDBs:          allowedDBs,
	}

	err := r.client.UpdateMetaDatabase(plan.ID.ValueInt64(), metaDB)
	if err != nil {
		resp.Diagnostics.AddError(
			"Unable to Update Superset Meta Database",
			fmt.Sprintf("UpdateMetaDatabase failed: %s", err.Error()),
		)
		return
	}

	diags = resp.State.Set(ctx, plan)
	resp.Diagnostics.Append(diags...)
}

// Delete deletes the resource and removes the Terraform state on success.
func (r *metaDatabaseResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state metaDatabaseResourceModel
	diags := req.State.Get(ctx, &state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	err := r.client.DeleteMetaDatabase(state.ID.ValueInt64())
	if err != nil {
		resp.Diagnostics.AddError(
			"Unable to Delete Superset Meta Database",
			fmt.Sprintf("DeleteMetaDatabase failed: %s", err.Error()),
		)
		return
	}
}

// ImportState imports the resource into Terraform state.
func (r *metaDatabaseResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	id, err := strconv.ParseInt(req.ID, 10, 64)
	if err != nil {
		resp.Diagnostics.AddError(
			"Unable to Parse Import ID",
			fmt.Sprintf("Import ID should be a numeric ID, got: %s", req.ID),
		)
		return
	}

	// Set the ID first
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("id"), id)...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Fetch the full resource data from Superset
	metaDB, err := r.client.GetMetaDatabase(id)
	if err != nil {
		resp.Diagnostics.AddError(
			"Unable to Read Meta Database During Import",
			fmt.Sprintf("GetMetaDatabase failed: %s", err.Error()),
		)
		return
	}

	if metaDB == nil {
		resp.Diagnostics.AddError(
			"Meta Database Not Found During Import",
			fmt.Sprintf("Meta database with ID %d not found", id),
		)
		return
	}

	tflog.Info(ctx, "ImportState: Retrieved meta database", map[string]interface{}{
		"id":               metaDB.ID,
		"database_name":    metaDB.DatabaseName,
		"sqlalchemy_uri":   metaDB.SqlalchemyURI,
		"allowed_dbs_len":  len(metaDB.AllowedDBs),
		"allowed_dbs":      metaDB.AllowedDBs,
	})

	// Set all attributes
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("database_name"), metaDB.DatabaseName)...)
	
	// Set sqlalchemy_uri with default for meta databases
	if metaDB.SqlalchemyURI == "" {
		resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("sqlalchemy_uri"), "superset://")...)
	} else {
		resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("sqlalchemy_uri"), metaDB.SqlalchemyURI)...)
	}
	
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("expose_in_sqllab"), metaDB.ExposeInSqllab)...)
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("allow_ctas"), metaDB.AllowCtas)...)
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("allow_cvas"), metaDB.AllowCvas)...)
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("allow_dml"), metaDB.AllowDml)...)
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("allow_run_async"), metaDB.AllowRunAsync)...)
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("is_managed_externally"), metaDB.IsManagedExternally)...)
	
	// Set allowed_databases - if API didn't return any, don't set it (leave as null in import)
	// This will cause terraform to see the configured value as needing to be applied
	if len(metaDB.AllowedDBs) > 0 {
		allowedDBsList, diags := types.ListValueFrom(ctx, types.StringType, metaDB.AllowedDBs)
		resp.Diagnostics.Append(diags...)
		if !resp.Diagnostics.HasError() {
			resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("allowed_databases"), allowedDBsList)...)
		}
	}
	// If API doesn't return allowed_databases, don't set it - terraform will handle the difference
}