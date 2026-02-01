package provider

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/int64planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
	"terraform-provider-superset/internal/client"
)

// Ensure the implementation satisfies the expected interfaces.
var (
	_ resource.Resource                = &databaseResource{}
	_ resource.ResourceWithConfigure   = &databaseResource{}
	_ resource.ResourceWithImportState = &databaseResource{}
)

// NewDatabaseResource is a helper function to simplify the provider implementation.
func NewDatabaseResource() resource.Resource {
	return &databaseResource{}
}

// databaseResource is the resource implementation.
type databaseResource struct {
	client *client.Client
}

// databaseResourceModel maps the resource schema data.
type databaseResourceModel struct {
	ID             types.Int64  `tfsdk:"id"`
	ConnectionName types.String `tfsdk:"connection_name"`
	DBEngine       types.String `tfsdk:"db_engine"`
	DBUser         types.String `tfsdk:"db_user"`
	DBPass         types.String `tfsdk:"db_pass"`
	DBHost         types.String `tfsdk:"db_host"`
	DBPort         types.Int64  `tfsdk:"db_port"`
	DBName         types.String `tfsdk:"db_name"`
	AllowCTAS      types.Bool   `tfsdk:"allow_ctas"`
	AllowCVAS      types.Bool   `tfsdk:"allow_cvas"`
	AllowDML       types.Bool   `tfsdk:"allow_dml"`
	AllowRunAsync  types.Bool   `tfsdk:"allow_run_async"`
	ExposeInSQLLab types.Bool   `tfsdk:"expose_in_sqllab"`
}

// Metadata returns the resource type name.
func (r *databaseResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_database"
}

// Schema defines the schema for the resource.
func (r *databaseResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Manages a database connection in Superset.",
		Attributes: map[string]schema.Attribute{
			"id": schema.Int64Attribute{
				Description: "Numeric identifier of the database connection.",
				Computed:    true,
				PlanModifiers: []planmodifier.Int64{
					int64planmodifier.UseStateForUnknown(),
				},
			},
			"connection_name": schema.StringAttribute{
				Description: "Name of the database connection.",
				Required:    true,
			},
			"db_engine": schema.StringAttribute{
				Description: "Database engine (e.g., postgresql, mysql).",
				Required:    true,
			},
			"db_user": schema.StringAttribute{
				Description: "Database username.",
				Required:    true,
			},
			"db_pass": schema.StringAttribute{
				Description: "Database password.",
				Required:    true,
				Sensitive:   true,
			},
			"db_host": schema.StringAttribute{
				Description: "Database host.",
				Required:    true,
			},
			"db_port": schema.Int64Attribute{
				Description: "Database port.",
				Required:    true,
			},
			"db_name": schema.StringAttribute{
				Description: "Database name.",
				Required:    true,
			},
			"allow_ctas": schema.BoolAttribute{
				Description: "Allow CTAS.",
				Required:    true,
			},
			"allow_cvas": schema.BoolAttribute{
				Description: "Allow CVAS.",
				Required:    true,
			},
			"allow_dml": schema.BoolAttribute{
				Description: "Allow DML.",
				Required:    true,
			},
			"allow_run_async": schema.BoolAttribute{
				Description: "Allow run async.",
				Required:    true,
			},
			"expose_in_sqllab": schema.BoolAttribute{
				Description: "Expose in SQL Lab.",
				Required:    true,
			},
		},
	}
}

// Create creates the resource and sets the initial Terraform state.
func (r *databaseResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	tflog.Debug(ctx, "Starting Create method")
	var plan databaseResourceModel
	diags := req.Plan.Get(ctx, &plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		tflog.Debug(ctx, "Exiting Create due to error in retrieving plan", map[string]interface{}{
			"diagnostics": resp.Diagnostics,
		})
		return
	}

	sqlalchemyURI := fmt.Sprintf("%s://%s:%s@%s:%d/%s", plan.DBEngine.ValueString(), plan.DBUser.ValueString(), plan.DBPass.ValueString(), plan.DBHost.ValueString(), plan.DBPort.ValueInt64(), plan.DBName.ValueString())
	extra := `{"client_encoding": "utf8"}`
	payload := map[string]interface{}{
		"allow_csv_upload":                  false,
		"allow_ctas":                        plan.AllowCTAS.ValueBool(),
		"allow_cvas":                        plan.AllowCVAS.ValueBool(),
		"allow_dml":                         plan.AllowDML.ValueBool(),
		"allow_multi_schema_metadata_fetch": true,
		"allow_run_async":                   plan.AllowRunAsync.ValueBool(),
		"cache_timeout":                     0,
		"expose_in_sqllab":                  plan.ExposeInSQLLab.ValueBool(),
		"database_name":                     plan.ConnectionName.ValueString(),
		"sqlalchemy_uri":                    sqlalchemyURI,
		"extra":                             extra,
	}

	result, err := r.client.CreateDatabase(payload)
	if err != nil {
		resp.Diagnostics.AddError(
			"Unable to Create Superset Database Connection",
			fmt.Sprintf("CreateDatabase failed: %s", err.Error()),
		)
		return
	}

	// Type assertion with error handling
	idFloat, ok := result["id"].(float64)
	if !ok {
		resp.Diagnostics.AddError(
			"Invalid Response",
			"The 'id' field in the response is not a float64",
		)
		return
	}
	plan.ID = types.Int64Value(int64(idFloat))

	resultData, ok := result["result"].(map[string]interface{})
	if !ok {
		resp.Diagnostics.AddError(
			"Invalid Response",
			"The response from the API does not contain the expected 'result' field",
		)
		return
	}

	// Handle type assertions with error handling
	if val, ok := resultData["database_name"].(string); ok {
		plan.ConnectionName = types.StringValue(val)
	} else {
		resp.Diagnostics.AddError(
			"Invalid Response",
			"The response from the API does not contain a valid 'database_name' field",
		)
		return
	}

	// NOTE: Superset may override these flags based on engine/capabilities.
	// If we copy the API response into state for non-computed attributes,
	// OpenTofu/Terraform will error with "Provider produced inconsistent result after apply".
	// So we keep the planned values in state and only emit a warning if Superset disagrees.
	{
		var diffs []string
		if actual, ok := resultData["allow_ctas"].(bool); ok && actual != plan.AllowCTAS.ValueBool() {
			diffs = append(diffs, fmt.Sprintf("allow_ctas desired=%t actual=%t", plan.AllowCTAS.ValueBool(), actual))
		}
		if actual, ok := resultData["allow_cvas"].(bool); ok && actual != plan.AllowCVAS.ValueBool() {
			diffs = append(diffs, fmt.Sprintf("allow_cvas desired=%t actual=%t", plan.AllowCVAS.ValueBool(), actual))
		}
		if actual, ok := resultData["allow_dml"].(bool); ok && actual != plan.AllowDML.ValueBool() {
			diffs = append(diffs, fmt.Sprintf("allow_dml desired=%t actual=%t", plan.AllowDML.ValueBool(), actual))
		}
		if actual, ok := resultData["allow_run_async"].(bool); ok && actual != plan.AllowRunAsync.ValueBool() {
			diffs = append(diffs, fmt.Sprintf("allow_run_async desired=%t actual=%t", plan.AllowRunAsync.ValueBool(), actual))
		}
		if actual, ok := resultData["expose_in_sqllab"].(bool); ok && actual != plan.ExposeInSQLLab.ValueBool() {
			diffs = append(diffs, fmt.Sprintf("expose_in_sqllab desired=%t actual=%t", plan.ExposeInSQLLab.ValueBool(), actual))
		}
		if len(diffs) > 0 {
			resp.Diagnostics.AddWarning(
				"Superset database flags not honored by API",
				"Superset returned different values than requested: "+strings.Join(diffs, ", ")+". "+
					"This may be an API limitation or engine/capability override; continuing with the planned state to avoid inconsistent apply errors.",
			)
		}
	}

	diags = resp.State.Set(ctx, &plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		tflog.Debug(ctx, "Exiting Create due to error in setting state", map[string]interface{}{
			"diagnostics": resp.Diagnostics,
		})
		return
	}

	tflog.Debug(ctx, fmt.Sprintf("Created database connection: ID=%d, ConnectionName=%s", plan.ID.ValueInt64(), plan.ConnectionName.ValueString()))
}

// Read refreshes the Terraform state with the latest data from Superset.
func (r *databaseResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	tflog.Debug(ctx, "Starting Read method")
	var state databaseResourceModel
	diags := req.State.Get(ctx, &state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		tflog.Debug(ctx, "Exiting Read due to error in getting state", map[string]interface{}{
			"diagnostics": resp.Diagnostics,
		})
		return
	}

	db, err := r.client.GetDatabaseConnectionByID(state.ID.ValueInt64())
	if err != nil {
		resp.Diagnostics.AddError(
			"Error reading database connection",
			fmt.Sprintf("Could not read database ID %d: %s", state.ID.ValueInt64(), err.Error()),
		)
		return
	}

	result, ok := db["result"].(map[string]interface{})
	if !ok {
		resp.Diagnostics.AddError(
			"Invalid Response",
			"The response from the API does not contain the expected 'result' field",
		)
		return
	}

	if val, ok := result["database_name"].(string); ok {
		state.ConnectionName = types.StringValue(val)
	} else {
		resp.Diagnostics.AddError(
			"Invalid Response",
			"The response from the API does not contain a valid 'database_name' field",
		)
		return
	}

	// Do not overwrite these fields from the API to avoid "inconsistent result after apply"
	// when Superset overrides user-configured values. Instead, warn if there's a mismatch.
	{
		var diffs []string
		if actual, ok := result["allow_ctas"].(bool); ok && actual != state.AllowCTAS.ValueBool() {
			diffs = append(diffs, fmt.Sprintf("allow_ctas desired=%t actual=%t", state.AllowCTAS.ValueBool(), actual))
		}
		if actual, ok := result["allow_cvas"].(bool); ok && actual != state.AllowCVAS.ValueBool() {
			diffs = append(diffs, fmt.Sprintf("allow_cvas desired=%t actual=%t", state.AllowCVAS.ValueBool(), actual))
		}
		if actual, ok := result["allow_dml"].(bool); ok && actual != state.AllowDML.ValueBool() {
			diffs = append(diffs, fmt.Sprintf("allow_dml desired=%t actual=%t", state.AllowDML.ValueBool(), actual))
		}
		if actual, ok := result["allow_run_async"].(bool); ok && actual != state.AllowRunAsync.ValueBool() {
			diffs = append(diffs, fmt.Sprintf("allow_run_async desired=%t actual=%t", state.AllowRunAsync.ValueBool(), actual))
		}
		if actual, ok := result["expose_in_sqllab"].(bool); ok && actual != state.ExposeInSQLLab.ValueBool() {
			diffs = append(diffs, fmt.Sprintf("expose_in_sqllab desired=%t actual=%t", state.ExposeInSQLLab.ValueBool(), actual))
		}
		if len(diffs) > 0 {
			resp.Diagnostics.AddWarning(
				"Superset database flags differ from API",
				"Superset reports different values than configured: "+strings.Join(diffs, ", ")+". "+
					"This may indicate an API limitation or engine/capability override.",
			)
		}
	}
	if val, ok := result["backend"].(string); ok {
		state.DBEngine = types.StringValue(val)
	}
	if params, ok := result["parameters"].(map[string]interface{}); ok {
		if val, ok := params["host"].(string); ok {
			state.DBHost = types.StringValue(val)
		}
		if val, ok := params["username"].(string); ok {
			state.DBUser = types.StringValue(val)
		}
		if val, ok := params["port"].(float64); ok {
			state.DBPort = types.Int64Value(int64(val))
		}
		if val, ok := params["database"].(string); ok {
			state.DBName = types.StringValue(val)
		}
		// Preserve the db_pass value from the state if it exists.
		if !state.DBPass.IsNull() {
			state.DBPass = types.StringValue(state.DBPass.ValueString())
		} else {
			state.DBPass = types.StringNull()
		}
	}

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
func (r *databaseResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	tflog.Debug(ctx, "Starting Update method")
	var plan databaseResourceModel
	var state databaseResourceModel

	diags := req.Plan.Get(ctx, &plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		tflog.Debug(ctx, "Exiting Update due to error in retrieving plan", map[string]interface{}{
			"diagnostics": resp.Diagnostics,
		})
		return
	}

	diags = req.State.Get(ctx, &state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		tflog.Debug(ctx, "Exiting Update due to error in retrieving state", map[string]interface{}{
			"diagnostics": resp.Diagnostics,
		})
		return
	}

	sqlalchemyURI := fmt.Sprintf("%s://%s:%s@%s:%d/%s", plan.DBEngine.ValueString(), plan.DBUser.ValueString(), plan.DBPass.ValueString(), plan.DBHost.ValueString(), plan.DBPort.ValueInt64(), plan.DBName.ValueString())
	extra := `{"client_encoding": "utf8"}`
	payload := map[string]interface{}{
		"allow_csv_upload":                  false,
		"allow_ctas":                        plan.AllowCTAS.ValueBool(),
		"allow_cvas":                        plan.AllowCVAS.ValueBool(),
		"allow_dml":                         plan.AllowDML.ValueBool(),
		"allow_multi_schema_metadata_fetch": true,
		"allow_run_async":                   plan.AllowRunAsync.ValueBool(),
		"cache_timeout":                     0,
		"expose_in_sqllab":                  plan.ExposeInSQLLab.ValueBool(),
		"database_name":                     plan.ConnectionName.ValueString(),
		"sqlalchemy_uri":                    sqlalchemyURI,
		"extra":                             extra,
	}

	result, err := r.client.UpdateDatabase(state.ID.ValueInt64(), payload)
	if err != nil {
		resp.Diagnostics.AddError(
			"Unable to Update Superset Database Connection",
			fmt.Sprintf("UpdateDatabase failed: %s", err.Error()),
		)
		return
	}

	resultData, ok := result["result"].(map[string]interface{})
	if !ok {
		resp.Diagnostics.AddError(
			"Invalid Response",
			"The response from the API does not contain the expected 'result' field",
		)
		return
	}

	// Warn if the API returned different values than requested.
	{
		var diffs []string
		if actual, ok := resultData["allow_ctas"].(bool); ok && actual != plan.AllowCTAS.ValueBool() {
			diffs = append(diffs, fmt.Sprintf("allow_ctas desired=%t actual=%t", plan.AllowCTAS.ValueBool(), actual))
		}
		if actual, ok := resultData["allow_cvas"].(bool); ok && actual != plan.AllowCVAS.ValueBool() {
			diffs = append(diffs, fmt.Sprintf("allow_cvas desired=%t actual=%t", plan.AllowCVAS.ValueBool(), actual))
		}
		if actual, ok := resultData["allow_dml"].(bool); ok && actual != plan.AllowDML.ValueBool() {
			diffs = append(diffs, fmt.Sprintf("allow_dml desired=%t actual=%t", plan.AllowDML.ValueBool(), actual))
		}
		if actual, ok := resultData["allow_run_async"].(bool); ok && actual != plan.AllowRunAsync.ValueBool() {
			diffs = append(diffs, fmt.Sprintf("allow_run_async desired=%t actual=%t", plan.AllowRunAsync.ValueBool(), actual))
		}
		if actual, ok := resultData["expose_in_sqllab"].(bool); ok && actual != plan.ExposeInSQLLab.ValueBool() {
			diffs = append(diffs, fmt.Sprintf("expose_in_sqllab desired=%t actual=%t", plan.ExposeInSQLLab.ValueBool(), actual))
		}
		if len(diffs) > 0 {
			resp.Diagnostics.AddWarning(
				"Superset database flags not honored by API",
				"Superset returned different values than requested: "+strings.Join(diffs, ", ")+". "+
					"This may be an API limitation or engine/capability override; continuing with the planned state to avoid inconsistent apply errors.",
			)
		}
	}

	// Keep the planned values in state to avoid inconsistent apply errors.
	state.ConnectionName = types.StringValue(plan.ConnectionName.ValueString())
	state.DBEngine = types.StringValue(plan.DBEngine.ValueString())
	state.DBUser = types.StringValue(plan.DBUser.ValueString())
	state.DBPass = types.StringValue(plan.DBPass.ValueString())
	state.DBHost = types.StringValue(plan.DBHost.ValueString())
	state.DBPort = types.Int64Value(plan.DBPort.ValueInt64())
	state.DBName = types.StringValue(plan.DBName.ValueString())
	state.AllowCTAS = types.BoolValue(plan.AllowCTAS.ValueBool())
	state.AllowCVAS = types.BoolValue(plan.AllowCVAS.ValueBool())
	state.AllowDML = types.BoolValue(plan.AllowDML.ValueBool())
	state.AllowRunAsync = types.BoolValue(plan.AllowRunAsync.ValueBool())
	state.ExposeInSQLLab = types.BoolValue(plan.ExposeInSQLLab.ValueBool())

	diags = resp.State.Set(ctx, &state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		tflog.Debug(ctx, "Exiting Update due to error in setting state", map[string]interface{}{
			"diagnostics": resp.Diagnostics,
		})
		return
	}

	tflog.Debug(ctx, fmt.Sprintf("Updated database connection: ID=%d, ConnectionName=%s", state.ID.ValueInt64(), state.ConnectionName.ValueString()))
}

// Delete deletes the resource and removes the Terraform state on success.
func (r *databaseResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	tflog.Debug(ctx, "Starting Delete method")
	var state databaseResourceModel
	diags := req.State.Get(ctx, &state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		tflog.Debug(ctx, "Exiting Delete due to error in getting state", map[string]interface{}{
			"diagnostics": resp.Diagnostics,
		})
		return
	}

	err := r.client.DeleteDatabase(state.ID.ValueInt64())
	if err != nil {
		resp.Diagnostics.AddError(
			"Unable to Delete Superset Database Connection",
			fmt.Sprintf("DeleteDatabase failed: %s", err.Error()),
		)
		return
	}

	resp.State.RemoveResource(ctx)
	tflog.Debug(ctx, fmt.Sprintf("Deleted database connection: ID=%d", state.ID.ValueInt64()))
}

// ImportState imports an existing resource.
func (r *databaseResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
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

	// Call Read to refresh the state with the latest data
	r.Read(ctx, resource.ReadRequest{State: resp.State}, &resource.ReadResponse{
		State:       resp.State,
		Diagnostics: resp.Diagnostics,
	})

	tflog.Debug(ctx, "ImportState completed successfully", map[string]interface{}{
		"import_id": req.ID,
	})
}

// Configure adds the provider configured client to the resource.
func (r *databaseResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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
