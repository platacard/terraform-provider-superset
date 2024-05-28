package provider

import (
	"context"
	"fmt"
	"strconv"

	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/int64planmodifier"
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
	SQLAlchemyURI  types.String `tfsdk:"sqlalchemy_uri"`
	Driver         types.String `tfsdk:"driver"`
	UUID           types.String `tfsdk:"uuid"`
	AllowCTAS      types.Bool   `tfsdk:"allow_ctas"`
	AllowCVAS      types.Bool   `tfsdk:"allow_cvas"`
	AllowDML       types.Bool   `tfsdk:"allow_dml"`
	AllowRunAsync  types.Bool   `tfsdk:"allow_run_async"`
	ExposeInSQLLab types.Bool   `tfsdk:"expose_in_sqllab"`
	Extra          types.String `tfsdk:"extra"`
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
			"sqlalchemy_uri": schema.StringAttribute{
				Description: "SQLAlchemy URI of the database connection.",
				Computed:    true,
			},
			"driver": schema.StringAttribute{
				Description: "Database driver.",
				Computed:    true,
			},
			"uuid": schema.StringAttribute{
				Description: "UUID of the database connection.",
				Computed:    true,
			},
			"allow_ctas": schema.BoolAttribute{
				Description: "Allow CTAS.",
				Computed:    true,
			},
			"allow_cvas": schema.BoolAttribute{
				Description: "Allow CVAS.",
				Computed:    true,
			},
			"allow_dml": schema.BoolAttribute{
				Description: "Allow DML.",
				Computed:    true,
			},
			"allow_run_async": schema.BoolAttribute{
				Description: "Allow run async.",
				Computed:    true,
			},
			"expose_in_sqllab": schema.BoolAttribute{
				Description: "Expose in SQL Lab.",
				Computed:    true,
			},
			"extra": schema.StringAttribute{
				Description: "Extra configuration in JSON format.",
				Computed:    true,
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
        "allow_ctas":                        false,
        "allow_cvas":                        false,
        "allow_dml":                         false,
        "allow_multi_schema_metadata_fetch": true,
        "allow_run_async":                   true,
        "cache_timeout":                     0,
        "expose_in_sqllab":                  true,
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

    plan.ID = types.Int64Value(int64(result["id"].(float64)))
    resultData := result["result"].(map[string]interface{})
    plan.ConnectionName = types.StringValue(resultData["database_name"].(string))
    plan.SQLAlchemyURI = types.StringValue(resultData["sqlalchemy_uri"].(string))
    plan.UUID = types.StringValue(resultData["uuid"].(string))
    plan.AllowCTAS = types.BoolValue(resultData["allow_ctas"].(bool))
    plan.AllowCVAS = types.BoolValue(resultData["allow_cvas"].(bool))
    plan.AllowDML = types.BoolValue(resultData["allow_dml"].(bool))
    plan.AllowRunAsync = types.BoolValue(resultData["allow_run_async"].(bool))
    plan.ExposeInSQLLab = types.BoolValue(resultData["expose_in_sqllab"].(bool))
    plan.Extra = types.StringValue(resultData["extra"].(string))

    if driver, ok := resultData["driver"].(string); ok {
        plan.Driver = types.StringValue(driver)
    } else {
        resp.Diagnostics.AddError(
            "Driver not found",
            "The driver attribute is missing in the response",
        )
        return
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
    if val, ok := result["sqlalchemy_uri"].(string); ok {
        state.SQLAlchemyURI = types.StringValue(val)
    } else {
        resp.Diagnostics.AddError(
            "Invalid Response",
            "The response from the API does not contain a valid 'sqlalchemy_uri' field",
        )
        return
    }
    if val, ok := result["uuid"].(string); ok {
        state.UUID = types.StringValue(val)
    } else {
        tflog.Warn(ctx, "The response from the API does not contain a valid 'uuid' field")
    }
    if val, ok := result["allow_ctas"].(bool); ok {
        state.AllowCTAS = types.BoolValue(val)
    }
    if val, ok := result["allow_cvas"].(bool); ok {
        state.AllowCVAS = types.BoolValue(val)
    }
    if val, ok := result["allow_dml"].(bool); ok {
        state.AllowDML = types.BoolValue(val)
    }
    if val, ok := result["allow_run_async"].(bool); ok {
        state.AllowRunAsync = types.BoolValue(val)
    }
    if val, ok := result["expose_in_sqllab"].(bool); ok {
        state.ExposeInSQLLab = types.BoolValue(val)
    }
    if val, ok := result["extra"].(string); ok {
        state.Extra = types.StringValue(val)
    }

    if driver, ok := result["driver"].(string); ok && driver != "" {
        state.Driver = types.StringValue(driver)
    } else {
        tflog.Warn(ctx, "The response from the API does not contain a valid 'driver' field")
    }

    state.DBEngine = types.StringValue(state.DBEngine.ValueString())

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

    // Update state attributes with the values from the response
    if val, ok := resultData["database_name"].(string); ok {
        state.ConnectionName = types.StringValue(val)
    } else {
        resp.Diagnostics.AddError(
            "Invalid Response",
            "The response from the API does not contain a valid 'database_name' field",
        )
        return
    }
    if val, ok := resultData["sqlalchemy_uri"].(string); ok {
        state.SQLAlchemyURI = types.StringValue(val)
    } else {
        resp.Diagnostics.AddError(
            "Invalid Response",
            "The response from the API does not contain a valid 'sqlalchemy_uri' field",
        )
        return
    }
    if val, ok := resultData["uuid"].(string); ok {
        state.UUID = types.StringValue(val)
    } else {
        tflog.Warn(ctx, "The response from the API does not contain a valid 'uuid' field")
    }
    if val, ok := resultData["allow_ctas"].(bool); ok {
        state.AllowCTAS = types.BoolValue(val)
    }
    if val, ok := resultData["allow_cvas"].(bool); ok {
        state.AllowCVAS = types.BoolValue(val)
    }
    if val, ok := resultData["allow_dml"].(bool); ok {
        state.AllowDML = types.BoolValue(val)
    }
    if val, ok := resultData["allow_run_async"].(bool); ok {
        state.AllowRunAsync = types.BoolValue(val)
    }
    if val, ok := resultData["expose_in_sqllab"].(bool); ok {
        state.ExposeInSQLLab = types.BoolValue(val)
    }
    if val, ok := resultData["extra"].(string); ok {
        state.Extra = types.StringValue(val)
    }

    if driver, ok := resultData["driver"].(string); ok {
        state.Driver = types.StringValue(driver)
    } else {
        tflog.Warn(ctx, "The response from the API does not contain a valid 'driver' field")
        state.Driver = types.StringValue(plan.Driver.ValueString())
    }

    state.DBEngine = types.StringValue(plan.DBEngine.ValueString())
    state.DBUser = types.StringValue(plan.DBUser.ValueString())
    state.DBPass = types.StringValue(plan.DBPass.ValueString())
    state.DBHost = types.StringValue(plan.DBHost.ValueString())
    state.DBPort = types.Int64Value(plan.DBPort.ValueInt64())
    state.DBName = types.StringValue(plan.DBName.ValueString())

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
