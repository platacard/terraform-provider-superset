package provider

import (
	"context"
	"fmt"
	"strconv"

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
	_ resource.Resource                = &datasetResource{}
	_ resource.ResourceWithConfigure   = &datasetResource{}
	_ resource.ResourceWithImportState = &datasetResource{}
)

// NewDatasetResource is a helper function to simplify the provider implementation.
func NewDatasetResource() resource.Resource {
	return &datasetResource{}
}

// datasetResource is the resource implementation.
type datasetResource struct {
	client *client.Client
}

// datasetResourceModel maps the resource schema data.
type datasetResourceModel struct {
	ID           types.Int64  `tfsdk:"id"`
	TableName    types.String `tfsdk:"table_name"`
	DatabaseName types.String `tfsdk:"database_name"`
	Schema       types.String `tfsdk:"schema"`
	SQL          types.String `tfsdk:"sql"`
}

// Metadata returns the resource type name.
func (r *datasetResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_dataset"
}

// Schema defines the schema for the resource.
func (r *datasetResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Manages a dataset in Superset.",
		Attributes: map[string]schema.Attribute{
			"id": schema.Int64Attribute{
				Description: "Numeric identifier of the dataset.",
				Computed:    true,
				PlanModifiers: []planmodifier.Int64{
					int64planmodifier.UseStateForUnknown(),
				},
			},
			"table_name": schema.StringAttribute{
				Description: "Name of the table or dataset.",
				Required:    true,
			},
			"database_name": schema.StringAttribute{
				Description: "Name of the database where the dataset resides. Cannot be changed after creation.",
				Required:    true,
			},
			"schema": schema.StringAttribute{
				Description: "Database schema name (optional).",
				Optional:    true,
			},
			"sql": schema.StringAttribute{
				Description: "SQL query for the dataset (optional, for SQL-based datasets).",
				Optional:    true,
			},
		},
	}
}

// Create creates the resource and sets the initial Terraform state.
func (r *datasetResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	// Retrieve values from plan
	var plan datasetResourceModel
	diags := req.Plan.Get(ctx, &plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	tflog.Debug(ctx, "Creating dataset", map[string]interface{}{
		"table_name":    plan.TableName.ValueString(),
		"database_name": plan.DatabaseName.ValueString(),
	})

	// Get database ID by name
	databaseID, err := r.client.GetDatabaseIDByName(plan.DatabaseName.ValueString())
	if err != nil {
		resp.Diagnostics.AddError(
			"Error finding database",
			fmt.Sprintf("Could not find database '%s': %s", plan.DatabaseName.ValueString(), err.Error()),
		)
		return
	}

	// Create dataset request
	datasetReq := client.DatasetRequest{
		TableName: plan.TableName.ValueString(),
		Database:  databaseID,
		Schema:    plan.Schema.ValueString(),
		SQL:       plan.SQL.ValueString(),
	}

	// Create dataset
	datasetResp, err := r.client.CreateDataset(datasetReq)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error creating dataset",
			"Could not create dataset: "+err.Error(),
		)
		return
	}

	// Extract ID from response
	var datasetID int64
	if id, ok := (*datasetResp)["id"].(float64); ok {
		datasetID = int64(id)
	} else {
		resp.Diagnostics.AddError(
			"Error creating dataset",
			"Could not extract ID from create response",
		)
		return
	}

	// Update the state
	plan.ID = types.Int64Value(datasetID)

	tflog.Debug(ctx, "Created dataset", map[string]interface{}{
		"id": datasetID,
	})

	// Set state to fully populated data
	diags = resp.State.Set(ctx, plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
}

// Read refreshes the Terraform state with the latest data.
func (r *datasetResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	// Get current state
	var state datasetResourceModel
	diags := req.State.Get(ctx, &state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Get dataset from API
	dataset, err := r.client.GetDataset(state.ID.ValueInt64())
	if err != nil {
		resp.Diagnostics.AddError(
			"Error reading dataset",
			"Could not read dataset ID "+fmt.Sprintf("%d", state.ID.ValueInt64())+": "+err.Error(),
		)
		return
	}

	// Update state from API response
	if tableName, ok := (*dataset)["table_name"].(string); ok {
		state.TableName = types.StringValue(tableName)
	}

	if schema, ok := (*dataset)["schema"].(string); ok {
		state.Schema = types.StringValue(schema)
	}

	if sql, ok := (*dataset)["sql"].(string); ok {
		state.SQL = types.StringValue(sql)
	}

	// Get database name by ID
	if database, ok := (*dataset)["database"].(map[string]interface{}); ok {
		if dbID, ok := database["id"].(float64); ok {
			databaseName, err := r.client.GetDatabaseNameByID(int64(dbID))
			if err != nil {
				resp.Diagnostics.AddError(
					"Error reading dataset",
					"Could not get database name: "+err.Error(),
				)
				return
			}
			state.DatabaseName = types.StringValue(databaseName)
		}
	}

	// Set refreshed state
	diags = resp.State.Set(ctx, &state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
}

// Update updates the resource and sets the updated Terraform state on success.
func (r *datasetResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	// Retrieve values from plan
	var plan datasetResourceModel
	diags := req.Plan.Get(ctx, &plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Update dataset (database cannot be changed, so we don't validate it)
	err := r.client.UpdateDataset(
		plan.ID.ValueInt64(),
		plan.TableName.ValueString(),
		plan.Schema.ValueString(),
		plan.SQL.ValueString(),
	)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error updating dataset",
			"Could not update dataset ID "+fmt.Sprintf("%d", plan.ID.ValueInt64())+": "+err.Error(),
		)
		return
	}

	tflog.Debug(ctx, "Updated dataset", map[string]interface{}{
		"id": plan.ID.ValueInt64(),
	})

	// Set updated state
	diags = resp.State.Set(ctx, plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
}

// Delete deletes the resource and removes the Terraform state on success.
func (r *datasetResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	// Retrieve values from state
	var state datasetResourceModel
	diags := req.State.Get(ctx, &state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Delete existing dataset
	err := r.client.DeleteDataset(state.ID.ValueInt64())
	if err != nil {
		resp.Diagnostics.AddError(
			"Error deleting dataset",
			"Could not delete dataset, unexpected error: "+err.Error(),
		)
		return
	}

	tflog.Debug(ctx, "Deleted dataset", map[string]interface{}{
		"id": state.ID.ValueInt64(),
	})
}

// Configure adds the provider configured client to the resource.
func (r *datasetResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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
func (r *datasetResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	// Retrieve import ID and save to id attribute
	id, err := strconv.ParseInt(req.ID, 10, 64)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error importing dataset",
			"Could not parse dataset ID: "+err.Error(),
		)
		return
	}

	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("id"), id)...)
}
