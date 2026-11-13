package provider

import (
	"context"
	"errors"
	"fmt"

	"github.com/hashicorp/terraform-plugin-framework/diag"
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
	_ resource.Resource              = &sqlExecutionResource{}
	_ resource.ResourceWithConfigure = &sqlExecutionResource{}
)

// NewSQLExecutionResource is a helper function to simplify the provider implementation.
func NewSQLExecutionResource() resource.Resource {
	return &sqlExecutionResource{}
}

// sqlExecutionResource runs a SQL statement through SQL Lab.
type sqlExecutionResource struct {
	client *client.Client
}

// sqlExecutionResourceModel maps the resource schema data.
type sqlExecutionResourceModel struct {
	ID           types.Int64  `tfsdk:"id"`
	DatabaseName types.String `tfsdk:"database_name"`
	DatabaseID   types.Int64  `tfsdk:"database_id"`
	SQL          types.String `tfsdk:"sql"`
	Schema       types.String `tfsdk:"schema"`
	Catalog      types.String `tfsdk:"catalog"`
	DestroySQL   types.String `tfsdk:"destroy_sql"`
	Triggers     types.Map    `tfsdk:"triggers"`
}

// Metadata returns the resource type name.
func (r *sqlExecutionResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_sql_execution"
}

// Schema defines the schema for the resource.
func (r *sqlExecutionResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Runs a SQL statement through SQL Lab on a database connection, for example to create a view or a table that a dataset is then built on. The statement runs when the resource is created and again whenever `triggers` change; a change of the statement, the connection, the schema or the catalog replaces the resource, which runs `destroy_sql` first when it is set. The connection must be exposed in SQL Lab (`expose_in_sqllab`) and allow DML (`allow_dml`) for a statement that changes data or structure. Several statements separated by `;` run in order; the rows a statement returns are discarded.",
		Attributes: map[string]schema.Attribute{
			"id": schema.Int64Attribute{
				Description: "Identifier of the last run in the query history of SQL Lab.",
				Computed:    true,
			},
			"database_name": schema.StringAttribute{
				Description: "Name of the database connection to run on. Exactly one of `database_name` and `database_id` must be set.",
				Optional:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"database_id": schema.Int64Attribute{
				Description: "Numeric identifier of the connection, looked up from `database_name` when that is set.",
				Optional:    true,
				Computed:    true,
				PlanModifiers: []planmodifier.Int64{
					int64planmodifier.RequiresReplace(),
					int64planmodifier.UseStateForUnknown(),
				},
			},
			"sql": schema.StringAttribute{
				Description: "The statement to run.",
				Required:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"schema": schema.StringAttribute{
				Description: "Schema the unqualified names of the statement are resolved in.",
				Optional:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"catalog": schema.StringAttribute{
				Description: "Catalog to run in, on Superset 6 connections with several catalogs. Left to the default of the connection when unset.",
				Optional:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"destroy_sql": schema.StringAttribute{
				Description: "Statement run when the resource is destroyed or replaced, for example `DROP VIEW IF EXISTS daily_sales`. Nothing runs when unset, and nothing runs when the connection no longer exists.",
				Optional:    true,
			},
			"triggers": schema.MapAttribute{
				Description: "Arbitrary values whose change runs the statement again, for example a version.",
				ElementType: types.StringType,
				Optional:    true,
			},
		},
	}
}

// Create runs the statement.
func (r *sqlExecutionResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan sqlExecutionResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	databaseID, ok := r.databaseID(plan, &resp.Diagnostics)
	if !ok {
		return
	}
	plan.DatabaseID = types.Int64Value(databaseID)
	queryID, err := r.run(plan, plan.SQL.ValueString())
	if err != nil {
		resp.Diagnostics.AddError("Unable to Run the SQL Statement in Superset", err.Error())
		return
	}

	plan.ID = types.Int64Value(queryID)
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

// Read checks that the connection still exists, so the statement runs again on a connection
// that was recreated.
func (r *sqlExecutionResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state sqlExecutionResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	_, err := r.client.GetDatabaseConnection(state.DatabaseID.ValueInt64())
	if errors.Is(err, client.ErrNotFound) {
		tflog.Warn(ctx, "database connection no longer exists in Superset, removing the SQL execution from the state", map[string]any{"database_id": state.DatabaseID.ValueInt64()})
		resp.State.RemoveResource(ctx)
		return
	}
	if err != nil {
		resp.Diagnostics.AddError("Unable to Read Superset Database Connection", err.Error())
		return
	}
}

// Update runs the statement again when the triggers changed; a change of destroy_sql alone is
// only recorded.
func (r *sqlExecutionResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state sqlExecutionResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	plan.ID = state.ID
	if !plan.Triggers.Equal(state.Triggers) {
		queryID, err := r.run(plan, plan.SQL.ValueString())
		if err != nil {
			resp.Diagnostics.AddError("Unable to Run the SQL Statement in Superset", err.Error())
			return
		}
		plan.ID = types.Int64Value(queryID)
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

// Delete runs destroy_sql when it is set and the connection still exists.
func (r *sqlExecutionResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state sqlExecutionResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	if state.DestroySQL.ValueString() == "" {
		return
	}

	_, err := r.client.GetDatabaseConnection(state.DatabaseID.ValueInt64())
	if errors.Is(err, client.ErrNotFound) {
		tflog.Warn(ctx, "database connection no longer exists in Superset, skipping the destroy statement", map[string]any{"database_id": state.DatabaseID.ValueInt64()})
		return
	}
	if err != nil {
		resp.Diagnostics.AddError("Unable to Read Superset Database Connection", err.Error())
		return
	}
	if _, err := r.run(state, state.DestroySQL.ValueString()); err != nil {
		resp.Diagnostics.AddError("Unable to Run the Destroy SQL Statement in Superset", err.Error()+"\nRemove destroy_sql from the configuration to forget the resource without running it.")
		return
	}
}

// databaseID resolves the connection of the model: by name when the configuration names it.
func (r *sqlExecutionResource) databaseID(model sqlExecutionResourceModel, diags *diag.Diagnostics) (int64, bool) {
	switch {
	case !model.DatabaseName.IsNull() && !model.DatabaseID.IsNull() && !model.DatabaseID.IsUnknown():
		diags.AddAttributeError(path.Root("database_id"), "Conflicting Attributes", "Set either database_name or database_id, not both.")
		return 0, false
	case !model.DatabaseName.IsNull():
		return databaseIDByName(r.client, model.DatabaseName.ValueString(), diags)
	case !model.DatabaseID.IsNull() && !model.DatabaseID.IsUnknown():
		return model.DatabaseID.ValueInt64(), true
	default:
		diags.AddAttributeError(path.Root("database_name"), "Missing Database", "Set database_name or database_id.")
		return 0, false
	}
}

// run executes a statement on the connection of the model and returns the id of the run. The
// rows of a statement that returns any are discarded, so the limit is kept minimal.
func (r *sqlExecutionResource) run(model sqlExecutionResourceModel, sql string) (int64, error) {
	if sql == "" {
		return 0, errors.New("the statement must not be empty")
	}
	result, err := r.client.ExecuteSQL(client.SQLQuery{
		DatabaseID: model.DatabaseID.ValueInt64(),
		SQL:        sql,
		Schema:     model.Schema.ValueString(),
		Catalog:    model.Catalog.ValueString(),
		Limit:      1,
	})
	if err != nil {
		return 0, err
	}
	return result.QueryID, nil
}

// Configure adds the provider configured client to the resource.
func (r *sqlExecutionResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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
