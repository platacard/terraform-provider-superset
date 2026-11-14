package provider

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/hashicorp/terraform-plugin-framework/path"
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
	_ resource.Resource                = &savedQueryResource{}
	_ resource.ResourceWithConfigure   = &savedQueryResource{}
	_ resource.ResourceWithImportState = &savedQueryResource{}
)

// NewSavedQueryResource is a helper function to simplify the provider implementation.
func NewSavedQueryResource() resource.Resource {
	return &savedQueryResource{}
}

// savedQueryResource manages a query saved in SQL Lab.
type savedQueryResource struct {
	client *client.Client
}

// savedQueryResourceModel maps the resource schema data.
type savedQueryResourceModel struct {
	ID                 types.Int64  `tfsdk:"id"`
	DatabaseID         types.Int64  `tfsdk:"database_id"`
	Label              types.String `tfsdk:"label"`
	SQL                types.String `tfsdk:"sql"`
	Description        types.String `tfsdk:"description"`
	Schema             types.String `tfsdk:"schema"`
	Catalog            types.String `tfsdk:"catalog"`
	TemplateParameters types.String `tfsdk:"template_parameters"`
}

// Metadata returns the resource type name.
func (r *savedQueryResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_saved_query"
}

// Schema defines the schema for the resource.
func (r *savedQueryResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Manages a query saved in SQL Lab.",
		Attributes: map[string]schema.Attribute{
			"id": schema.Int64Attribute{
				Description: "Numeric identifier of the saved query.",
				Computed:    true,
				PlanModifiers: []planmodifier.Int64{
					int64planmodifier.UseStateForUnknown(),
				},
			},
			"database_id": schema.Int64Attribute{
				Description: "Identifier of the database connection the query runs against.",
				Required:    true,
			},
			"label": schema.StringAttribute{
				Description: "Name of the saved query as shown in SQL Lab.",
				Required:    true,
			},
			"sql": schema.StringAttribute{
				Description: "The SQL text.",
				Required:    true,
			},
			"description": schema.StringAttribute{
				Description: "Free-text description of the query.",
				Optional:    true,
			},
			"schema": schema.StringAttribute{
				Description: "Default schema the query runs in.",
				Optional:    true,
			},
			"catalog": schema.StringAttribute{
				Description: "Default catalog the query runs in. Only Superset 4.1 and later support catalogs.",
				Optional:    true,
			},
			"template_parameters": schema.StringAttribute{
				Description: "JSON object with the Jinja template parameters of the query.",
				Optional:    true,
			},
		},
	}
}

// Create creates the saved query and sets the initial Terraform state.
func (r *savedQueryResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan savedQueryResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	id, err := r.client.CreateSavedQuery(savedQueryFromModel(plan))
	if err != nil {
		resp.Diagnostics.AddError("Unable to Create Superset Saved Query", err.Error())
		return
	}

	plan.ID = types.Int64Value(id)
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

// Read refreshes the Terraform state with the saved query as it is in Superset.
func (r *savedQueryResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state savedQueryResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	query, err := r.client.GetSavedQuery(state.ID.ValueInt64())
	if errors.Is(err, client.ErrNotFound) {
		tflog.Warn(ctx, "saved query no longer exists in Superset, removing it from the state", map[string]any{"id": state.ID.ValueInt64()})
		resp.State.RemoveResource(ctx)
		return
	}
	if err != nil {
		resp.Diagnostics.AddError("Unable to Read Superset Saved Query", err.Error())
		return
	}

	state.DatabaseID = types.Int64Value(query.DatabaseID)
	state.Label = types.StringValue(query.Label)
	state.SQL = types.StringValue(query.SQL)
	state.Description = optionalString(query.Description)
	state.Schema = optionalString(query.Schema)
	state.Catalog = optionalString(query.Catalog)
	state.TemplateParameters = optionalString(query.TemplateParameters)
	resp.Diagnostics.Append(resp.State.Set(ctx, &state)...)
}

// Update replaces the attributes of the saved query.
func (r *savedQueryResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state savedQueryResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	clearCatalog := !state.Catalog.IsNull() && plan.Catalog.IsNull()
	if err := r.client.UpdateSavedQuery(state.ID.ValueInt64(), savedQueryFromModel(plan), clearCatalog); err != nil {
		resp.Diagnostics.AddError("Unable to Update Superset Saved Query", err.Error())
		return
	}

	plan.ID = state.ID
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

// Delete deletes the saved query; a query that is already gone is not an error.
func (r *savedQueryResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state savedQueryResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	err := r.client.DeleteSavedQuery(state.ID.ValueInt64())
	if err != nil && !errors.Is(err, client.ErrNotFound) {
		resp.Diagnostics.AddError("Unable to Delete Superset Saved Query", err.Error())
		return
	}
}

// ImportState imports a saved query by its numeric id.
func (r *savedQueryResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	id, err := strconv.ParseInt(req.ID, 10, 64)
	if err != nil {
		resp.Diagnostics.AddError("Invalid Import ID", fmt.Sprintf("The import ID %q is not a numeric saved query id.", req.ID))
		return
	}
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("id"), id)...)
}

// Configure adds the provider configured client to the resource.
func (r *savedQueryResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

func savedQueryFromModel(model savedQueryResourceModel) client.SavedQuery {
	return client.SavedQuery{
		DatabaseID:         model.DatabaseID.ValueInt64(),
		Label:              model.Label.ValueString(),
		SQL:                model.SQL.ValueString(),
		Description:        model.Description.ValueString(),
		Schema:             model.Schema.ValueString(),
		Catalog:            model.Catalog.ValueString(),
		TemplateParameters: model.TemplateParameters.ValueString(),
	}
}
