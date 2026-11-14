package provider

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/int64planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringdefault"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
	"github.com/platacard/terraform-provider-superset/internal/client"
)

// Ensure the implementation satisfies the expected interfaces.
var (
	_ resource.Resource                = &rowLevelSecurityResource{}
	_ resource.ResourceWithConfigure   = &rowLevelSecurityResource{}
	_ resource.ResourceWithImportState = &rowLevelSecurityResource{}
)

// NewRowLevelSecurityResource is a helper function to simplify the provider implementation.
func NewRowLevelSecurityResource() resource.Resource {
	return &rowLevelSecurityResource{}
}

// rowLevelSecurityResource manages a row level security rule.
type rowLevelSecurityResource struct {
	client *client.Client
}

// rowLevelSecurityResourceModel maps the resource schema data.
type rowLevelSecurityResourceModel struct {
	ID          types.Int64  `tfsdk:"id"`
	Name        types.String `tfsdk:"name"`
	Description types.String `tfsdk:"description"`
	FilterType  types.String `tfsdk:"filter_type"`
	GroupKey    types.String `tfsdk:"group_key"`
	Clause      types.String `tfsdk:"clause"`
	Tables      types.Set    `tfsdk:"tables"`
	Roles       types.Set    `tfsdk:"roles"`
}

// Metadata returns the resource type name.
func (r *rowLevelSecurityResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_row_level_security"
}

// Schema defines the schema for the resource.
func (r *rowLevelSecurityResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Manages a row level security rule: a SQL clause that Superset adds to the queries of the given datasets " +
			"for the members of the given roles.",
		Attributes: map[string]schema.Attribute{
			"id": schema.Int64Attribute{
				Description: "Numeric identifier of the rule.",
				Computed:    true,
				PlanModifiers: []planmodifier.Int64{
					int64planmodifier.UseStateForUnknown(),
				},
			},
			"name": schema.StringAttribute{
				Description: "Name of the rule.",
				Required:    true,
			},
			"description": schema.StringAttribute{
				Description: "Free-text description of the rule.",
				Optional:    true,
			},
			"filter_type": schema.StringAttribute{
				Description: "`Regular` applies the clause to the queries of the listed roles; `Base` applies it to everyone " +
					"except the listed roles. Defaults to `Regular`.",
				Optional:   true,
				Computed:   true,
				Default:    stringdefault.StaticString("Regular"),
				Validators: []validator.String{oneOf{"Regular", "Base"}},
			},
			"group_key": schema.StringAttribute{
				Description: "Rules that share a group key are combined with OR, rules with different keys with AND.",
				Optional:    true,
			},
			"clause": schema.StringAttribute{
				Description: "SQL expression added to the WHERE clause, for example `region = 'EMEA'`.",
				Required:    true,
			},
			"tables": schema.SetAttribute{
				Description: "Identifiers of the datasets the rule applies to.",
				Required:    true,
				ElementType: types.Int64Type,
			},
			"roles": schema.SetAttribute{
				Description: "Identifiers of the roles the rule applies to (or is excluded from, for a `Base` rule). Defaults to none.",
				Optional:    true,
				ElementType: types.Int64Type,
			},
		},
	}
}

// Create creates the rule and sets the initial Terraform state.
func (r *rowLevelSecurityResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan rowLevelSecurityResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	rule, diags := ruleFromModel(ctx, plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	id, err := r.client.CreateRowLevelSecurity(rule)
	if err != nil {
		resp.Diagnostics.AddError("Unable to Create Superset Row Level Security Rule", err.Error())
		return
	}

	plan.ID = types.Int64Value(id)
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

// Read refreshes the Terraform state with the rule as it is in Superset.
func (r *rowLevelSecurityResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state rowLevelSecurityResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	rule, err := r.client.GetRowLevelSecurity(state.ID.ValueInt64())
	if errors.Is(err, client.ErrNotFound) {
		tflog.Warn(ctx, "row level security rule no longer exists in Superset, removing it from the state", map[string]any{"id": state.ID.ValueInt64()})
		resp.State.RemoveResource(ctx)
		return
	}
	if err != nil {
		resp.Diagnostics.AddError("Unable to Read Superset Row Level Security Rule", err.Error())
		return
	}

	resp.Diagnostics.Append(modelFromRule(ctx, rule, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &state)...)
}

// Update replaces every attribute of the rule.
func (r *rowLevelSecurityResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state rowLevelSecurityResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	rule, diags := ruleFromModel(ctx, plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	if err := r.client.UpdateRowLevelSecurity(state.ID.ValueInt64(), rule); err != nil {
		resp.Diagnostics.AddError("Unable to Update Superset Row Level Security Rule", err.Error())
		return
	}

	plan.ID = state.ID
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

// Delete deletes the rule; a rule that is already gone is not an error.
func (r *rowLevelSecurityResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state rowLevelSecurityResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	err := r.client.DeleteRowLevelSecurity(state.ID.ValueInt64())
	if err != nil && !errors.Is(err, client.ErrNotFound) {
		resp.Diagnostics.AddError("Unable to Delete Superset Row Level Security Rule", err.Error())
		return
	}
}

// ImportState imports a rule by its numeric id.
func (r *rowLevelSecurityResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	id, err := strconv.ParseInt(req.ID, 10, 64)
	if err != nil {
		resp.Diagnostics.AddError("Invalid Import ID", fmt.Sprintf("The import ID %q is not a numeric rule id.", req.ID))
		return
	}
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("id"), id)...)
}

// Configure adds the provider configured client to the resource.
func (r *rowLevelSecurityResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

// ruleFromModel converts the planned attributes into the client representation.
func ruleFromModel(ctx context.Context, model rowLevelSecurityResourceModel) (client.RowLevelSecurity, diag.Diagnostics) {
	var diags diag.Diagnostics
	rule := client.RowLevelSecurity{
		Name:        model.Name.ValueString(),
		Description: model.Description.ValueString(),
		FilterType:  model.FilterType.ValueString(),
		GroupKey:    model.GroupKey.ValueString(),
		Clause:      model.Clause.ValueString(),
	}
	diags.Append(model.Tables.ElementsAs(ctx, &rule.TableIDs, false)...)
	if !model.Roles.IsNull() {
		diags.Append(model.Roles.ElementsAs(ctx, &rule.RoleIDs, false)...)
	}
	return rule, diags
}

// modelFromRule copies the rule as Superset reports it into the state. Empty optional strings
// stay null, and an empty role set stays null when the configuration did not set roles.
func modelFromRule(ctx context.Context, rule *client.RowLevelSecurity, state *rowLevelSecurityResourceModel) diag.Diagnostics {
	var diags diag.Diagnostics
	state.Name = types.StringValue(rule.Name)
	state.Description = optionalString(rule.Description)
	state.FilterType = types.StringValue(rule.FilterType)
	state.GroupKey = optionalString(rule.GroupKey)
	state.Clause = types.StringValue(rule.Clause)

	tables, d := types.SetValueFrom(ctx, types.Int64Type, rule.TableIDs)
	diags.Append(d...)
	state.Tables = tables

	if len(rule.RoleIDs) == 0 && state.Roles.IsNull() {
		return diags
	}
	roles, d := types.SetValueFrom(ctx, types.Int64Type, rule.RoleIDs)
	diags.Append(d...)
	state.Roles = roles
	return diags
}

// optionalString maps an empty value from the API to null, which is how an unset optional attribute is stored.
func optionalString(value string) types.String {
	if value == "" {
		return types.StringNull()
	}
	return types.StringValue(value)
}
