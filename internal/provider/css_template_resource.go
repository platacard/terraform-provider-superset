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
	_ resource.Resource                = &cssTemplateResource{}
	_ resource.ResourceWithConfigure   = &cssTemplateResource{}
	_ resource.ResourceWithImportState = &cssTemplateResource{}
)

// NewCSSTemplateResource is a helper function to simplify the provider implementation.
func NewCSSTemplateResource() resource.Resource {
	return &cssTemplateResource{}
}

// cssTemplateResource manages a CSS template.
type cssTemplateResource struct {
	client *client.Client
}

// cssTemplateResourceModel maps the resource schema data.
type cssTemplateResourceModel struct {
	ID           types.Int64  `tfsdk:"id"`
	TemplateName types.String `tfsdk:"template_name"`
	CSS          types.String `tfsdk:"css"`
}

// Metadata returns the resource type name.
func (r *cssTemplateResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_css_template"
}

// Schema defines the schema for the resource.
func (r *cssTemplateResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Manages a CSS template: a reusable stylesheet that can be applied to dashboards.",
		Attributes: map[string]schema.Attribute{
			"id": schema.Int64Attribute{
				Description: "Numeric identifier of the template.",
				Computed:    true,
				PlanModifiers: []planmodifier.Int64{
					int64planmodifier.UseStateForUnknown(),
				},
			},
			"template_name": schema.StringAttribute{
				Description: "Name of the template as shown in Superset.",
				Required:    true,
			},
			"css": schema.StringAttribute{
				Description: "The stylesheet.",
				Required:    true,
			},
		},
	}
}

// Create creates the template and sets the initial Terraform state.
func (r *cssTemplateResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan cssTemplateResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	id, err := r.client.CreateCSSTemplate(plan.TemplateName.ValueString(), plan.CSS.ValueString())
	if err != nil {
		resp.Diagnostics.AddError("Unable to Create Superset CSS Template", err.Error())
		return
	}

	plan.ID = types.Int64Value(id)
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

// Read refreshes the Terraform state with the template as it is in Superset.
func (r *cssTemplateResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state cssTemplateResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	template, err := r.client.GetCSSTemplate(state.ID.ValueInt64())
	if errors.Is(err, client.ErrNotFound) {
		tflog.Warn(ctx, "CSS template no longer exists in Superset, removing it from the state", map[string]any{"id": state.ID.ValueInt64()})
		resp.State.RemoveResource(ctx)
		return
	}
	if err != nil {
		resp.Diagnostics.AddError("Unable to Read Superset CSS Template", err.Error())
		return
	}

	state.TemplateName = types.StringValue(template.TemplateName)
	state.CSS = types.StringValue(template.CSS)
	resp.Diagnostics.Append(resp.State.Set(ctx, &state)...)
}

// Update replaces the name and the CSS of the template.
func (r *cssTemplateResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state cssTemplateResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	if err := r.client.UpdateCSSTemplate(state.ID.ValueInt64(), plan.TemplateName.ValueString(), plan.CSS.ValueString()); err != nil {
		resp.Diagnostics.AddError("Unable to Update Superset CSS Template", err.Error())
		return
	}

	plan.ID = state.ID
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

// Delete deletes the template; a template that is already gone is not an error.
func (r *cssTemplateResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state cssTemplateResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	err := r.client.DeleteCSSTemplate(state.ID.ValueInt64())
	if err != nil && !errors.Is(err, client.ErrNotFound) {
		resp.Diagnostics.AddError("Unable to Delete Superset CSS Template", err.Error())
		return
	}
}

// ImportState imports a template by its numeric id.
func (r *cssTemplateResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	id, err := strconv.ParseInt(req.ID, 10, 64)
	if err != nil {
		resp.Diagnostics.AddError("Invalid Import ID", fmt.Sprintf("The import ID %q is not a numeric template id.", req.ID))
		return
	}
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("id"), id)...)
}

// Configure adds the provider configured client to the resource.
func (r *cssTemplateResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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
