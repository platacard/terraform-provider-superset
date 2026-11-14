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
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/booldefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/int64planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
	"github.com/platacard/terraform-provider-superset/internal/client"
)

// Ensure the implementation satisfies the expected interfaces.
var (
	_ resource.Resource                = &themeResource{}
	_ resource.ResourceWithConfigure   = &themeResource{}
	_ resource.ResourceWithImportState = &themeResource{}
)

// NewThemeResource is a helper function to simplify the provider implementation.
func NewThemeResource() resource.Resource {
	return &themeResource{}
}

// themeResource manages a theme of Superset 6.
type themeResource struct {
	client *client.Client
}

// themeResourceModel maps the resource schema data.
type themeResourceModel struct {
	ID            types.Int64  `tfsdk:"id"`
	Name          types.String `tfsdk:"name"`
	JSONData      jsonValue    `tfsdk:"json_data"`
	SystemDefault types.Bool   `tfsdk:"system_default"`
	SystemDark    types.Bool   `tfsdk:"system_dark"`
	UUID          types.String `tfsdk:"uuid"`
}

// Metadata returns the resource type name.
func (r *themeResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_theme"
}

// Schema defines the schema for the resource.
func (r *themeResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Manages a theme of the Superset UI, on Superset 6: a JSON document of design tokens that a `superset_dashboard` applies with `theme_id`, and that can be the default or the dark theme of the whole instance.",
		Attributes: map[string]schema.Attribute{
			"id": schema.Int64Attribute{
				Description: "Numeric identifier of the theme.",
				Computed:    true,
				PlanModifiers: []planmodifier.Int64{
					int64planmodifier.UseStateForUnknown(),
				},
			},
			"name": schema.StringAttribute{
				Description: "Name of the theme.",
				Required:    true,
			},
			"json_data": schema.StringAttribute{
				Description: "JSON document with the design tokens of the theme, as the theme editor exports it, for example `{\"token\": {\"colorPrimary\": \"#1f77b4\"}}`.",
				Required:    true,
				CustomType:  jsonDocument,
			},
			"system_default": schema.BoolAttribute{
				Description: "Whether the theme is the default theme of the instance. Only one theme is, so setting it on a theme takes the place of the previous one. Defaults to `false`.",
				Optional:    true,
				Computed:    true,
				Default:     booldefault.StaticBool(false),
			},
			"system_dark": schema.BoolAttribute{
				Description: "Whether the theme is the dark theme of the instance. Only one theme is. Defaults to `false`.",
				Optional:    true,
				Computed:    true,
				Default:     booldefault.StaticBool(false),
			},
			"uuid": schema.StringAttribute{
				Description: "UUID of the theme, as export bundles reference it.",
				Computed:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
		},
	}
}

// Create creates the theme, makes it a system theme when asked, and sets the initial state.
func (r *themeResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan themeResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	id, err := r.client.CreateTheme(client.Theme{Name: plan.Name.ValueString(), JSONData: plan.JSONData.ValueString()})
	if err != nil {
		resp.Diagnostics.AddError("Unable to Create Superset Theme", err.Error())
		return
	}

	resp.Diagnostics.Append(r.applySystem(id, plan, themeResourceModel{SystemDefault: types.BoolValue(false), SystemDark: types.BoolValue(false)})...)
	r.readInto(ctx, id, &resp.State, &resp.Diagnostics, "Unable to Read Superset Theme After Creation")
}

// Read refreshes the Terraform state with the theme as it is in Superset.
func (r *themeResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state themeResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	theme, err := r.client.GetTheme(state.ID.ValueInt64())
	if errors.Is(err, client.ErrNotFound) {
		tflog.Warn(ctx, "theme no longer exists in Superset, removing it from the state", map[string]any{"id": state.ID.ValueInt64()})
		resp.State.RemoveResource(ctx)
		return
	}
	if err != nil {
		resp.Diagnostics.AddError("Unable to Read Superset Theme", err.Error())
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, themeModel(theme))...)
}

// Update replaces the name and the tokens of the theme, and its places as a system theme.
func (r *themeResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state themeResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	if err := r.client.UpdateTheme(state.ID.ValueInt64(), client.Theme{Name: plan.Name.ValueString(), JSONData: plan.JSONData.ValueString()}); err != nil {
		resp.Diagnostics.AddError("Unable to Update Superset Theme", err.Error())
		return
	}

	resp.Diagnostics.Append(r.applySystem(state.ID.ValueInt64(), plan, state)...)
	r.readInto(ctx, state.ID.ValueInt64(), &resp.State, &resp.Diagnostics, "Unable to Read Superset Theme After Update")
}

// Delete gives up the places of the theme as a system theme, which Superset requires, and
// deletes it; a theme that is already gone is not an error.
func (r *themeResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state themeResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	none := themeResourceModel{SystemDefault: types.BoolValue(false), SystemDark: types.BoolValue(false)}
	resp.Diagnostics.Append(r.applySystem(state.ID.ValueInt64(), none, state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	err := r.client.DeleteTheme(state.ID.ValueInt64())
	if err != nil && !errors.Is(err, client.ErrNotFound) {
		resp.Diagnostics.AddError("Unable to Delete Superset Theme", err.Error())
		return
	}
}

// ImportState imports a theme by its numeric id.
func (r *themeResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	id, err := strconv.ParseInt(req.ID, 10, 64)
	if err != nil {
		resp.Diagnostics.AddError("Invalid Import ID", fmt.Sprintf("The import ID %q is not a numeric theme id.", req.ID))
		return
	}
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("id"), id)...)
}

// Configure adds the provider configured client to the resource.
func (r *themeResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

// applySystem takes or gives up the places of the theme as the default and the dark theme of
// the instance, when the plan differs from the state. A place is only given up when the theme
// still holds it, so another theme that took it is left alone.
func (r *themeResource) applySystem(id int64, plan, state themeResourceModel) diag.Diagnostics {
	var diags diag.Diagnostics
	for _, place := range []struct {
		dark    bool
		planned types.Bool
		prior   types.Bool
	}{
		{dark: false, planned: plan.SystemDefault, prior: state.SystemDefault},
		{dark: true, planned: plan.SystemDark, prior: state.SystemDark},
	} {
		switch {
		case place.planned.ValueBool() && !place.prior.ValueBool():
			if err := r.client.SetSystemTheme(id, place.dark); err != nil {
				diags.AddError("Unable to Make the Superset Theme a System Theme", err.Error())
				return diags
			}
		case !place.planned.ValueBool() && place.prior.ValueBool():
			current, err := r.client.GetTheme(id)
			if err != nil {
				diags.AddError("Unable to Read Superset Theme", err.Error())
				return diags
			}
			if (place.dark && current.SystemDark) || (!place.dark && current.SystemDefault) {
				if err := r.client.UnsetSystemTheme(place.dark); err != nil {
					diags.AddError("Unable to Unset the Superset System Theme", err.Error())
					return diags
				}
			}
		}
	}
	return diags
}

// readInto fetches the theme and stores it in the state after a create or an update.
func (r *themeResource) readInto(ctx context.Context, id int64, state stateSetter, diags *diag.Diagnostics, summary string) {
	theme, err := r.client.GetTheme(id)
	if err != nil {
		diags.AddError(summary, err.Error())
		return
	}
	diags.Append(state.Set(ctx, themeModel(theme))...)
}

func themeModel(theme *client.Theme) *themeResourceModel {
	return &themeResourceModel{
		ID:            types.Int64Value(theme.ID),
		Name:          types.StringValue(theme.Name),
		JSONData:      jsonDocument.value(theme.JSONData),
		SystemDefault: types.BoolValue(theme.SystemDefault),
		SystemDark:    types.BoolValue(theme.SystemDark),
		UUID:          optionalString(theme.UUID),
	}
}
