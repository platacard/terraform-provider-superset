package provider

import (
	"context"
	"fmt"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/platacard/terraform-provider-superset/internal/client"
)

// Ensure the implementation satisfies the expected interfaces.
var (
	_ datasource.DataSource              = &cssTemplateDataSource{}
	_ datasource.DataSourceWithConfigure = &cssTemplateDataSource{}
)

// NewCSSTemplateDataSource is a helper function to simplify the provider implementation.
func NewCSSTemplateDataSource() datasource.DataSource {
	return &cssTemplateDataSource{}
}

// cssTemplateDataSource looks up one CSS template by its name.
type cssTemplateDataSource struct {
	client *client.Client
}

// cssTemplateDataSourceModel maps the data source schema data.
type cssTemplateDataSourceModel struct {
	ID           types.Int64  `tfsdk:"id"`
	TemplateName types.String `tfsdk:"template_name"`
	CSS          types.String `tfsdk:"css"`
}

// Metadata returns the data source type name.
func (d *cssTemplateDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_css_template"
}

// Schema defines the schema for the data source.
func (d *cssTemplateDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Looks up a CSS template by its name. The lookup fails when no template has exactly that name.",
		Attributes: map[string]schema.Attribute{
			"template_name": schema.StringAttribute{
				Description: "Name of the template. The comparison is exact.",
				Required:    true,
			},
			"id": schema.Int64Attribute{
				Description: "Numeric identifier of the template.",
				Computed:    true,
			},
			"css": schema.StringAttribute{
				Description: "The stylesheet.",
				Computed:    true,
			},
		},
	}
}

// Read looks the template up and stores its id and CSS.
func (d *cssTemplateDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var config cssTemplateDataSourceModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &config)...)
	if resp.Diagnostics.HasError() {
		return
	}

	name := config.TemplateName.ValueString()
	if name == "" {
		resp.Diagnostics.AddAttributeError(path.Root("template_name"), "Missing Template Name", "The template name must not be empty.")
		return
	}

	template, err := d.client.FindCSSTemplateByName(name)
	if err != nil {
		resp.Diagnostics.AddError(
			"Unable to Read Superset CSS Template",
			fmt.Sprintf("Could not look up CSS template %q: %s", name, err),
		)
		return
	}

	config.ID = types.Int64Value(template.ID)
	config.CSS = types.StringValue(template.CSS)
	resp.Diagnostics.Append(resp.State.Set(ctx, &config)...)
}

// Configure adds the provider configured client to the data source.
func (d *cssTemplateDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}

	client, ok := req.ProviderData.(*client.Client)
	if !ok {
		resp.Diagnostics.AddError(
			"Unexpected Data Source Configure Type",
			fmt.Sprintf("Expected *client.Client, got: %T. Please report this issue to the provider developers.", req.ProviderData),
		)
		return
	}

	d.client = client
}
