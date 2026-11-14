package provider

import (
	"context"
	"fmt"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/platacard/terraform-provider-superset/internal/client"
)

var (
	_ datasource.DataSource              = &databaseEnginesDataSource{}
	_ datasource.DataSourceWithConfigure = &databaseEnginesDataSource{}
)

// NewDatabaseEnginesDataSource is a helper function to simplify the provider implementation.
func NewDatabaseEnginesDataSource() datasource.DataSource {
	return &databaseEnginesDataSource{}
}

// databaseEnginesDataSource lists the database engines the instance can connect to.
type databaseEnginesDataSource struct {
	client *client.Client
}

type databaseEnginesDataSourceModel struct {
	Engine  types.String         `tfsdk:"engine"`
	Engines []databaseEngineItem `tfsdk:"engines"`
}

type databaseEngineItem struct {
	Name                     types.String `tfsdk:"name"`
	Engine                   types.String `tfsdk:"engine"`
	DefaultDriver            types.String `tfsdk:"default_driver"`
	AvailableDrivers         types.List   `tfsdk:"available_drivers"`
	SQLAlchemyURIPlaceholder types.String `tfsdk:"sqlalchemy_uri_placeholder"`
	Preferred                types.Bool   `tfsdk:"preferred"`
	SupportsFileUpload       types.Bool   `tfsdk:"supports_file_upload"`
	SupportsSSHTunneling     types.Bool   `tfsdk:"supports_ssh_tunneling"`
}

func (d *databaseEnginesDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_database_engines"
}

func (d *databaseEnginesDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Lists the database engines the Superset instance can connect to: the dialects whose drivers are installed, with the shape of their connection URI, for example to check that a driver is available before creating a `superset_database` connection. Preferred engines come first, then the others by name.",
		Attributes: map[string]schema.Attribute{
			"engine": schema.StringAttribute{
				Description: "Keep the entries of this SQLAlchemy dialect only, for example `postgresql`.",
				Optional:    true,
			},
			"engines": schema.ListNestedAttribute{
				Description: "The engines.",
				Computed:    true,
				NestedObject: schema.NestedAttributeObject{
					Attributes: map[string]schema.Attribute{
						"name": schema.StringAttribute{
							Description: "Name of the engine as the UI shows it, for example `PostgreSQL`.",
							Computed:    true,
						},
						"engine": schema.StringAttribute{
							Description: "SQLAlchemy dialect, the scheme of the connection URI.",
							Computed:    true,
						},
						"default_driver": schema.StringAttribute{
							Description: "Driver Superset uses when the URI names none; null when the engine has no default.",
							Computed:    true,
						},
						"available_drivers": schema.ListAttribute{
							Description: "Drivers installed for the engine.",
							Computed:    true,
							ElementType: types.StringType,
						},
						"sqlalchemy_uri_placeholder": schema.StringAttribute{
							Description: "Shape of the connection URI of the engine.",
							Computed:    true,
						},
						"preferred": schema.BoolAttribute{
							Description: "Whether the UI lists the engine among the preferred ones.",
							Computed:    true,
						},
						"supports_file_upload": schema.BoolAttribute{
							Description: "Whether files can be uploaded into a connection of the engine.",
							Computed:    true,
						},
						"supports_ssh_tunneling": schema.BoolAttribute{
							Description: "Whether a connection of the engine can go through an SSH tunnel.",
							Computed:    true,
						},
					},
				},
			},
		},
	}
}

func (d *databaseEnginesDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var config databaseEnginesDataSourceModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &config)...)
	if resp.Diagnostics.HasError() {
		return
	}

	engines, err := d.client.ListDatabaseEngines()
	if err != nil {
		resp.Diagnostics.AddError("Unable to List the Superset Database Engines", fmt.Sprintf("Could not list the database engines: %s", err))
		return
	}

	only := config.Engine.ValueString()
	config.Engines = make([]databaseEngineItem, 0, len(engines))
	for _, engine := range engines {
		if only != "" && engine.Engine != only {
			continue
		}
		drivers, diags := types.ListValueFrom(ctx, types.StringType, nonNilStrings(engine.AvailableDrivers))
		resp.Diagnostics.Append(diags...)
		if resp.Diagnostics.HasError() {
			return
		}
		config.Engines = append(config.Engines, databaseEngineItem{
			Name:                     types.StringValue(engine.Name),
			Engine:                   types.StringValue(engine.Engine),
			DefaultDriver:            optionalString(engine.DefaultDriver),
			AvailableDrivers:         drivers,
			SQLAlchemyURIPlaceholder: optionalString(engine.SQLAlchemyURIPlaceholder),
			Preferred:                types.BoolValue(engine.Preferred),
			SupportsFileUpload:       types.BoolValue(engine.Information.SupportsFileUpload),
			SupportsSSHTunneling:     types.BoolValue(!engine.Information.DisableSSHTunneling),
		})
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &config)...)
}

func (d *databaseEnginesDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	d.client = dataSourceClient(req, &resp.Diagnostics)
}
