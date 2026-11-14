package provider

import (
	"context"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/platacard/terraform-provider-superset/internal/client"
)

var (
	_ datasource.DataSource              = &databaseCatalogsDataSource{}
	_ datasource.DataSourceWithConfigure = &databaseCatalogsDataSource{}
)

// NewDatabaseCatalogsDataSource is a helper function to simplify the provider implementation.
func NewDatabaseCatalogsDataSource() datasource.DataSource {
	return &databaseCatalogsDataSource{}
}

// databaseCatalogsDataSource lists the catalogs of a database connection, on Superset 6.
type databaseCatalogsDataSource struct {
	client *client.Client
}

type databaseCatalogsDataSourceModel struct {
	DatabaseName types.String `tfsdk:"database_name"`
	Force        types.Bool   `tfsdk:"force"`
	DatabaseID   types.Int64  `tfsdk:"database_id"`
	Catalogs     types.List   `tfsdk:"catalogs"`
}

func (d *databaseCatalogsDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_database_catalogs"
}

func (d *databaseCatalogsDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Lists the catalogs Superset sees in a database connection, on Superset 6, for engines with several catalogs such as PostgreSQL databases or Trino catalogs, for example to list the schemas of each of them with `superset_database_schemas`.",
		Attributes: map[string]schema.Attribute{
			"database_name": schema.StringAttribute{
				Description: "Name of the database connection.",
				Required:    true,
			},
			"force": schema.BoolAttribute{
				Description: "Whether Superset reads the catalogs from the database instead of its cache. Defaults to `false`.",
				Optional:    true,
			},
			"database_id": schema.Int64Attribute{
				Description: "Numeric identifier of the connection.",
				Computed:    true,
			},
			"catalogs": schema.ListAttribute{
				Description: "Names of the catalogs, sorted.",
				ElementType: types.StringType,
				Computed:    true,
			},
		},
	}
}

func (d *databaseCatalogsDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var config databaseCatalogsDataSourceModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &config)...)
	if resp.Diagnostics.HasError() {
		return
	}

	databaseID, ok := databaseIDByName(d.client, config.DatabaseName.ValueString(), &resp.Diagnostics)
	if !ok {
		return
	}
	catalogs, err := d.client.DatabaseCatalogs(databaseID, config.Force.ValueBool())
	if err != nil {
		resp.Diagnostics.AddError("Unable to List the Catalogs of the Superset Database", err.Error())
		return
	}

	config.DatabaseID = types.Int64Value(databaseID)
	var diags diag.Diagnostics
	config.Catalogs, diags = types.ListValueFrom(ctx, types.StringType, nonNilStrings(catalogs))
	resp.Diagnostics.Append(diags...)
	resp.Diagnostics.Append(resp.State.Set(ctx, &config)...)
}

func (d *databaseCatalogsDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	d.client = dataSourceClient(req, &resp.Diagnostics)
}
