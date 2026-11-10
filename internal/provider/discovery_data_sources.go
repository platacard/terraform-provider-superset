package provider

import (
	"context"
	"fmt"

	"github.com/hashicorp/terraform-plugin-framework/attr"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/platacard/terraform-provider-superset/internal/client"
)

// The discovery data sources ask Superset what it sees in a database connection: its schemas,
// the tables of a schema and the columns of a table. They look the connection up by name and
// fail when it does not exist.

// databaseIDByName looks a database connection up for a discovery data source.
func databaseIDByName(c *client.Client, name string, diags *diag.Diagnostics) (int64, bool) {
	if name == "" {
		diags.AddAttributeError(path.Root("database_name"), "Missing Database Name", "The database name must not be empty.")
		return 0, false
	}
	database, err := c.FindDatabaseByName(name)
	if err != nil {
		diags.AddAttributeError(path.Root("database_name"), "Unable to Find the Database", err.Error())
		return 0, false
	}
	return database.ID, true
}

// Schemas

var (
	_ datasource.DataSource              = &databaseSchemasDataSource{}
	_ datasource.DataSourceWithConfigure = &databaseSchemasDataSource{}
)

// NewDatabaseSchemasDataSource is a helper function to simplify the provider implementation.
func NewDatabaseSchemasDataSource() datasource.DataSource {
	return &databaseSchemasDataSource{}
}

// databaseSchemasDataSource lists the schemas of a database connection.
type databaseSchemasDataSource struct {
	client *client.Client
}

type databaseSchemasDataSourceModel struct {
	DatabaseName types.String `tfsdk:"database_name"`
	DatabaseID   types.Int64  `tfsdk:"database_id"`
	Schemas      types.List   `tfsdk:"schemas"`
}

func (d *databaseSchemasDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_database_schemas"
}

func (d *databaseSchemasDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Lists the schemas Superset sees in a database connection, for example to create a dataset for every table of a schema that exists.",
		Attributes: map[string]schema.Attribute{
			"database_name": schema.StringAttribute{
				Description: "Name of the database connection.",
				Required:    true,
			},
			"database_id": schema.Int64Attribute{
				Description: "Numeric identifier of the connection.",
				Computed:    true,
			},
			"schemas": schema.ListAttribute{
				Description: "Names of the schemas, sorted.",
				ElementType: types.StringType,
				Computed:    true,
			},
		},
	}
}

func (d *databaseSchemasDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var config databaseSchemasDataSourceModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &config)...)
	if resp.Diagnostics.HasError() {
		return
	}

	databaseID, ok := databaseIDByName(d.client, config.DatabaseName.ValueString(), &resp.Diagnostics)
	if !ok {
		return
	}
	schemas, err := d.client.DatabaseSchemas(databaseID)
	if err != nil {
		resp.Diagnostics.AddError("Unable to List the Schemas of the Superset Database", err.Error())
		return
	}

	config.DatabaseID = types.Int64Value(databaseID)
	var diags diag.Diagnostics
	config.Schemas, diags = types.ListValueFrom(ctx, types.StringType, nonNilStrings(schemas))
	resp.Diagnostics.Append(diags...)
	resp.Diagnostics.Append(resp.State.Set(ctx, &config)...)
}

func (d *databaseSchemasDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	d.client = dataSourceClient(req, &resp.Diagnostics)
}

// Tables

var (
	_ datasource.DataSource              = &databaseTablesDataSource{}
	_ datasource.DataSourceWithConfigure = &databaseTablesDataSource{}
)

// NewDatabaseTablesDataSource is a helper function to simplify the provider implementation.
func NewDatabaseTablesDataSource() datasource.DataSource {
	return &databaseTablesDataSource{}
}

// databaseTablesDataSource lists the tables and views of a schema.
type databaseTablesDataSource struct {
	client *client.Client
}

type databaseTablesDataSourceModel struct {
	DatabaseName types.String `tfsdk:"database_name"`
	Schema       types.String `tfsdk:"schema"`
	DatabaseID   types.Int64  `tfsdk:"database_id"`
	Tables       types.List   `tfsdk:"tables"`
}

type databaseTableModel struct {
	Name types.String `tfsdk:"name"`
	Type types.String `tfsdk:"type"`
}

var databaseTableAttributeTypes = map[string]attr.Type{
	"name": types.StringType,
	"type": types.StringType,
}

func (d *databaseTablesDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_database_tables"
}

func (d *databaseTablesDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Lists the tables and views Superset sees in a schema of a database connection, for example to create a dataset for each of them.",
		Attributes: map[string]schema.Attribute{
			"database_name": schema.StringAttribute{
				Description: "Name of the database connection.",
				Required:    true,
			},
			"schema": schema.StringAttribute{
				Description: "Name of the schema.",
				Required:    true,
			},
			"database_id": schema.Int64Attribute{
				Description: "Numeric identifier of the connection.",
				Computed:    true,
			},
			"tables": schema.ListNestedAttribute{
				Description: "Tables and views of the schema, sorted by name.",
				Computed:    true,
				NestedObject: schema.NestedAttributeObject{
					Attributes: map[string]schema.Attribute{
						"name": schema.StringAttribute{
							Description: "Name of the table or view.",
							Computed:    true,
						},
						"type": schema.StringAttribute{
							Description: "`table` or `view`.",
							Computed:    true,
						},
					},
				},
			},
		},
	}
}

func (d *databaseTablesDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var config databaseTablesDataSourceModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &config)...)
	if resp.Diagnostics.HasError() {
		return
	}

	databaseID, ok := databaseIDByName(d.client, config.DatabaseName.ValueString(), &resp.Diagnostics)
	if !ok {
		return
	}
	if config.Schema.ValueString() == "" {
		resp.Diagnostics.AddAttributeError(path.Root("schema"), "Missing Schema", "The schema must not be empty.")
		return
	}
	tables, err := d.client.DatabaseTables(databaseID, config.Schema.ValueString())
	if err != nil {
		resp.Diagnostics.AddError("Unable to List the Tables of the Superset Database", err.Error())
		return
	}

	models := make([]databaseTableModel, 0, len(tables))
	for _, table := range tables {
		models = append(models, databaseTableModel{Name: types.StringValue(table.Name), Type: types.StringValue(table.Type)})
	}
	config.DatabaseID = types.Int64Value(databaseID)
	var diags diag.Diagnostics
	config.Tables, diags = types.ListValueFrom(ctx, types.ObjectType{AttrTypes: databaseTableAttributeTypes}, models)
	resp.Diagnostics.Append(diags...)
	resp.Diagnostics.Append(resp.State.Set(ctx, &config)...)
}

func (d *databaseTablesDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	d.client = dataSourceClient(req, &resp.Diagnostics)
}

// Table

var (
	_ datasource.DataSource              = &databaseTableDataSource{}
	_ datasource.DataSourceWithConfigure = &databaseTableDataSource{}
)

// NewDatabaseTableDataSource is a helper function to simplify the provider implementation.
func NewDatabaseTableDataSource() datasource.DataSource {
	return &databaseTableDataSource{}
}

// databaseTableDataSource describes the columns of a table.
type databaseTableDataSource struct {
	client *client.Client
}

type databaseTableDataSourceModel struct {
	DatabaseName types.String `tfsdk:"database_name"`
	Schema       types.String `tfsdk:"schema"`
	TableName    types.String `tfsdk:"table_name"`
	DatabaseID   types.Int64  `tfsdk:"database_id"`
	Comment      types.String `tfsdk:"comment"`
	Columns      types.List   `tfsdk:"columns"`
}

type tableColumnModel struct {
	Name     types.String `tfsdk:"name"`
	Type     types.String `tfsdk:"type"`
	LongType types.String `tfsdk:"long_type"`
	Comment  types.String `tfsdk:"comment"`
}

var tableColumnAttributeTypes = map[string]attr.Type{
	"name":      types.StringType,
	"type":      types.StringType,
	"long_type": types.StringType,
	"comment":   types.StringType,
}

func (d *databaseTableDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_database_table"
}

func (d *databaseTableDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Describes a table or a view as Superset reads it from the database: its columns and their types, for example to declare the `columns` of a `superset_dataset`.",
		Attributes: map[string]schema.Attribute{
			"database_name": schema.StringAttribute{
				Description: "Name of the database connection.",
				Required:    true,
			},
			"schema": schema.StringAttribute{
				Description: "Name of the schema.",
				Required:    true,
			},
			"table_name": schema.StringAttribute{
				Description: "Name of the table or view.",
				Required:    true,
			},
			"database_id": schema.Int64Attribute{
				Description: "Numeric identifier of the connection.",
				Computed:    true,
			},
			"comment": schema.StringAttribute{
				Description: "Comment of the table in the database.",
				Computed:    true,
			},
			"columns": schema.ListNestedAttribute{
				Description: "Columns of the table, in the order of the database.",
				Computed:    true,
				NestedObject: schema.NestedAttributeObject{
					Attributes: map[string]schema.Attribute{
						"name": schema.StringAttribute{
							Description: "Name of the column.",
							Computed:    true,
						},
						"type": schema.StringAttribute{
							Description: "Generic type of the column, for example `NUMERIC`.",
							Computed:    true,
						},
						"long_type": schema.StringAttribute{
							Description: "Full type of the column, for example `NUMERIC(10, 2)`.",
							Computed:    true,
						},
						"comment": schema.StringAttribute{
							Description: "Comment of the column in the database.",
							Computed:    true,
						},
					},
				},
			},
		},
	}
}

func (d *databaseTableDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var config databaseTableDataSourceModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &config)...)
	if resp.Diagnostics.HasError() {
		return
	}

	databaseID, ok := databaseIDByName(d.client, config.DatabaseName.ValueString(), &resp.Diagnostics)
	if !ok {
		return
	}
	for name, value := range map[string]types.String{"schema": config.Schema, "table_name": config.TableName} {
		if value.ValueString() == "" {
			resp.Diagnostics.AddAttributeError(path.Root(name), "Missing Attribute", fmt.Sprintf("The %s must not be empty.", name))
			return
		}
	}
	metadata, err := d.client.DatabaseTableMetadata(databaseID, config.Schema.ValueString(), config.TableName.ValueString())
	if err != nil {
		resp.Diagnostics.AddError("Unable to Read the Superset Database Table", err.Error())
		return
	}

	columns := make([]tableColumnModel, 0, len(metadata.Columns))
	for _, column := range metadata.Columns {
		columns = append(columns, tableColumnModel{
			Name:     types.StringValue(column.Name),
			Type:     types.StringValue(column.Type),
			LongType: optionalString(column.LongType),
			Comment:  optionalString(column.Comment),
		})
	}
	config.DatabaseID = types.Int64Value(databaseID)
	config.Comment = optionalString(metadata.Comment)
	var diags diag.Diagnostics
	config.Columns, diags = types.ListValueFrom(ctx, types.ObjectType{AttrTypes: tableColumnAttributeTypes}, columns)
	resp.Diagnostics.Append(diags...)
	resp.Diagnostics.Append(resp.State.Set(ctx, &config)...)
}

func (d *databaseTableDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	d.client = dataSourceClient(req, &resp.Diagnostics)
}
