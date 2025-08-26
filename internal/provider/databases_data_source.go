package provider

import (
	"context"
	"fmt"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
	"terraform-provider-superset/internal/client"
)

// Ensure the implementation satisfies the expected interfaces.
var (
	_ datasource.DataSource              = &databasesDataSource{}
	_ datasource.DataSourceWithConfigure = &databasesDataSource{}
)

// NewDatabasesDataSource is a helper function to simplify the provider implementation.
func NewDatabasesDataSource() datasource.DataSource {
	return &databasesDataSource{}
}

// databasesDataSource is the data source implementation.
type databasesDataSource struct {
	client *client.Client
}

// databasesDataSourceModel maps the data source schema data.
type databasesDataSourceModel struct {
	Databases []databaseModel `tfsdk:"databases"`
}

// databaseModel maps the database schema data.
type databaseModel struct {
	ID           types.Int64  `tfsdk:"id"`
	DatabaseName types.String `tfsdk:"database_name"`
}

// Metadata returns the data source type name.
func (d *databasesDataSource) Metadata(ctx context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	tflog.Debug(ctx, "Starting Metadata method")
	resp.TypeName = req.ProviderTypeName + "_databases"
	tflog.Debug(ctx, "Completed Metadata method", map[string]interface{}{
		"type_name": resp.TypeName,
	})
}

// Schema defines the schema for the data source.
func (d *databasesDataSource) Schema(ctx context.Context, req datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	tflog.Debug(ctx, "Starting Schema method")
	resp.Schema = schema.Schema{
		Description: "Fetches the list of databases from Superset.",
		Attributes: map[string]schema.Attribute{
			"databases": schema.ListNestedAttribute{
				Description: "List of databases.",
				Computed:    true,
				NestedObject: schema.NestedAttributeObject{
					Attributes: map[string]schema.Attribute{
						"id": schema.Int64Attribute{
							Description: "Numeric identifier of the database.",
							Computed:    true,
						},
						"database_name": schema.StringAttribute{
							Description: "Name of the database.",
							Computed:    true,
						},
					},
				},
			},
		},
	}
	tflog.Debug(ctx, "Completed Schema method")
}

// Read refreshes the Terraform state with the latest data.
func (d *databasesDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	tflog.Debug(ctx, "Starting Read method")

	var state databasesDataSourceModel

	dbInfosRaw, err := d.client.GetAllDatabases()
	if err != nil {
		tflog.Error(ctx, "Error fetching databases", map[string]interface{}{
			"error": err.Error(),
		})
		resp.Diagnostics.AddError(
			"Unable to Read Superset Databases",
			err.Error(),
		)
		return
	}

	for _, db := range dbInfosRaw {
		tflog.Debug(ctx, "Processing database", map[string]interface{}{
			"database": db,
		})

		// Use type assertion to handle int64 type conversion
		id, ok := db["id"].(int64)
		if !ok {
			if floatID, ok := db["id"].(float64); ok {
				id = int64(floatID)
			} else {
				tflog.Error(ctx, "Type assertion error for database ID", map[string]interface{}{
					"database_id_type": fmt.Sprintf("%T", db["id"]),
				})
				resp.Diagnostics.AddError(
					"Type Assertion Error",
					fmt.Sprintf("Expected int64 or float64 for database ID, got: %T", db["id"]),
				)
				return
			}
		}

		name, ok := db["database_name"].(string)
		if !ok {
			tflog.Error(ctx, "Type assertion error for database name", map[string]interface{}{
				"database_name_type": fmt.Sprintf("%T", db["database_name"]),
			})
			resp.Diagnostics.AddError(
				"Type Assertion Error",
				fmt.Sprintf("Expected string for database name, got: %T", db["database_name"]),
			)
			return
		}

		state.Databases = append(state.Databases, databaseModel{
			ID:           types.Int64Value(id),
			DatabaseName: types.StringValue(name),
		})
	}

	diags := resp.State.Set(ctx, &state)
	resp.Diagnostics.Append(diags...)

	tflog.Debug(ctx, "Completed Read method")
}

// Configure adds the provider configured client to the data source.
func (d *databasesDataSource) Configure(ctx context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	tflog.Debug(ctx, "Starting Configure method")
	if req.ProviderData == nil {
		tflog.Debug(ctx, "No provider data received")
		return
	}

	client, ok := req.ProviderData.(*client.Client)
	if !ok {
		tflog.Error(ctx, "Unexpected Data Source Configure Type", map[string]interface{}{
			"expected": "*client.Client",
			"got":      fmt.Sprintf("%T", req.ProviderData),
		})
		resp.Diagnostics.AddError(
			"Unexpected Data Source Configure Type",
			fmt.Sprintf("Expected *client.Client, got: %T. Please report this issue to the provider developers.", req.ProviderData),
		)
		return
	}

	d.client = client
	tflog.Debug(ctx, "Completed Configure method")
}
