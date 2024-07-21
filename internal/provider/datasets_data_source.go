package provider

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/hashicorp/terraform-plugin-framework/attr"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
	"terraform-provider-superset/internal/client"
)

var (
	_ datasource.DataSource              = &datasetsDataSource{}
	_ datasource.DataSourceWithConfigure = &datasetsDataSource{}
)

func NewDatasetsDataSource() datasource.DataSource {
	return &datasetsDataSource{}
}

type datasetsDataSource struct {
	client *client.Client
}

type datasetsDataSourceModel struct {
	Datasets []dataset `tfsdk:"datasets"`
}

type dataset struct {
	ID           types.Int64  `tfsdk:"id"`
	TableName    types.String `tfsdk:"table_name"`
	DatabaseID   types.Int64  `tfsdk:"database_id"`
	DatabaseName types.String `tfsdk:"database_name"`
	Schema       types.String `tfsdk:"schema"`
	SQL          types.String `tfsdk:"sql"`
	Kind         types.String `tfsdk:"kind"`
	Owners       types.List   `tfsdk:"owners"`
}

func (d *datasetsDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_datasets"
}

func (d *datasetsDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Fetches all datasets from Superset.",
		Attributes: map[string]schema.Attribute{
			"datasets": schema.ListNestedAttribute{
				Description: "List of Superset datasets.",
				Computed:    true,
				NestedObject: schema.NestedAttributeObject{
					Attributes: map[string]schema.Attribute{
						"id": schema.Int64Attribute{
							Description: "Dataset ID.",
							Computed:    true,
						},
						"table_name": schema.StringAttribute{
							Description: "Name of the table.",
							Computed:    true,
						},
						"database_id": schema.Int64Attribute{
							Description: "Database ID to which the dataset belongs.",
							Computed:    true,
						},
						"database_name": schema.StringAttribute{
							Description: "Database name to which the dataset belongs.",
							Computed:    true,
						},
						"schema": schema.StringAttribute{
							Description: "Schema of the dataset.",
							Computed:    true,
						},
						"sql": schema.StringAttribute{
							Description: "SQL query of the dataset.",
							Computed:    true,
						},
						"kind": schema.StringAttribute{
							Description: "Kind of the dataset.",
							Computed:    true,
						},
						"owners": schema.ListNestedAttribute{
							Description: "List of owners of the dataset.",
							Computed:    true,
							NestedObject: schema.NestedAttributeObject{
								Attributes: map[string]schema.Attribute{
									"id": schema.Int64Attribute{
										Description: "Owner ID.",
										Computed:    true,
									},
									"first_name": schema.StringAttribute{
										Description: "First name of the owner.",
										Computed:    true,
									},
									"last_name": schema.StringAttribute{
										Description: "Last name of the owner.",
										Computed:    true,
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func (d *datasetsDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	tflog.Debug(ctx, "Starting Read method")

	// Fetch datasets from the Superset instance
	datasets, err := d.client.GetAllDatasets()
	if err != nil {
		resp.Diagnostics.AddError(
			"Unable to Read Superset Datasets",
			fmt.Sprintf("GetAllDatasets failed: %s", err.Error()),
		)
		return
	}

	// Log the entire API response for debugging
	responseJSON, _ := json.Marshal(datasets)
	tflog.Debug(ctx, fmt.Sprintf("API Response: %s", responseJSON))

	var datasetsModel []dataset
	for _, ds := range datasets {
		tflog.Debug(ctx, fmt.Sprintf("Processing dataset: %v", ds))

		id, ok := ds["id"].(float64)
		if !ok {
			resp.Diagnostics.AddError(
				"Invalid Response",
				"Missing or invalid 'id' field in the API response",
			)
			return
		}
		tflog.Debug(ctx, fmt.Sprintf("Dataset ID: %f", id))

		tableName, ok := ds["table_name"].(string)
		if !ok {
			resp.Diagnostics.AddError(
				"Invalid Response",
				"Missing or invalid 'table_name' field in the API response",
			)
			return
		}
		tflog.Debug(ctx, fmt.Sprintf("Table Name: %s", tableName))

		database, ok := ds["database"].(map[string]interface{})
		if !ok {
			resp.Diagnostics.AddError(
				"Invalid Response",
				"Missing or invalid 'database' field in the API response",
			)
			return
		}
		tflog.Debug(ctx, fmt.Sprintf("Database Field: %v", database))

		databaseID, ok := database["id"].(float64)
		if !ok {
			resp.Diagnostics.AddError(
				"Invalid Response",
				"Missing or invalid 'database.id' field in the API response",
			)
			return
		}
		tflog.Debug(ctx, fmt.Sprintf("Database ID: %f", databaseID))

		databaseName, ok := database["database_name"].(string)
		if !ok {
			resp.Diagnostics.AddError(
				"Invalid Response",
				"Missing or invalid 'database.database_name' field in the API response",
			)
			return
		}
		tflog.Debug(ctx, fmt.Sprintf("Database Name: %s", databaseName))

		schema, ok := ds["schema"].(string)
		if !ok {
			schema = ""
		}
		tflog.Debug(ctx, fmt.Sprintf("Schema: %s", schema))

		sql, ok := ds["sql"].(string)
		if !ok {
			sql = ""
		}
		tflog.Debug(ctx, fmt.Sprintf("SQL: %s", sql))

		kind, ok := ds["kind"].(string)
		if !ok {
			kind = ""
		}
		tflog.Debug(ctx, fmt.Sprintf("Kind: %s", kind))

		ownersList := []attr.Value{}
		owners, ok := ds["owners"].([]interface{})
		if ok {
			for _, owner := range owners {
				ownerMap, ok := owner.(map[string]interface{})
				if ok {
					ownerID, ok := ownerMap["id"].(float64)
					if !ok {
						ownerID = 0
					}
					firstName, ok := ownerMap["first_name"].(string)
					if !ok {
						firstName = ""
					}
					lastName, ok := ownerMap["last_name"].(string)
					if !ok {
						lastName = ""
					}

					ownerObject := map[string]attr.Value{
						"id":         types.Int64Value(int64(ownerID)),
						"first_name": types.StringValue(firstName),
						"last_name":  types.StringValue(lastName),
					}
					ownerVal, diags := types.ObjectValue(map[string]attr.Type{
						"id":         types.Int64Type,
						"first_name": types.StringType,
						"last_name":  types.StringType,
					}, ownerObject)
					resp.Diagnostics.Append(diags...)
					if resp.Diagnostics.HasError() {
						return
					}
					ownersList = append(ownersList, ownerVal)
				}
			}
		}

		ownersAttr, diags := types.ListValue(types.ObjectType{AttrTypes: map[string]attr.Type{"id": types.Int64Type, "first_name": types.StringType, "last_name": types.StringType}}, ownersList)
		resp.Diagnostics.Append(diags...)
		if resp.Diagnostics.HasError() {
			return
		}

		dataset := dataset{
			ID:           types.Int64Value(int64(id)),
			TableName:    types.StringValue(tableName),
			DatabaseID:   types.Int64Value(int64(databaseID)),
			DatabaseName: types.StringValue(databaseName),
			Schema:       types.StringValue(schema),
			SQL:          types.StringValue(sql),
			Kind:         types.StringValue(kind),
			Owners:       ownersAttr,
		}
		datasetsModel = append(datasetsModel, dataset)
	}

	state := datasetsDataSourceModel{
		Datasets: datasetsModel,
	}

	// Additional debug for the final state
	finalStateJSON, _ := json.Marshal(state)
	tflog.Debug(ctx, fmt.Sprintf("Final state to be set: %s", finalStateJSON))

	diags := resp.State.Set(ctx, &state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		tflog.Debug(ctx, fmt.Sprintf("Error setting state: %v", resp.Diagnostics))
		return
	}

	tflog.Debug(ctx, "Fetched datasets successfully", map[string]interface{}{
		"datasets_count": len(datasetsModel),
	})
}

func (d *datasetsDataSource) Configure(ctx context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
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

	d.client = client
}
