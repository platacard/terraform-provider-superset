package provider

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/hashicorp/terraform-plugin-framework/attr"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"terraform-provider-superset/internal/client"
)

var (
	_ datasource.DataSource              = &sqlQueryDataSource{}
	_ datasource.DataSourceWithConfigure = &sqlQueryDataSource{}
)

// NewSQLQueryDataSource is a helper function to simplify the provider implementation.
func NewSQLQueryDataSource() datasource.DataSource {
	return &sqlQueryDataSource{}
}

// sqlQueryDataSource runs a query through SQL Lab and exposes its rows.
type sqlQueryDataSource struct {
	client *client.Client
}

// defaultSQLQueryLimit caps the rows of a query whose configuration sets no limit; a data
// source runs on every plan, so its result is kept small by default.
const defaultSQLQueryLimit = 1000

type sqlQueryDataSourceModel struct {
	DatabaseName   types.String `tfsdk:"database_name"`
	DatabaseID     types.Int64  `tfsdk:"database_id"`
	SQL            types.String `tfsdk:"sql"`
	Schema         types.String `tfsdk:"schema"`
	Catalog        types.String `tfsdk:"catalog"`
	Limit          types.Int64  `tfsdk:"limit"`
	TemplateParams types.String `tfsdk:"template_params"`
	Columns        types.List   `tfsdk:"columns"`
	Rows           types.List   `tfsdk:"rows"`
	RowsJSON       types.String `tfsdk:"rows_json"`
	RowCount       types.Int64  `tfsdk:"row_count"`
	Limited        types.Bool   `tfsdk:"limited"`
}

type sqlColumnModel struct {
	Name       types.String `tfsdk:"name"`
	Type       types.String `tfsdk:"type"`
	IsDateTime types.Bool   `tfsdk:"is_datetime"`
}

var sqlColumnAttributeTypes = map[string]attr.Type{
	"name":        types.StringType,
	"type":        types.StringType,
	"is_datetime": types.BoolType,
}

var sqlRowType = types.MapType{ElemType: types.StringType}

func (d *sqlQueryDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_sql_query"
}

func (d *sqlQueryDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Runs a SQL query through SQL Lab on a database connection and returns its rows, for example to create a dataset, a role or a user per row of a table. The query runs on every plan and refresh, so keep it cheap. The connection must be exposed in SQL Lab (`expose_in_sqllab`), a query that changes data needs DML allowed on it (`allow_dml`), and the user of the provider needs access to the database in SQL Lab.",
		Attributes: map[string]schema.Attribute{
			"database_name": schema.StringAttribute{
				Description: "Name of the database connection to run the query on. Exactly one of `database_name` and `database_id` must be set.",
				Optional:    true,
			},
			"database_id": schema.Int64Attribute{
				Description: "Numeric identifier of the connection, looked up from `database_name` when that is set.",
				Optional:    true,
				Computed:    true,
			},
			"sql": schema.StringAttribute{
				Description: "The query. Superset appends its own `LIMIT`; with several statements only the result of the last one is returned.",
				Required:    true,
			},
			"schema": schema.StringAttribute{
				Description: "Schema the unqualified table names of the query are resolved in.",
				Optional:    true,
			},
			"catalog": schema.StringAttribute{
				Description: "Catalog (database, project or similar depending on the engine) to run in, on Superset 6 connections with several catalogs. Left to the default of the connection when unset.",
				Optional:    true,
			},
			"limit": schema.Int64Attribute{
				Description: "Maximum number of rows returned. Defaults to 1000; Superset caps it at its `SQL_MAX_ROW`.",
				Optional:    true,
				Computed:    true,
			},
			"template_params": schema.StringAttribute{
				Description: "JSON object with the values of the Jinja parameters of the query, for example `jsonencode({ region = \"north\" })` for `{{ region }}`; needs the `ENABLE_TEMPLATE_PROCESSING` feature flag.",
				Optional:    true,
			},
			"columns": schema.ListNestedAttribute{
				Description: "Columns of the result, in the order of the query. Superset 3.1 returns none for a query without rows.",
				Computed:    true,
				NestedObject: schema.NestedAttributeObject{
					Attributes: map[string]schema.Attribute{
						"name": schema.StringAttribute{
							Description: "Name of the column; duplicate names get a `__1`, `__2` suffix.",
							Computed:    true,
						},
						"type": schema.StringAttribute{
							Description: "Generic type of the column as Superset names it, for example `INTEGER` or `STRING`.",
							Computed:    true,
						},
						"is_datetime": schema.BoolAttribute{
							Description: "Whether the column holds dates or timestamps.",
							Computed:    true,
						},
					},
				},
			},
			"rows": schema.ListAttribute{
				Description: "Rows of the result, each a map from the column name to the value as text: numbers and booleans as Superset wrote them, dates in ISO 8601, and null values left out of the map.",
				ElementType: sqlRowType,
				Computed:    true,
			},
			"rows_json": schema.StringAttribute{
				Description: "The rows as Superset serialised them, a JSON array of objects, for `jsondecode` when the types of the values matter.",
				Computed:    true,
			},
			"row_count": schema.Int64Attribute{
				Description: "Number of rows returned.",
				Computed:    true,
			},
			"limited": schema.BoolAttribute{
				Description: "Whether the rows were cut by `limit` or by the cap of Superset.",
				Computed:    true,
			},
		},
	}
}

func (d *sqlQueryDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var config sqlQueryDataSourceModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &config)...)
	if resp.Diagnostics.HasError() {
		return
	}

	query, ok := d.queryFromModel(config, &resp.Diagnostics)
	if !ok {
		return
	}
	result, err := d.client.ExecuteSQL(query)
	if err != nil {
		resp.Diagnostics.AddError("Unable to Run the SQL Query in Superset", err.Error())
		return
	}

	config.DatabaseID = types.Int64Value(query.DatabaseID)
	config.Limit = types.Int64Value(query.Limit)
	config.RowsJSON = types.StringValue(string(result.RawRows))
	config.RowCount = types.Int64Value(int64(len(result.Rows)))
	config.Limited = types.BoolValue(result.Limited)
	var diags diag.Diagnostics
	config.Columns, diags = sqlColumnsValue(ctx, result.Columns)
	resp.Diagnostics.Append(diags...)
	config.Rows, diags = sqlRowsValue(ctx, result.Rows)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &config)...)
}

// queryFromModel builds the query of the configuration, looking the connection up by name
// when the configuration names it.
func (d *sqlQueryDataSource) queryFromModel(config sqlQueryDataSourceModel, diags *diag.Diagnostics) (client.SQLQuery, bool) {
	query := client.SQLQuery{
		SQL:            config.SQL.ValueString(),
		Schema:         config.Schema.ValueString(),
		Catalog:        config.Catalog.ValueString(),
		Limit:          defaultSQLQueryLimit,
		TemplateParams: config.TemplateParams.ValueString(),
	}
	switch {
	case !config.DatabaseName.IsNull() && !config.DatabaseID.IsNull():
		diags.AddAttributeError(path.Root("database_id"), "Conflicting Attributes", "Set either database_name or database_id, not both.")
		return query, false
	case !config.DatabaseName.IsNull():
		id, ok := databaseIDByName(d.client, config.DatabaseName.ValueString(), diags)
		if !ok {
			return query, false
		}
		query.DatabaseID = id
	case !config.DatabaseID.IsNull():
		query.DatabaseID = config.DatabaseID.ValueInt64()
	default:
		diags.AddAttributeError(path.Root("database_name"), "Missing Database", "Set database_name or database_id.")
		return query, false
	}
	if query.SQL == "" {
		diags.AddAttributeError(path.Root("sql"), "Missing SQL", "The sql must not be empty.")
		return query, false
	}
	if !config.Limit.IsNull() {
		query.Limit = config.Limit.ValueInt64()
		if query.Limit < 1 {
			diags.AddAttributeError(path.Root("limit"), "Invalid Limit", "The limit must be at least 1.")
			return query, false
		}
	}
	if query.TemplateParams != "" {
		var params map[string]json.RawMessage
		if err := json.Unmarshal([]byte(query.TemplateParams), &params); err != nil {
			diags.AddAttributeError(path.Root("template_params"), "Invalid Template Parameters", fmt.Sprintf("The template parameters must be a JSON object: %s.", err))
			return query, false
		}
	}
	return query, true
}

// sqlColumnsValue maps the columns of a result to the state.
func sqlColumnsValue(ctx context.Context, columns []client.SQLColumn) (types.List, diag.Diagnostics) {
	models := make([]sqlColumnModel, 0, len(columns))
	for _, column := range columns {
		models = append(models, sqlColumnModel{
			Name:       types.StringValue(column.Name),
			Type:       types.StringValue(column.Type),
			IsDateTime: types.BoolValue(column.IsDateTime),
		})
	}
	return types.ListValueFrom(ctx, types.ObjectType{AttrTypes: sqlColumnAttributeTypes}, models)
}

// sqlRowsValue maps the rows of a result to the state: one map of texts per row, without
// the null values.
func sqlRowsValue(ctx context.Context, rows []map[string]json.RawMessage) (types.List, diag.Diagnostics) {
	var diags diag.Diagnostics
	values := make([]attr.Value, 0, len(rows))
	for _, row := range rows {
		texts := make(map[string]string, len(row))
		for name, value := range row {
			if text, ok := client.SQLValueText(value); ok {
				texts[name] = text
			}
		}
		value, d := types.MapValueFrom(ctx, types.StringType, texts)
		diags.Append(d...)
		values = append(values, value)
	}
	list, d := types.ListValue(sqlRowType, values)
	diags.Append(d...)
	return list, diags
}

func (d *sqlQueryDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	d.client = dataSourceClient(req, &resp.Diagnostics)
}
