package provider

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/booldefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/int64planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringdefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
	"github.com/platacard/terraform-provider-superset/internal/client"
)

// Ensure the implementation satisfies the expected interfaces.
var (
	_ resource.Resource              = &databaseUploadResource{}
	_ resource.ResourceWithConfigure = &databaseUploadResource{}
)

// NewDatabaseUploadResource is a helper function to simplify the provider implementation.
func NewDatabaseUploadResource() resource.Resource {
	return &databaseUploadResource{}
}

// databaseUploadResource loads a file into a table of a database connection.
type databaseUploadResource struct {
	client *client.Client
}

// databaseUploadResourceModel maps the resource schema data.
type databaseUploadResourceModel struct {
	ID               types.String `tfsdk:"id"`
	DatabaseName     types.String `tfsdk:"database_name"`
	DatabaseID       types.Int64  `tfsdk:"database_id"`
	TableName        types.String `tfsdk:"table_name"`
	Schema           types.String `tfsdk:"schema"`
	Type             types.String `tfsdk:"type"`
	File             types.String `tfsdk:"file"`
	Content          types.String `tfsdk:"content"`
	AlreadyExists    types.String `tfsdk:"already_exists"`
	Delimiter        types.String `tfsdk:"delimiter"`
	HeaderRow        types.Int64  `tfsdk:"header_row"`
	SkipRows         types.Int64  `tfsdk:"skip_rows"`
	RowsToRead       types.Int64  `tfsdk:"rows_to_read"`
	ColumnDates      types.List   `tfsdk:"column_dates"`
	ColumnDataTypes  types.String `tfsdk:"column_data_types"`
	NullValues       types.List   `tfsdk:"null_values"`
	DecimalCharacter types.String `tfsdk:"decimal_character"`
	DayFirst         types.Bool   `tfsdk:"day_first"`
	SkipBlankLines   types.Bool   `tfsdk:"skip_blank_lines"`
	SkipInitialSpace types.Bool   `tfsdk:"skip_initial_space"`
	ColumnsRead      types.List   `tfsdk:"columns_read"`
	IndexColumn      types.String `tfsdk:"index_column"`
	DataframeIndex   types.Bool   `tfsdk:"dataframe_index"`
	IndexLabel       types.String `tfsdk:"index_label"`
	SheetName        types.String `tfsdk:"sheet_name"`
	Triggers         types.Map    `tfsdk:"triggers"`
	DropOnDestroy    types.Bool   `tfsdk:"drop_on_destroy"`
	DatasetID        types.Int64  `tfsdk:"dataset_id"`
}

// Metadata returns the resource type name.
func (r *databaseUploadResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_database_upload"
}

// Schema defines the schema for the resource.
func (r *databaseUploadResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	replaceString := []planmodifier.String{stringplanmodifier.RequiresReplace()}
	resp.Schema = schema.Schema{
		Description: "Loads a CSV, Excel or columnar file into a table of a database connection through the upload endpoint of Superset 4.1 and later, which also creates a dataset for the table, for example to publish reference data next to the dashboards that use it. The connection must allow file uploads and list the schema in `schemas_allowed_for_file_upload` of its extra. The file is uploaded when the resource is created and whenever the content, the reading options, `already_exists` or `triggers` change; a file given by path is read at apply time, so pair it with a trigger such as `filemd5()` to upload it again when it changes. A table that disappears is uploaded again. Destroying the resource leaves the table and its dataset unless `drop_on_destroy` is set.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Description: "Identifier of the resource: the connection, the schema and the table.",
				Computed:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"database_name": schema.StringAttribute{
				Description:   "Name of the database connection. Exactly one of `database_name` and `database_id` must be set.",
				Optional:      true,
				PlanModifiers: replaceString,
			},
			"database_id": schema.Int64Attribute{
				Description: "Numeric identifier of the connection, looked up from `database_name` when that is set.",
				Optional:    true,
				Computed:    true,
				PlanModifiers: []planmodifier.Int64{
					int64planmodifier.RequiresReplace(),
					int64planmodifier.UseStateForUnknown(),
				},
			},
			"table_name": schema.StringAttribute{
				Description:   "Name of the table the file is loaded into.",
				Required:      true,
				PlanModifiers: replaceString,
			},
			"schema": schema.StringAttribute{
				Description:   "Schema of the table. When unset, the default schema of the connection is used and only the existence of the connection is checked, not the table.",
				Optional:      true,
				PlanModifiers: replaceString,
			},
			"type": schema.StringAttribute{
				Description:   "Kind of file: `csv`, `excel` or `columnar` (Parquet). Defaults to `csv`.",
				Optional:      true,
				Computed:      true,
				Default:       stringdefault.StaticString("csv"),
				PlanModifiers: replaceString,
			},
			"file": schema.StringAttribute{
				Description: "Path of the file to upload. Exactly one of `file` and `content` must be set.",
				Optional:    true,
			},
			"content": schema.StringAttribute{
				Description: "Content of the file, for a CSV written in the configuration. Exactly one of `file` and `content` must be set.",
				Optional:    true,
			},
			"already_exists": schema.StringAttribute{
				Description: "What to do when the table exists: `replace` it, `append` the rows to it, or `fail`. Defaults to `replace`, which keeps every upload idempotent; with `fail`, a second upload of the resource fails.",
				Optional:    true,
				Computed:    true,
				Default:     stringdefault.StaticString("replace"),
			},
			"delimiter": schema.StringAttribute{
				Description: "Delimiter of a CSV file. Defaults to a comma.",
				Optional:    true,
			},
			"header_row": schema.Int64Attribute{
				Description: "Row of the column names, counted from 0. Defaults to the first row.",
				Optional:    true,
			},
			"skip_rows": schema.Int64Attribute{
				Description: "Number of rows to skip at the start of the file.",
				Optional:    true,
			},
			"rows_to_read": schema.Int64Attribute{
				Description: "Number of rows to read; all of them when unset.",
				Optional:    true,
			},
			"column_dates": schema.ListAttribute{
				Description: "Names of the columns to parse as dates.",
				ElementType: types.StringType,
				Optional:    true,
			},
			"column_data_types": schema.StringAttribute{
				Description: "JSON object with the type of columns that must not be inferred, for example `{\"zip\": \"str\"}`.",
				Optional:    true,
			},
			"null_values": schema.ListAttribute{
				Description: "Values to read as null, in addition to the defaults of pandas.",
				ElementType: types.StringType,
				Optional:    true,
			},
			"decimal_character": schema.StringAttribute{
				Description: "Decimal separator of the numbers. Defaults to a point.",
				Optional:    true,
			},
			"day_first": schema.BoolAttribute{
				Description: "Whether dates start with the day, as in `31/12/2026`.",
				Optional:    true,
			},
			"skip_blank_lines": schema.BoolAttribute{
				Description: "Whether blank lines are skipped instead of read as rows of nulls.",
				Optional:    true,
			},
			"skip_initial_space": schema.BoolAttribute{
				Description: "Whether spaces after the delimiter are skipped.",
				Optional:    true,
			},
			"columns_read": schema.ListAttribute{
				Description: "Names of the columns to read; all of them when unset.",
				ElementType: types.StringType,
				Optional:    true,
			},
			"index_column": schema.StringAttribute{
				Description: "Column to use as the index of the data frame.",
				Optional:    true,
			},
			"dataframe_index": schema.BoolAttribute{
				Description: "Whether the index of the data frame is written as a column.",
				Optional:    true,
			},
			"index_label": schema.StringAttribute{
				Description: "Name of the index column when it is written.",
				Optional:    true,
			},
			"sheet_name": schema.StringAttribute{
				Description: "Sheet of an Excel file to read. Defaults to the first one.",
				Optional:    true,
			},
			"triggers": schema.MapAttribute{
				Description: "Arbitrary values whose change uploads the file again, for example `filemd5()` of the file.",
				ElementType: types.StringType,
				Optional:    true,
			},
			"drop_on_destroy": schema.BoolAttribute{
				Description: "Whether the destroy drops the table through SQL Lab, which needs `allow_dml` on the connection, and deletes the dataset Superset created for it. Defaults to `false`: the table and the dataset stay.",
				Optional:    true,
				Computed:    true,
				Default:     booldefault.StaticBool(false),
			},
			"dataset_id": schema.Int64Attribute{
				Description: "Identifier of the dataset Superset created for the table; null when none was found.",
				Computed:    true,
			},
		},
	}
}

// Create uploads the file.
func (r *databaseUploadResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan databaseUploadResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}
	if !r.resolveDatabase(&plan, &resp.Diagnostics) || !r.upload(ctx, &plan, &resp.Diagnostics) {
		return
	}
	resp.Diagnostics.Append(r.refresh(&plan)...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

// Read checks that the table still exists and refreshes its dataset.
func (r *databaseUploadResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state databaseUploadResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	databaseID := state.DatabaseID.ValueInt64()
	if schema := state.Schema.ValueString(); schema == "" {
		// Without a schema the table cannot be listed; the connection at least must exist.
		if _, err := r.client.GetDatabaseConnection(databaseID); err != nil {
			if errors.Is(err, client.ErrNotFound) {
				tflog.Warn(ctx, "The database connection no longer exists; the upload is planned again", map[string]any{"database_id": databaseID})
				resp.State.RemoveResource(ctx)
				return
			}
			resp.Diagnostics.AddError("Unable to Read the Superset Database", err.Error())
			return
		}
	} else {
		tables, err := r.client.DatabaseTables(databaseID, schema, "")
		if err != nil {
			if errors.Is(err, client.ErrNotFound) {
				tflog.Warn(ctx, "The database connection no longer exists; the upload is planned again", map[string]any{"database_id": databaseID})
				resp.State.RemoveResource(ctx)
				return
			}
			resp.Diagnostics.AddError("Unable to Read the Tables of the Superset Database", err.Error())
			return
		}
		found := false
		for _, table := range tables {
			if table.Name == state.TableName.ValueString() {
				found = true
			}
		}
		if !found {
			tflog.Warn(ctx, "The uploaded table no longer exists; the upload is planned again", map[string]any{"table": state.TableName.ValueString()})
			resp.State.RemoveResource(ctx)
			return
		}
	}
	resp.Diagnostics.Append(r.refresh(&state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &state)...)
}

// Update uploads the file again when the content, the options or the triggers changed.
func (r *databaseUploadResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state databaseUploadResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	plan.ID = state.ID
	if !plan.uploadEquals(state) && !r.upload(ctx, &plan, &resp.Diagnostics) {
		return
	}
	resp.Diagnostics.Append(r.refresh(&plan)...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

// Delete drops the table and deletes its dataset when drop_on_destroy is set.
func (r *databaseUploadResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state databaseUploadResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() || !state.DropOnDestroy.ValueBool() {
		return
	}

	if !state.DatasetID.IsNull() {
		if err := r.client.DeleteDataset(state.DatasetID.ValueInt64()); err != nil && !errors.Is(err, client.ErrNotFound) {
			resp.Diagnostics.AddError("Unable to Delete the Dataset of the Uploaded Table", fmt.Sprintf("Could not delete dataset %d: %s", state.DatasetID.ValueInt64(), err))
			return
		}
	}
	table := state.TableName.ValueString()
	if schema := state.Schema.ValueString(); schema != "" {
		table = schema + "." + table
	}
	_, err := r.client.ExecuteSQL(client.SQLQuery{DatabaseID: state.DatabaseID.ValueInt64(), SQL: "DROP TABLE IF EXISTS " + table, Limit: 1})
	if err != nil {
		if errors.Is(err, client.ErrNotFound) {
			return
		}
		resp.Diagnostics.AddError("Unable to Drop the Uploaded Table", fmt.Sprintf("Could not drop table %s through SQL Lab, which needs allow_dml on the connection; set drop_on_destroy to false to keep the table: %s", table, err))
	}
}

// resolveDatabase checks that exactly one of the name and the id is set and resolves the id.
func (r *databaseUploadResource) resolveDatabase(model *databaseUploadResourceModel, diags *diag.Diagnostics) bool {
	name := model.DatabaseName.ValueString()
	if (name == "") == (model.DatabaseID.IsNull() || model.DatabaseID.IsUnknown()) {
		diags.AddAttributeError(path.Root("database_name"), "Invalid Database Reference", "Set exactly one of database_name and database_id.")
		return false
	}
	if name != "" {
		database, err := r.client.FindDatabaseByName(name)
		if err != nil {
			diags.AddAttributeError(path.Root("database_name"), "Unable to Find the Superset Database", fmt.Sprintf("Could not look up database %q: %s", name, err))
			return false
		}
		model.DatabaseID = types.Int64Value(database.ID)
	}
	return true
}

// upload reads the file and sends it, and sets the id of the model.
func (r *databaseUploadResource) upload(ctx context.Context, model *databaseUploadResourceModel, diags *diag.Diagnostics) bool {
	file, content := model.File.ValueString(), model.Content.ValueString()
	if (file == "") == (content == "") {
		diags.AddAttributeError(path.Root("file"), "Invalid Upload Source", "Set exactly one of file and content.")
		return false
	}
	name, data := "content.csv", []byte(content)
	if file != "" {
		read, err := os.ReadFile(file)
		if err != nil {
			diags.AddAttributeError(path.Root("file"), "Unable to Read the File", err.Error())
			return false
		}
		name, data = filepath.Base(file), read
	}

	upload := client.Upload{
		DatabaseID:       model.DatabaseID.ValueInt64(),
		TableName:        model.TableName.ValueString(),
		Schema:           model.Schema.ValueString(),
		Type:             model.Type.ValueString(),
		FileName:         name,
		Content:          data,
		AlreadyExists:    model.AlreadyExists.ValueString(),
		Delimiter:        model.Delimiter.ValueString(),
		HeaderRow:        model.HeaderRow.ValueInt64Pointer(),
		SkipRows:         model.SkipRows.ValueInt64Pointer(),
		RowsToRead:       model.RowsToRead.ValueInt64Pointer(),
		ColumnDataTypes:  model.ColumnDataTypes.ValueString(),
		DecimalCharacter: model.DecimalCharacter.ValueString(),
		DayFirst:         model.DayFirst.ValueBoolPointer(),
		SkipBlankLines:   model.SkipBlankLines.ValueBoolPointer(),
		SkipInitialSpace: model.SkipInitialSpace.ValueBoolPointer(),
		IndexColumn:      model.IndexColumn.ValueString(),
		DataframeIndex:   model.DataframeIndex.ValueBoolPointer(),
		IndexLabel:       model.IndexLabel.ValueString(),
		SheetName:        model.SheetName.ValueString(),
	}
	for _, list := range []struct {
		value  types.List
		target *[]string
	}{{model.ColumnDates, &upload.ColumnDates}, {model.NullValues, &upload.NullValues}, {model.ColumnsRead, &upload.ColumnsRead}} {
		if !list.value.IsNull() && !list.value.IsUnknown() {
			diags.Append(list.value.ElementsAs(ctx, list.target, false)...)
		}
	}
	if diags.HasError() {
		return false
	}

	if err := r.client.UploadFile(upload); err != nil {
		diags.AddError("Unable to Upload the File into Superset", fmt.Sprintf("Could not upload %s into table %s: %s", name, upload.TableName, err))
		return false
	}
	tflog.Debug(ctx, "Uploaded the file", map[string]any{"table": upload.TableName, "bytes": len(data)})
	model.ID = types.StringValue(fmt.Sprintf("%d:%s.%s", upload.DatabaseID, upload.Schema, upload.TableName))
	return true
}

// refresh finds the dataset Superset keeps for the table.
func (r *databaseUploadResource) refresh(model *databaseUploadResourceModel) diag.Diagnostics {
	var diags diag.Diagnostics
	databaseID := model.DatabaseID.ValueInt64()
	datasets, err := r.client.FindDatasets(model.TableName.ValueString(), model.Schema.ValueString(), &databaseID)
	if err != nil {
		diags.AddError("Unable to Find the Dataset of the Uploaded Table", err.Error())
		return diags
	}
	model.DatasetID = types.Int64Null()
	if len(datasets) > 0 {
		model.DatasetID = types.Int64Value(datasets[0].ID)
	}
	return diags
}

// uploadEquals reports whether the attributes that decide an upload are the same.
func (m databaseUploadResourceModel) uploadEquals(other databaseUploadResourceModel) bool {
	return m.File.Equal(other.File) && m.Content.Equal(other.Content) && m.AlreadyExists.Equal(other.AlreadyExists) &&
		m.Delimiter.Equal(other.Delimiter) && m.HeaderRow.Equal(other.HeaderRow) && m.SkipRows.Equal(other.SkipRows) &&
		m.RowsToRead.Equal(other.RowsToRead) && m.ColumnDates.Equal(other.ColumnDates) && m.ColumnDataTypes.Equal(other.ColumnDataTypes) &&
		m.NullValues.Equal(other.NullValues) && m.DecimalCharacter.Equal(other.DecimalCharacter) && m.DayFirst.Equal(other.DayFirst) &&
		m.SkipBlankLines.Equal(other.SkipBlankLines) && m.SkipInitialSpace.Equal(other.SkipInitialSpace) && m.ColumnsRead.Equal(other.ColumnsRead) &&
		m.IndexColumn.Equal(other.IndexColumn) && m.DataframeIndex.Equal(other.DataframeIndex) && m.IndexLabel.Equal(other.IndexLabel) &&
		m.SheetName.Equal(other.SheetName) && m.Triggers.Equal(other.Triggers)
}

// Configure adds the provider configured client to the resource.
func (r *databaseUploadResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.client = resourceClient(req, &resp.Diagnostics)
}
