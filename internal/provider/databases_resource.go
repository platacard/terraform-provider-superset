package provider

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework/attr"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/booldefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/int64planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-framework/types/basetypes"
	"github.com/hashicorp/terraform-plugin-log/tflog"
	"terraform-provider-superset/internal/client"
)

// Ensure the implementation satisfies the expected interfaces.
var (
	_ resource.Resource                   = &databaseResource{}
	_ resource.ResourceWithConfigure      = &databaseResource{}
	_ resource.ResourceWithImportState    = &databaseResource{}
	_ resource.ResourceWithValidateConfig = &databaseResource{}
)

// defaultDatabaseExtra is the extra configuration of a connection created without one.
const defaultDatabaseExtra = `{"client_encoding": "utf8"}`

// NewDatabaseResource is a helper function to simplify the provider implementation.
func NewDatabaseResource() resource.Resource {
	return &databaseResource{}
}

// databaseResource manages a database connection.
type databaseResource struct {
	client *client.Client
}

// databaseResourceModel maps the resource schema data.
type databaseResourceModel struct {
	ID                   types.Int64  `tfsdk:"id"`
	ConnectionName       types.String `tfsdk:"connection_name"`
	SQLAlchemyURI        types.String `tfsdk:"sqlalchemy_uri"`
	DBEngine             types.String `tfsdk:"db_engine"`
	DBUser               types.String `tfsdk:"db_user"`
	DBPass               types.String `tfsdk:"db_pass"`
	DBHost               types.String `tfsdk:"db_host"`
	DBPort               types.Int64  `tfsdk:"db_port"`
	DBName               types.String `tfsdk:"db_name"`
	AllowCTAS            types.Bool   `tfsdk:"allow_ctas"`
	AllowCVAS            types.Bool   `tfsdk:"allow_cvas"`
	AllowDML             types.Bool   `tfsdk:"allow_dml"`
	AllowRunAsync        types.Bool   `tfsdk:"allow_run_async"`
	ExposeInSQLLab       types.Bool   `tfsdk:"expose_in_sqllab"`
	AllowFileUpload      types.Bool   `tfsdk:"allow_file_upload"`
	ImpersonateUser      types.Bool   `tfsdk:"impersonate_user"`
	CacheTimeout         types.Int64  `tfsdk:"cache_timeout"`
	ForceCTASSchema      types.String `tfsdk:"force_ctas_schema"`
	Extra                jsonValue    `tfsdk:"extra"`
	MaskedEncryptedExtra types.String `tfsdk:"masked_encrypted_extra"`
	ServerCert           types.String `tfsdk:"server_cert"`
	SSHTunnel            types.Object `tfsdk:"ssh_tunnel"`
}

// sshTunnelModel maps the ssh_tunnel attribute.
type sshTunnelModel struct {
	ServerAddress      types.String `tfsdk:"server_address"`
	ServerPort         types.Int64  `tfsdk:"server_port"`
	Username           types.String `tfsdk:"username"`
	Password           types.String `tfsdk:"password"`
	PrivateKey         types.String `tfsdk:"private_key"`
	PrivateKeyPassword types.String `tfsdk:"private_key_password"`
}

var sshTunnelAttributeTypes = map[string]attr.Type{
	"server_address":       types.StringType,
	"server_port":          types.Int64Type,
	"username":             types.StringType,
	"password":             types.StringType,
	"private_key":          types.StringType,
	"private_key_password": types.StringType,
}

// connectionParts are the attributes that together form the connection URI.
var connectionParts = []string{"db_engine", "db_user", "db_pass", "db_host", "db_port", "db_name"}

// Metadata returns the resource type name.
func (r *databaseResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_database"
}

// Schema defines the schema for the resource.
func (r *databaseResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Manages a database connection. The connection is given either as a full `sqlalchemy_uri` or as its parts (`db_engine`, `db_user`, `db_pass`, `db_host`, `db_port` and `db_name`). Superset tests the connection when it is created or updated.",
		Attributes: map[string]schema.Attribute{
			"id": schema.Int64Attribute{
				Description: "Numeric identifier of the database connection.",
				Computed:    true,
				PlanModifiers: []planmodifier.Int64{
					int64planmodifier.UseStateForUnknown(),
				},
			},
			"connection_name": schema.StringAttribute{
				Description: "Name of the database connection.",
				Required:    true,
			},
			"sqlalchemy_uri": schema.StringAttribute{
				Description: "Full SQLAlchemy URI of the connection, for engines whose URI does not fit the parts, for example `awsathena+rest://...` or `trino://...`. Conflicts with the parts. Changes made to the URI in the Superset UI are not detected, because Superset masks the password in the URI it returns.",
				Optional:    true,
				Sensitive:   true,
			},
			"db_engine": schema.StringAttribute{
				Description: "SQLAlchemy dialect of the connection, for example `postgresql` or `mysql+pymysql`. Required with the other parts when `sqlalchemy_uri` is not set.",
				Optional:    true,
			},
			"db_user": schema.StringAttribute{
				Description: "Username of the connection.",
				Optional:    true,
			},
			"db_pass": schema.StringAttribute{
				Description: "Password of the connection. It is inserted into the URI as is, so percent-encode special characters or use `sqlalchemy_uri`.",
				Optional:    true,
				Sensitive:   true,
			},
			"db_host": schema.StringAttribute{
				Description: "Host of the database server.",
				Optional:    true,
			},
			"db_port": schema.Int64Attribute{
				Description: "Port of the database server.",
				Optional:    true,
			},
			"db_name": schema.StringAttribute{
				Description: "Name of the database.",
				Optional:    true,
			},
			"allow_ctas": schema.BoolAttribute{
				Description: "Whether `CREATE TABLE AS` is allowed in SQL Lab.",
				Required:    true,
			},
			"allow_cvas": schema.BoolAttribute{
				Description: "Whether `CREATE VIEW AS` is allowed in SQL Lab.",
				Required:    true,
			},
			"allow_dml": schema.BoolAttribute{
				Description: "Whether DML statements are allowed in SQL Lab.",
				Required:    true,
			},
			"allow_run_async": schema.BoolAttribute{
				Description: "Whether SQL Lab queries run asynchronously through Celery.",
				Required:    true,
			},
			"expose_in_sqllab": schema.BoolAttribute{
				Description: "Whether the connection is available in SQL Lab.",
				Required:    true,
			},
			"allow_file_upload": schema.BoolAttribute{
				Description: "Whether files can be uploaded into the database. The allowed schemas are set in `extra` as `schemas_allowed_for_file_upload`. Defaults to `false`.",
				Optional:    true,
				Computed:    true,
				Default:     booldefault.StaticBool(false),
			},
			"impersonate_user": schema.BoolAttribute{
				Description: "Whether queries run as the logged-in user instead of the user of the connection. Defaults to `false`.",
				Optional:    true,
				Computed:    true,
				Default:     booldefault.StaticBool(false),
			},
			"cache_timeout": schema.Int64Attribute{
				Description: "Duration in seconds for which the charts of the database are cached. When unset, the value is left as it is in Superset: the timeout of the Superset instance for a new connection.",
				Optional:    true,
				Computed:    true,
				PlanModifiers: []planmodifier.Int64{
					int64planmodifier.UseStateForUnknown(),
				},
			},
			"force_ctas_schema": schema.StringAttribute{
				Description: "Schema in which `CREATE TABLE AS` creates its tables when the query does not name one.",
				Optional:    true,
			},
			"extra": schema.StringAttribute{
				Description: "JSON document with the extra configuration of the connection, such as `engine_params`, `metadata_params` or `schemas_allowed_for_file_upload`. Defaults to `{\"client_encoding\": \"utf8\"}` when the connection is created without it.",
				Optional:    true,
				Computed:    true,
				CustomType:  jsonDocument,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"masked_encrypted_extra": schema.StringAttribute{
				Description: "JSON document with the encrypted extra configuration of the connection, such as the credentials of BigQuery or Google Sheets. Superset masks it in its responses, so changes made in the UI are not detected.",
				Optional:    true,
				Sensitive:   true,
			},
			"server_cert": schema.StringAttribute{
				Description: "CA bundle, in PEM format, used to validate the certificate of the database server.",
				Optional:    true,
			},
			"ssh_tunnel": schema.SingleNestedAttribute{
				Description: "SSH tunnel through which Superset reaches the database. Requires the `SSH_TUNNELING` feature flag in Superset. Superset authenticates with either `password` or `private_key`; the private key must be an RSA key.",
				Optional:    true,
				Attributes: map[string]schema.Attribute{
					"server_address": schema.StringAttribute{
						Description: "Host of the SSH server.",
						Required:    true,
					},
					"server_port": schema.Int64Attribute{
						Description: "Port of the SSH server.",
						Required:    true,
					},
					"username": schema.StringAttribute{
						Description: "User on the SSH server.",
						Required:    true,
					},
					"password": schema.StringAttribute{
						Description: "Password of the user. Conflicts with `private_key`.",
						Optional:    true,
						Sensitive:   true,
					},
					"private_key": schema.StringAttribute{
						Description: "RSA private key of the user, in PEM format. Conflicts with `password`.",
						Optional:    true,
						Sensitive:   true,
					},
					"private_key_password": schema.StringAttribute{
						Description: "Passphrase of the private key.",
						Optional:    true,
						Sensitive:   true,
					},
				},
			},
		},
	}
}

// ValidateConfig checks that the connection is given either as a URI or as all of its parts, and
// that a tunnel authenticates with exactly one of password and private key.
func (r *databaseResource) ValidateConfig(ctx context.Context, req resource.ValidateConfigRequest, resp *resource.ValidateConfigResponse) {
	var config databaseResourceModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &config)...)
	if resp.Diagnostics.HasError() {
		return
	}

	parts := []attr.Value{config.DBEngine, config.DBUser, config.DBPass, config.DBHost, config.DBPort, config.DBName}
	set := 0
	for _, part := range parts {
		if !part.IsNull() {
			set++
		}
	}
	switch {
	case !config.SQLAlchemyURI.IsNull() && set > 0:
		resp.Diagnostics.AddAttributeError(path.Root("sqlalchemy_uri"), "Conflicting Connection Attributes",
			"sqlalchemy_uri cannot be combined with "+strings.Join(connectionParts, ", ")+".")
	case config.SQLAlchemyURI.IsNull() && set < len(parts):
		resp.Diagnostics.AddAttributeError(path.Root("connection_name"), "Incomplete Connection",
			"Set either sqlalchemy_uri or all of "+strings.Join(connectionParts, ", ")+".")
	}

	if config.SSHTunnel.IsNull() || config.SSHTunnel.IsUnknown() {
		return
	}
	var tunnel sshTunnelModel
	resp.Diagnostics.Append(config.SSHTunnel.As(ctx, &tunnel, basetypesObjectAsOptions)...)
	if resp.Diagnostics.HasError() {
		return
	}
	if tunnel.Password.IsUnknown() || tunnel.PrivateKey.IsUnknown() {
		return
	}
	if tunnel.Password.IsNull() == tunnel.PrivateKey.IsNull() {
		resp.Diagnostics.AddAttributeError(path.Root("ssh_tunnel"), "Invalid SSH Tunnel Authentication",
			"Set exactly one of password and private_key.")
	}
}

// Create creates the connection and sets the initial Terraform state.
func (r *databaseResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan databaseResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	conn, diags := connectionFromModel(ctx, plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
	if conn.Extra == "" {
		conn.Extra = defaultDatabaseExtra
	}

	id, err := r.client.CreateDatabaseConnection(conn)
	if err != nil {
		resp.Diagnostics.AddError("Unable to Create Superset Database Connection", err.Error())
		return
	}

	r.readInto(ctx, id, plan, &resp.State, &resp.Diagnostics, "Unable to Read Superset Database Connection After Creation")
}

// Read refreshes the Terraform state with the connection as it is in Superset.
func (r *databaseResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state databaseResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	conn, err := r.client.GetDatabaseConnection(state.ID.ValueInt64())
	if errors.Is(err, client.ErrNotFound) {
		tflog.Warn(ctx, "database connection no longer exists in Superset, removing it from the state", map[string]any{"id": state.ID.ValueInt64()})
		resp.State.RemoveResource(ctx)
		return
	}
	if err != nil {
		resp.Diagnostics.AddError("Unable to Read Superset Database Connection", err.Error())
		return
	}

	model, diags := databaseConnectionModel(ctx, conn, state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &model)...)
}

// Update replaces the attributes of the connection.
func (r *databaseResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state databaseResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	conn, diags := connectionFromModel(ctx, plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	removeTunnel := !state.SSHTunnel.IsNull() && plan.SSHTunnel.IsNull()
	if err := r.client.UpdateDatabaseConnection(state.ID.ValueInt64(), conn, removeTunnel); err != nil {
		resp.Diagnostics.AddError("Unable to Update Superset Database Connection", err.Error())
		return
	}

	r.readInto(ctx, state.ID.ValueInt64(), plan, &resp.State, &resp.Diagnostics, "Unable to Read Superset Database Connection After Update")
}

// Delete deletes the connection; a connection that is already gone is not an error.
func (r *databaseResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state databaseResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	err := r.client.DeleteDatabaseConnection(state.ID.ValueInt64())
	if err != nil && !errors.Is(err, client.ErrNotFound) {
		resp.Diagnostics.AddError("Unable to Delete Superset Database Connection", err.Error())
		return
	}
}

// ImportState imports a connection by its numeric id. Superset does not return the password of
// the connection or the secrets of its tunnel, so those come from the configuration on the next apply.
func (r *databaseResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	id, err := strconv.ParseInt(req.ID, 10, 64)
	if err != nil {
		resp.Diagnostics.AddError("Invalid Import ID", fmt.Sprintf("The import ID %q is not a numeric database id.", req.ID))
		return
	}
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("id"), id)...)
}

// Configure adds the provider configured client to the resource.
func (r *databaseResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

// readInto fetches the connection and stores it in the state after a create or an update, so the
// state carries the values Superset computed, such as the extra configuration it completed.
func (r *databaseResource) readInto(ctx context.Context, id int64, plan databaseResourceModel, state stateSetter, diags *diag.Diagnostics, summary string) {
	conn, err := r.client.GetDatabaseConnection(id)
	if err != nil {
		diags.AddError(summary, err.Error())
		return
	}

	model, d := databaseConnectionModel(ctx, conn, plan)
	diags.Append(d...)
	if diags.HasError() {
		return
	}
	diags.Append(state.Set(ctx, &model)...)
}

func connectionFromModel(ctx context.Context, model databaseResourceModel) (client.DatabaseConnection, diag.Diagnostics) {
	conn := client.DatabaseConnection{
		Name:                 model.ConnectionName.ValueString(),
		SQLAlchemyURI:        model.SQLAlchemyURI.ValueString(),
		ExposeInSQLLab:       model.ExposeInSQLLab.ValueBool(),
		AllowCTAS:            model.AllowCTAS.ValueBool(),
		AllowCVAS:            model.AllowCVAS.ValueBool(),
		AllowDML:             model.AllowDML.ValueBool(),
		AllowRunAsync:        model.AllowRunAsync.ValueBool(),
		AllowFileUpload:      model.AllowFileUpload.ValueBool(),
		ImpersonateUser:      model.ImpersonateUser.ValueBool(),
		CacheTimeout:         model.CacheTimeout.ValueInt64Pointer(),
		ForceCTASSchema:      model.ForceCTASSchema.ValueString(),
		Extra:                model.Extra.ValueString(),
		MaskedEncryptedExtra: model.MaskedEncryptedExtra.ValueString(),
		ServerCert:           model.ServerCert.ValueString(),
	}
	if model.SQLAlchemyURI.IsNull() {
		conn.SQLAlchemyURI = fmt.Sprintf("%s://%s:%s@%s:%d/%s", model.DBEngine.ValueString(), model.DBUser.ValueString(),
			model.DBPass.ValueString(), model.DBHost.ValueString(), model.DBPort.ValueInt64(), model.DBName.ValueString())
	}

	var diags diag.Diagnostics
	if !model.SSHTunnel.IsNull() && !model.SSHTunnel.IsUnknown() {
		var tunnel sshTunnelModel
		diags.Append(model.SSHTunnel.As(ctx, &tunnel, basetypesObjectAsOptions)...)
		conn.SSHTunnel = &client.SSHTunnel{
			ServerAddress:      tunnel.ServerAddress.ValueString(),
			ServerPort:         tunnel.ServerPort.ValueInt64(),
			Username:           tunnel.Username.ValueString(),
			Password:           tunnel.Password.ValueString(),
			PrivateKey:         tunnel.PrivateKey.ValueString(),
			PrivateKeyPassword: tunnel.PrivateKeyPassword.ValueString(),
		}
	}
	return conn, diags
}

// databaseConnectionModel maps a connection to the state. Secrets, which Superset masks, are taken from
// the prior model: the password of the connection, the encrypted extra and the secrets of the
// tunnel. A connection configured as a URI keeps its URI, because the masked URI Superset
// returns cannot be compared with it.
func databaseConnectionModel(ctx context.Context, conn *client.DatabaseConnection, prior databaseResourceModel) (databaseResourceModel, diag.Diagnostics) {
	model := databaseResourceModel{
		ID:                   types.Int64Value(conn.ID),
		ConnectionName:       types.StringValue(conn.Name),
		SQLAlchemyURI:        prior.SQLAlchemyURI,
		AllowCTAS:            types.BoolValue(conn.AllowCTAS),
		AllowCVAS:            types.BoolValue(conn.AllowCVAS),
		AllowDML:             types.BoolValue(conn.AllowDML),
		AllowRunAsync:        types.BoolValue(conn.AllowRunAsync),
		ExposeInSQLLab:       types.BoolValue(conn.ExposeInSQLLab),
		AllowFileUpload:      types.BoolValue(conn.AllowFileUpload),
		ImpersonateUser:      types.BoolValue(conn.ImpersonateUser),
		CacheTimeout:         types.Int64PointerValue(conn.CacheTimeout),
		ForceCTASSchema:      optionalString(conn.ForceCTASSchema),
		Extra:                jsonDocument.value(conn.Extra),
		MaskedEncryptedExtra: prior.MaskedEncryptedExtra,
		ServerCert:           optionalString(conn.ServerCert),
		SSHTunnel:            types.ObjectNull(sshTunnelAttributeTypes),
	}
	if conn.MaskedEncryptedExtra == "" {
		model.MaskedEncryptedExtra = types.StringNull()
	}
	if prior.SQLAlchemyURI.IsNull() {
		model.DBEngine = engineValue(prior.DBEngine, conn.Backend)
		model.DBUser = parameterValue(prior.DBUser, conn.Parameters.Username)
		model.DBPass = prior.DBPass
		model.DBHost = parameterValue(prior.DBHost, conn.Parameters.Host)
		model.DBName = parameterValue(prior.DBName, conn.Parameters.Database)
		model.DBPort = prior.DBPort
		if conn.Parameters.Port != 0 {
			model.DBPort = types.Int64Value(conn.Parameters.Port)
		}
	}

	var diags diag.Diagnostics
	if tunnel := conn.SSHTunnel; tunnel != nil {
		var priorTunnel sshTunnelModel
		if !prior.SSHTunnel.IsNull() && !prior.SSHTunnel.IsUnknown() {
			diags.Append(prior.SSHTunnel.As(ctx, &priorTunnel, basetypesObjectAsOptions)...)
		}
		value := sshTunnelModel{
			ServerAddress:      types.StringValue(tunnel.ServerAddress),
			ServerPort:         types.Int64Value(tunnel.ServerPort),
			Username:           types.StringValue(tunnel.Username),
			Password:           nullIfUnset(priorTunnel.Password),
			PrivateKey:         nullIfUnset(priorTunnel.PrivateKey),
			PrivateKeyPassword: nullIfUnset(priorTunnel.PrivateKeyPassword),
		}
		object, d := types.ObjectValueFrom(ctx, sshTunnelAttributeTypes, value)
		diags.Append(d...)
		model.SSHTunnel = object
	}
	return model, diags
}

// engineValue keeps the configured engine when Superset reports its backend, so a driver suffix
// such as `postgresql+psycopg2` is not reported as drift, and takes the backend otherwise.
func engineValue(prior types.String, backend string) types.String {
	if backend == "" || strings.SplitN(prior.ValueString(), "+", 2)[0] == backend {
		return prior
	}
	return types.StringValue(backend)
}

// parameterValue takes the value Superset reports, or keeps the prior one for engines whose
// URI Superset does not break into parameters.
func parameterValue(prior types.String, reported string) types.String {
	if reported == "" {
		return prior
	}
	return types.StringValue(reported)
}

// nullIfUnset turns the zero value of a string, as found in a prior model without a tunnel, into null.
func nullIfUnset(value types.String) types.String {
	if value.IsNull() || value.IsUnknown() || value.ValueString() == "" {
		return types.StringNull()
	}
	return value
}

// basetypesObjectAsOptions converts objects into models without failing on null or unknown values.
var basetypesObjectAsOptions = basetypes.ObjectAsOptions{UnhandledNullAsEmpty: true, UnhandledUnknownAsEmpty: true}
