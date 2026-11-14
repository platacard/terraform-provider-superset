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

// The security lookups find a user, a group or a row level security rule by the name a
// person knows it by. Their names are unique in Superset, so each lookup returns at most one
// object.

// User

var (
	_ datasource.DataSource              = &userDataSource{}
	_ datasource.DataSourceWithConfigure = &userDataSource{}
)

// NewUserDataSource is a helper function to simplify the provider implementation.
func NewUserDataSource() datasource.DataSource {
	return &userDataSource{}
}

// userDataSource looks up one user by its username.
type userDataSource struct {
	client *client.Client
}

type userDataSourceModel struct {
	ID        types.Int64  `tfsdk:"id"`
	Username  types.String `tfsdk:"username"`
	FirstName types.String `tfsdk:"first_name"`
	LastName  types.String `tfsdk:"last_name"`
	Email     types.String `tfsdk:"email"`
	Active    types.Bool   `tfsdk:"active"`
	Roles     types.List   `tfsdk:"roles"`
	Groups    types.Set    `tfsdk:"groups"`
}

func (d *userDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_user"
}

func (d *userDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Looks up a user by its username, for example to reference its id in the `owners` of a dashboard or in `superset_group.users`. Usernames are unique; the lookup fails when no user has exactly that username. The users endpoint belongs to the Flask-AppBuilder security API, which Superset only enables with `FAB_ADD_SECURITY_API = True`.",
		Attributes: map[string]schema.Attribute{
			"username": schema.StringAttribute{
				Description: "Username of the user. The comparison is exact.",
				Required:    true,
			},
			"id": schema.Int64Attribute{
				Description: "Numeric identifier of the user.",
				Computed:    true,
			},
			"first_name": schema.StringAttribute{
				Description: "First name of the user.",
				Computed:    true,
			},
			"last_name": schema.StringAttribute{
				Description: "Last name of the user.",
				Computed:    true,
			},
			"email": schema.StringAttribute{
				Description: "Email address of the user.",
				Computed:    true,
			},
			"active": schema.BoolAttribute{
				Description: "Whether the user can log in.",
				Computed:    true,
			},
			"roles": schema.ListAttribute{
				Description: "Identifiers of the roles of the user.",
				Computed:    true,
				ElementType: types.Int64Type,
			},
			"groups": schema.SetAttribute{
				Description: "Identifiers of the groups the user is a member of, on Superset 6; null on versions without groups.",
				Computed:    true,
				ElementType: types.Int64Type,
			},
		},
	}
}

func (d *userDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var config userDataSourceModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &config)...)
	if resp.Diagnostics.HasError() {
		return
	}

	username := config.Username.ValueString()
	if username == "" {
		resp.Diagnostics.AddAttributeError(path.Root("username"), "Missing Username", "The username must not be empty.")
		return
	}

	user, err := d.client.FindUserByUsername(username)
	if err != nil {
		resp.Diagnostics.AddError("Unable to Read Superset User", fmt.Sprintf("Could not look up user %q: %s", username, err))
		return
	}

	roles, diags := types.ListValueFrom(ctx, types.Int64Type, nonNilIDs(user.Roles))
	resp.Diagnostics.Append(diags...)
	groups, diags := groupsValue(ctx, user.Groups)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	config.ID = types.Int64Value(user.ID)
	config.FirstName = types.StringValue(user.FirstName)
	config.LastName = types.StringValue(user.LastName)
	config.Email = types.StringValue(user.Email)
	config.Active = types.BoolValue(user.Active)
	config.Roles = roles
	config.Groups = groups
	resp.Diagnostics.Append(resp.State.Set(ctx, &config)...)
}

func (d *userDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	d.client = dataSourceClient(req, &resp.Diagnostics)
}

// Group

var (
	_ datasource.DataSource              = &groupDataSource{}
	_ datasource.DataSourceWithConfigure = &groupDataSource{}
)

// NewGroupDataSource is a helper function to simplify the provider implementation.
func NewGroupDataSource() datasource.DataSource {
	return &groupDataSource{}
}

// groupDataSource looks up one group by its name.
type groupDataSource struct {
	client *client.Client
}

type groupDataSourceModel struct {
	ID          types.Int64  `tfsdk:"id"`
	Name        types.String `tfsdk:"name"`
	Label       types.String `tfsdk:"label"`
	Description types.String `tfsdk:"description"`
	Roles       types.Set    `tfsdk:"roles"`
	Users       types.Set    `tfsdk:"users"`
}

func (d *groupDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_group"
}

func (d *groupDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Looks up a group of users by its name, on Superset 6, for example to reference its id in `superset_user.groups`. Group names are unique; the lookup fails when no group has exactly that name. The group endpoints belong to the Flask-AppBuilder security API, which Superset only enables with `FAB_ADD_SECURITY_API = True`.",
		Attributes: map[string]schema.Attribute{
			"name": schema.StringAttribute{
				Description: "Name of the group. The comparison is exact.",
				Required:    true,
			},
			"id": schema.Int64Attribute{
				Description: "Numeric identifier of the group.",
				Computed:    true,
			},
			"label": schema.StringAttribute{
				Description: "Label of the group, shown in the UI.",
				Computed:    true,
			},
			"description": schema.StringAttribute{
				Description: "Description of the group.",
				Computed:    true,
			},
			"roles": schema.SetAttribute{
				Description: "Identifiers of the roles the group grants to its members.",
				Computed:    true,
				ElementType: types.Int64Type,
			},
			"users": schema.SetAttribute{
				Description: "Identifiers of the members of the group.",
				Computed:    true,
				ElementType: types.Int64Type,
			},
		},
	}
}

func (d *groupDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var config groupDataSourceModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &config)...)
	if resp.Diagnostics.HasError() {
		return
	}

	name := config.Name.ValueString()
	if name == "" {
		resp.Diagnostics.AddAttributeError(path.Root("name"), "Missing Group Name", "The group name must not be empty.")
		return
	}

	group, err := d.client.FindGroupByName(name)
	if err != nil {
		resp.Diagnostics.AddError("Unable to Read Superset Group", fmt.Sprintf("Could not look up group %q: %s", name, err))
		return
	}

	roles, diags := types.SetValueFrom(ctx, types.Int64Type, nonNilIDs(group.Roles))
	resp.Diagnostics.Append(diags...)
	users, diags := types.SetValueFrom(ctx, types.Int64Type, nonNilIDs(group.Users))
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	config.ID = types.Int64Value(group.ID)
	config.Label = optionalString(group.Label)
	config.Description = optionalString(group.Description)
	config.Roles = roles
	config.Users = users
	resp.Diagnostics.Append(resp.State.Set(ctx, &config)...)
}

func (d *groupDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	d.client = dataSourceClient(req, &resp.Diagnostics)
}

// Row level security rule

var (
	_ datasource.DataSource              = &rowLevelSecurityDataSource{}
	_ datasource.DataSourceWithConfigure = &rowLevelSecurityDataSource{}
)

// NewRowLevelSecurityDataSource is a helper function to simplify the provider implementation.
func NewRowLevelSecurityDataSource() datasource.DataSource {
	return &rowLevelSecurityDataSource{}
}

// rowLevelSecurityDataSource looks up one row level security rule by its name.
type rowLevelSecurityDataSource struct {
	client *client.Client
}

type rowLevelSecurityDataSourceModel struct {
	ID          types.Int64  `tfsdk:"id"`
	Name        types.String `tfsdk:"name"`
	Description types.String `tfsdk:"description"`
	FilterType  types.String `tfsdk:"filter_type"`
	GroupKey    types.String `tfsdk:"group_key"`
	Clause      types.String `tfsdk:"clause"`
	Tables      types.Set    `tfsdk:"tables"`
	Roles       types.Set    `tfsdk:"roles"`
}

func (d *rowLevelSecurityDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_row_level_security"
}

func (d *rowLevelSecurityDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Looks up a row level security rule by its name, for example to check which datasets and roles a rule that is not managed by Terraform applies to. Rule names are unique; the lookup fails when no rule has exactly that name.",
		Attributes: map[string]schema.Attribute{
			"name": schema.StringAttribute{
				Description: "Name of the rule. The comparison is exact.",
				Required:    true,
			},
			"id": schema.Int64Attribute{
				Description: "Numeric identifier of the rule.",
				Computed:    true,
			},
			"description": schema.StringAttribute{
				Description: "Description of the rule.",
				Computed:    true,
			},
			"filter_type": schema.StringAttribute{
				Description: "`Regular` for a rule that applies to its roles, `Base` for a rule that applies to every role except its own.",
				Computed:    true,
			},
			"group_key": schema.StringAttribute{
				Description: "Group key of the rule: the clauses of rules with the same key are combined with OR.",
				Computed:    true,
			},
			"clause": schema.StringAttribute{
				Description: "SQL clause Superset appends to the queries of the datasets.",
				Computed:    true,
			},
			"tables": schema.SetAttribute{
				Description: "Identifiers of the datasets the rule applies to.",
				Computed:    true,
				ElementType: types.Int64Type,
			},
			"roles": schema.SetAttribute{
				Description: "Identifiers of the roles of the rule.",
				Computed:    true,
				ElementType: types.Int64Type,
			},
		},
	}
}

func (d *rowLevelSecurityDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var config rowLevelSecurityDataSourceModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &config)...)
	if resp.Diagnostics.HasError() {
		return
	}

	name := config.Name.ValueString()
	if name == "" {
		resp.Diagnostics.AddAttributeError(path.Root("name"), "Missing Rule Name", "The rule name must not be empty.")
		return
	}

	rule, err := d.client.FindRowLevelSecurityByName(name)
	if err != nil {
		resp.Diagnostics.AddError("Unable to Read Superset Row Level Security Rule", fmt.Sprintf("Could not look up row level security rule %q: %s", name, err))
		return
	}

	tables, diags := types.SetValueFrom(ctx, types.Int64Type, nonNilIDs(rule.TableIDs))
	resp.Diagnostics.Append(diags...)
	roles, diags := types.SetValueFrom(ctx, types.Int64Type, nonNilIDs(rule.RoleIDs))
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	config.ID = types.Int64Value(rule.ID)
	config.Description = optionalString(rule.Description)
	config.FilterType = types.StringValue(rule.FilterType)
	config.GroupKey = optionalString(rule.GroupKey)
	config.Clause = types.StringValue(rule.Clause)
	config.Tables = tables
	config.Roles = roles
	resp.Diagnostics.Append(resp.State.Set(ctx, &config)...)
}

func (d *rowLevelSecurityDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	d.client = dataSourceClient(req, &resp.Diagnostics)
}
