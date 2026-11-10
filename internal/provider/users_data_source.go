package provider

import (
	"context"
	"fmt"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/platacard/terraform-provider-superset/internal/client"
)

// Ensure the implementation satisfies the expected interfaces.
var (
	_ datasource.DataSource              = &usersDataSource{}
	_ datasource.DataSourceWithConfigure = &usersDataSource{}
)

// NewUsersDataSource is a helper function to simplify the provider implementation.
func NewUsersDataSource() datasource.DataSource {
	return &usersDataSource{}
}

// usersDataSource is the data source implementation.
type usersDataSource struct {
	client *client.Client
}

// usersDataSourceModel maps the data source schema data.
type usersDataSourceModel struct {
	Users []userModel `tfsdk:"users"`
}

// userModel maps the user schema data.
type userModel struct {
	ID        types.Int64  `tfsdk:"id"`
	Username  types.String `tfsdk:"username"`
	FirstName types.String `tfsdk:"first_name"`
	LastName  types.String `tfsdk:"last_name"`
	Email     types.String `tfsdk:"email"`
	Active    types.Bool   `tfsdk:"active"`
	Roles     []roleModel  `tfsdk:"roles"`
}

// Metadata returns the data source type name.
func (d *usersDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_users"
}

// Schema defines the schema for the data source.
func (d *usersDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Fetches the list of users from Superset.",
		Attributes: map[string]schema.Attribute{
			"users": schema.ListNestedAttribute{
				Description: "List of users.",
				Computed:    true,
				NestedObject: schema.NestedAttributeObject{
					Attributes: map[string]schema.Attribute{
						"id": schema.Int64Attribute{
							Description: "Numeric identifier of the user.",
							Computed:    true,
						},
						"username": schema.StringAttribute{
							Description: "Username of the user.",
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
							Description: "Whether the user is active.",
							Computed:    true,
						},
						"roles": schema.ListNestedAttribute{
							Description: "List of roles assigned to the user.",
							Computed:    true,
							NestedObject: schema.NestedAttributeObject{
								Attributes: map[string]schema.Attribute{
									"id": schema.Int64Attribute{
										Description: "Numeric identifier of the role.",
										Computed:    true,
									},
									"name": schema.StringAttribute{
										Description: "Name of the role.",
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

// Read refreshes the Terraform state with the latest data.
func (d *usersDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var state usersDataSourceModel

	users, err := d.client.FetchUsers()
	if err != nil {
		resp.Diagnostics.AddError(
			"Unable to Read Superset Users",
			err.Error(),
		)
		return
	}

	for _, user := range users {
		userRoles := make([]roleModel, len(user.Roles))
		for i, role := range user.Roles {
			userRoles[i] = roleModel{
				ID:   types.Int64Value(role.ID),
				Name: types.StringValue(role.Name),
			}
		}

		state.Users = append(state.Users, userModel{
			ID:        types.Int64Value(user.ID),
			Username:  types.StringValue(user.Username),
			FirstName: types.StringValue(user.FirstName),
			LastName:  types.StringValue(user.LastName),
			Email:     types.StringValue(user.Email),
			Active:    types.BoolValue(user.Active),
			Roles:     userRoles,
		})
	}

	diags := resp.State.Set(ctx, &state)
	resp.Diagnostics.Append(diags...)
}

// Configure adds the provider configured client to the data source.
func (d *usersDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
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
