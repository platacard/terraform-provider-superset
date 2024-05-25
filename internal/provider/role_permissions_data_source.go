package provider

import (
	"context"
	"fmt"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"terraform-provider-superset/internal/client"
)

// Ensure the implementation satisfies the expected interfaces.
var (
	_ datasource.DataSource              = &rolePermissionsDataSource{}
	_ datasource.DataSourceWithConfigure = &rolePermissionsDataSource{}
)

// NewRolePermissionsDataSource is a helper function to simplify the provider implementation.
func NewRolePermissionsDataSource() datasource.DataSource {
	return &rolePermissionsDataSource{}
}

// rolePermissionsDataSource is the data source implementation.
type rolePermissionsDataSource struct {
	client *client.Client
}

// rolePermissionsDataSourceModel maps the data source schema data.
type rolePermissionsDataSourceModel struct {
	RoleName    types.String      `tfsdk:"role_name"`
	Permissions []permissionModel `tfsdk:"permissions"`
}

// permissionModel maps the permission schema data.
type permissionModel struct {
	ID             types.Int64  `tfsdk:"id"`
	PermissionName types.String `tfsdk:"permission_name"`
	ViewMenuName   types.String `tfsdk:"view_menu_name"`
}

// Metadata returns the data source type name.
func (d *rolePermissionsDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_role_permissions"
}

// Schema defines the schema for the data source.
func (d *rolePermissionsDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Fetches the permissions for a role from Superset.",
		Attributes: map[string]schema.Attribute{
			"role_name": schema.StringAttribute{
				Description: "Name of the role.",
				Required:    true,
			},
			"permissions": schema.ListNestedAttribute{
				Description: "List of permissions.",
				Computed:    true,
				NestedObject: schema.NestedAttributeObject{
					Attributes: map[string]schema.Attribute{
						"id": schema.Int64Attribute{
							Description: "Numeric identifier of the permission.",
							Computed:    true,
						},
						"permission_name": schema.StringAttribute{
							Description: "Name of the permission.",
							Computed:    true,
						},
						"view_menu_name": schema.StringAttribute{
							Description: "Name of the view menu associated with the permission.",
							Computed:    true,
						},
					},
				},
			},
		},
	}
}

// Read refreshes the Terraform state with the latest data.
func (d *rolePermissionsDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var state rolePermissionsDataSourceModel

	diags := req.Config.Get(ctx, &state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	roleID, err := d.client.GetRoleIDByName(state.RoleName.ValueString())
	if err != nil {
		resp.Diagnostics.AddError(
			"Unable to Find Role",
			fmt.Sprintf("Unable to find role with name %s: %s", state.RoleName.ValueString(), err.Error()),
		)
		return
	}

	permissions, err := d.client.GetRolePermissions(roleID)
	if err != nil {
		resp.Diagnostics.AddError(
			"Unable to Read Superset Role Permissions",
			err.Error(),
		)
		return
	}

	for _, perm := range permissions {
		state.Permissions = append(state.Permissions, permissionModel{
			ID:             types.Int64Value(perm.ID),
			PermissionName: types.StringValue(perm.PermissionName),
			ViewMenuName:   types.StringValue(perm.ViewMenuName),
		})
	}

	diags = resp.State.Set(ctx, &state)
	resp.Diagnostics.Append(diags...)
}

// Configure adds the provider configured client to the data source.
func (d *rolePermissionsDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
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
