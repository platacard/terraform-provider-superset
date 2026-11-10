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
	_ datasource.DataSource              = &currentUserDataSource{}
	_ datasource.DataSourceWithConfigure = &currentUserDataSource{}
)

// NewCurrentUserDataSource is a helper function to simplify the provider implementation.
func NewCurrentUserDataSource() datasource.DataSource {
	return &currentUserDataSource{}
}

// currentUserDataSource describes the user the provider is authenticated as.
type currentUserDataSource struct {
	client *client.Client
}

type currentUserDataSourceModel struct {
	ID        types.Int64  `tfsdk:"id"`
	Username  types.String `tfsdk:"username"`
	FirstName types.String `tfsdk:"first_name"`
	LastName  types.String `tfsdk:"last_name"`
	Email     types.String `tfsdk:"email"`
	IsActive  types.Bool   `tfsdk:"is_active"`
	Roles     types.List   `tfsdk:"roles"`
}

func (d *currentUserDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_current_user"
}

func (d *currentUserDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Describes the user the provider is authenticated as, on Superset 6, for example to make it the owner of the objects it creates. Older versions only answer these endpoints to a browser session, so the data source fails there.",
		Attributes: map[string]schema.Attribute{
			"id": schema.Int64Attribute{
				Description: "Numeric identifier of the user, for the `owners` of charts, dashboards, datasets and connections.",
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
			"is_active": schema.BoolAttribute{
				Description: "Whether the user is active.",
				Computed:    true,
			},
			"roles": schema.ListAttribute{
				Description: "Names of the roles of the user, sorted.",
				ElementType: types.StringType,
				Computed:    true,
			},
		},
	}
}

func (d *currentUserDataSource) Read(ctx context.Context, _ datasource.ReadRequest, resp *datasource.ReadResponse) {
	user, err := d.client.GetCurrentUser()
	if err != nil {
		resp.Diagnostics.AddError("Unable to Read the Current Superset User", err.Error())
		return
	}

	var diags diag.Diagnostics
	model := currentUserDataSourceModel{
		ID:        types.Int64Value(user.ID),
		Username:  types.StringValue(user.Username),
		FirstName: types.StringValue(user.FirstName),
		LastName:  types.StringValue(user.LastName),
		Email:     types.StringValue(user.Email),
		IsActive:  types.BoolValue(user.IsActive),
	}
	model.Roles, diags = types.ListValueFrom(ctx, types.StringType, user.Roles)
	resp.Diagnostics.Append(diags...)
	resp.Diagnostics.Append(resp.State.Set(ctx, &model)...)
}

func (d *currentUserDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	d.client = dataSourceClient(req, &resp.Diagnostics)
}
