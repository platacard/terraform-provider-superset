package provider

import (
	"context"
	"os"

	"terraform-provider-superset/internal/client"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/provider/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
)

// Ensure the implementation satisfies the expected interfaces.
var (
	_ provider.Provider = &supersetProvider{}
)

// New is a helper function to simplify provider server and testing implementation.
func New(version string) func() provider.Provider {
	return func() provider.Provider {
		return &supersetProvider{
			version: version,
		}
	}
}

// supersetProvider is the provider implementation.
type supersetProvider struct {
	version string
}

// supersetProviderModel maps provider schema data to a Go type.
type supersetProviderModel struct {
	Host     types.String `tfsdk:"host"`
	Username types.String `tfsdk:"username"`
	Password types.String `tfsdk:"password"`
	// OIDC client credentials authentication
	OIDCTokenURL  types.String `tfsdk:"oidc_token_url"`
	ClientID      types.String `tfsdk:"client_id"`
	ClientSecret  types.String `tfsdk:"client_secret"`
}

// Metadata returns the provider type name.
func (p *supersetProvider) Metadata(_ context.Context, _ provider.MetadataRequest, resp *provider.MetadataResponse) {
	resp.TypeName = "superset"
	resp.Version = p.version
}

// Schema defines the provider-level schema for configuration data.
func (p *supersetProvider) Schema(_ context.Context, _ provider.SchemaRequest, resp *provider.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Superset provider for managing Superset resources. " +
			"Supports two authentication methods: database auth (username/password) or " +
			"OIDC client credentials (for OAuth/Keycloak setups).",
		Attributes: map[string]schema.Attribute{
			"host": schema.StringAttribute{
				Description: "The URL of the Superset instance. This should include the protocol (http or https) and the hostname or IP address. Example: 'https://superset.example.com'.",
				Optional:    true,
			},
			// Database authentication (legacy)
			"username": schema.StringAttribute{
				Description: "The username to authenticate with Superset using database auth. " +
					"Use this for Superset instances configured with AUTH_TYPE = AUTH_DB. " +
					"For OIDC/OAuth setups, use client_id and client_secret instead.",
				Optional: true,
			},
			"password": schema.StringAttribute{
				Description: "The password to authenticate with Superset using database auth. " +
					"This value is sensitive and will not be displayed in logs or state files.",
				Optional:  true,
				Sensitive: true,
			},
			// OIDC client credentials authentication
			"oidc_token_url": schema.StringAttribute{
				Description: "The OIDC token endpoint URL for client credentials authentication. " +
					"Example: 'https://keycloak.example.com/realms/myrealm/protocol/openid-connect/token'. " +
					"Required when using OIDC authentication.",
				Optional: true,
			},
			"client_id": schema.StringAttribute{
				Description: "The OIDC client ID for client credentials authentication. " +
					"The client must have 'Service Accounts Enabled' in the identity provider.",
				Optional: true,
			},
			"client_secret": schema.StringAttribute{
				Description: "The OIDC client secret for client credentials authentication. " +
					"This value is sensitive and will not be displayed in logs or state files.",
				Optional:  true,
				Sensitive: true,
			},
		},
	}
}

// Configure prepares a Superset API client for data sources and resources.
func (p *supersetProvider) Configure(ctx context.Context, req provider.ConfigureRequest, resp *provider.ConfigureResponse) {
	tflog.Info(ctx, "Configuring Superset client")

	// Retrieve provider data from configuration
	var config supersetProviderModel
	diags := req.Config.Get(ctx, &config)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Check for unknown values
	if config.Host.IsUnknown() {
		resp.Diagnostics.AddAttributeError(
			path.Root("host"),
			"Unknown Superset API Host",
			"The provider cannot create the Superset API client as there is an unknown configuration value for the Superset API host. "+
				"Either target apply the source of the value first, set the value statically in the configuration, or use the SUPERSET_HOST environment variable.",
		)
	}

	if resp.Diagnostics.HasError() {
		return
	}

	// Default values to environment variables, but override with Terraform configuration value if set.
	host := os.Getenv("SUPERSET_HOST")
	username := os.Getenv("SUPERSET_USERNAME")
	password := os.Getenv("SUPERSET_PASSWORD")
	oidcTokenURL := os.Getenv("SUPERSET_OIDC_TOKEN_URL")
	clientID := os.Getenv("SUPERSET_CLIENT_ID")
	clientSecret := os.Getenv("SUPERSET_CLIENT_SECRET")

	if !config.Host.IsNull() {
		host = config.Host.ValueString()
	}
	if !config.Username.IsNull() {
		username = config.Username.ValueString()
	}
	if !config.Password.IsNull() {
		password = config.Password.ValueString()
	}
	if !config.OIDCTokenURL.IsNull() {
		oidcTokenURL = config.OIDCTokenURL.ValueString()
	}
	if !config.ClientID.IsNull() {
		clientID = config.ClientID.ValueString()
	}
	if !config.ClientSecret.IsNull() {
		clientSecret = config.ClientSecret.ValueString()
	}

	// Validate host is always required
	if host == "" {
		resp.Diagnostics.AddAttributeError(
			path.Root("host"),
			"Missing Superset API Host",
			"The provider cannot create the Superset API client as there is a missing or empty value for the Superset API host. "+
				"Set the host value in the configuration or use the SUPERSET_HOST environment variable.",
		)
	}

	// Determine authentication method: OIDC takes precedence if configured
	useOIDC := oidcTokenURL != "" && clientID != "" && clientSecret != ""
	useDBAuth := username != "" && password != ""

	if !useOIDC && !useDBAuth {
		resp.Diagnostics.AddError(
			"Missing Authentication Configuration",
			"The provider requires authentication credentials. Configure either:\n\n"+
				"1. OIDC client credentials (recommended for OAuth/Keycloak):\n"+
				"   - oidc_token_url (or SUPERSET_OIDC_TOKEN_URL env var)\n"+
				"   - client_id (or SUPERSET_CLIENT_ID env var)\n"+
				"   - client_secret (or SUPERSET_CLIENT_SECRET env var)\n\n"+
				"2. Database authentication (for AUTH_DB setups):\n"+
				"   - username (or SUPERSET_USERNAME env var)\n"+
				"   - password (or SUPERSET_PASSWORD env var)",
		)
	}

	if resp.Diagnostics.HasError() {
		return
	}

	// Add structured log fields (mask sensitive values)
	ctx = tflog.SetField(ctx, "superset_host", host)
	ctx = tflog.SetField(ctx, "auth_method", map[bool]string{true: "oidc", false: "database"}[useOIDC])
	ctx = tflog.MaskFieldValuesWithFieldKeys(ctx, "superset_password")
	ctx = tflog.MaskFieldValuesWithFieldKeys(ctx, "client_secret")

	tflog.Debug(ctx, "Creating Superset client")

	// Create a new Superset client using the appropriate authentication method
	var supersetClient *client.Client
	var err error

	if useOIDC {
		tflog.Info(ctx, "Using OIDC client credentials authentication")
		supersetClient, err = client.NewClientWithOIDC(host, oidcTokenURL, clientID, clientSecret)
	} else {
		tflog.Info(ctx, "Using database authentication")
		supersetClient, err = client.NewClient(host, username, password)
	}

	if err != nil {
		resp.Diagnostics.AddError(
			"Unable to Create Superset API Client",
			"An unexpected error occurred when creating the Superset API client. "+
				"If the error is not clear, please contact the provider developers.\n\n"+
				"Superset Client Error: "+err.Error(),
		)
		return
	}

	// Make the Superset client available during DataSource and Resource type Configure methods.
	resp.DataSourceData = supersetClient
	resp.ResourceData = supersetClient

	tflog.Info(ctx, "Configured Superset client", map[string]any{"success": true})
}

// DataSources defines the data sources implemented in the provider.
func (p *supersetProvider) DataSources(_ context.Context) []func() datasource.DataSource {
	return []func() datasource.DataSource{
		NewRolesDataSource,           // Existing data source
		NewRolePermissionsDataSource, // New data source
		NewDatabasesDataSource,       // New databases data source
		NewDatasetsDataSource,        // New datasets data source
		NewUsersDataSource,           // New users data source
	}
}

// Resources defines the resources implemented in the provider.
func (p *supersetProvider) Resources(_ context.Context) []func() resource.Resource {
	return []func() resource.Resource{
		NewRoleResource,            // Role resource
		NewRolePermissionsResource, // Role permissions resource
		NewDatabaseResource,        // Database resource
		NewMetaDatabaseResource,    // Meta database resource
		NewDatasetResource,         // Dataset resource
		NewUserResource,            // User resource
		NewChartResource,           // Chart resource
		NewDashboardResource,       // Dashboard resource
	}
}
