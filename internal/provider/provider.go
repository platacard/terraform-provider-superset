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
}

// Metadata returns the provider type name.
func (p *supersetProvider) Metadata(_ context.Context, _ provider.MetadataRequest, resp *provider.MetadataResponse) {
	resp.TypeName = "superset"
	resp.Version = p.version
}

// Schema defines the provider-level schema for configuration data.
func (p *supersetProvider) Schema(_ context.Context, _ provider.SchemaRequest, resp *provider.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Superset provider for managing Superset resources.",
		Attributes: map[string]schema.Attribute{
			"host": schema.StringAttribute{
				Description: "The URL of the Superset instance. This should include the protocol (http or https) and the hostname or IP address. Example: 'https://superset.example.com'.",
				Optional:    true,
			},
			"username": schema.StringAttribute{
				Description: "The username to authenticate with Superset. This user should have the necessary permissions to manage resources within Superset.",
				Optional:    true,
			},
			"password": schema.StringAttribute{
				Description: "The password to authenticate with Superset. This value is sensitive and will not be displayed in logs or state files.",
				Optional:    true,
				Sensitive:   true,
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

	// If practitioner provided a configuration value for any of the attributes, it must be a known value.
	if config.Host.IsUnknown() {
		resp.Diagnostics.AddAttributeError(
			path.Root("host"),
			"Unknown Superset API Host",
			"The provider cannot create the Superset API client as there is an unknown configuration value for the Superset API host. "+
				"Either target apply the source of the value first, set the value statically in the configuration, or use the SUPERSET_HOST environment variable.",
		)
	}

	if config.Username.IsUnknown() {
		resp.Diagnostics.AddAttributeError(
			path.Root("username"),
			"Unknown Superset API Username",
			"The provider cannot create the Superset API client as there is an unknown configuration value for the Superset API username. "+
				"Either target apply the source of the value first, set the value statically in the configuration, or use the SUPERSET_USERNAME environment variable.",
		)
	}

	if config.Password.IsUnknown() {
		resp.Diagnostics.AddAttributeError(
			path.Root("password"),
			"Unknown Superset API Password",
			"The provider cannot create the Superset API client as there is an unknown configuration value for the Superset API password. "+
				"Either target apply the source of the value first, set the value statically in the configuration, or use the SUPERSET_PASSWORD environment variable.",
		)
	}

	if resp.Diagnostics.HasError() {
		return
	}

	// Default values to environment variables, but override with Terraform configuration value if set.
	host := os.Getenv("SUPERSET_HOST")
	username := os.Getenv("SUPERSET_USERNAME")
	password := os.Getenv("SUPERSET_PASSWORD")

	if !config.Host.IsNull() {
		host = config.Host.ValueString()
	}

	if !config.Username.IsNull() {
		username = config.Username.ValueString()
	}

	if !config.Password.IsNull() {
		password = config.Password.ValueString()
	}

	// If any of the expected configurations are missing, return errors with provider-specific guidance.
	if host == "" {
		resp.Diagnostics.AddAttributeError(
			path.Root("host"),
			"Missing Superset API Host",
			"The provider cannot create the Superset API client as there is a missing or empty value for the Superset API host. "+
				"Set the host value in the configuration or use the SUPERSET_HOST environment variable. "+
				"If either is already set, ensure the value is not empty.",
		)
	}

	if username == "" {
		resp.Diagnostics.AddAttributeError(
			path.Root("username"),
			"Missing Superset API Username",
			"The provider cannot create the Superset API client as there is a missing or empty value for the Superset API username. "+
				"Set the username value in the configuration or use the SUPERSET_USERNAME environment variable. "+
				"If either is already set, ensure the value is not empty.",
		)
	}

	if password == "" {
		resp.Diagnostics.AddAttributeError(
			path.Root("password"),
			"Missing Superset API Password",
			"The provider cannot create the Superset API client as there is a missing or empty value for the Superset API password. "+
				"Set the password value in the configuration or use the SUPERSET_PASSWORD environment variable. "+
				"If either is already set, ensure the value is not empty.",
		)
	}

	if resp.Diagnostics.HasError() {
		return
	}

	// Add structured log fields
	ctx = tflog.SetField(ctx, "superset_host", host)
	ctx = tflog.SetField(ctx, "superset_username", username)
	ctx = tflog.SetField(ctx, "superset_password", password)
	ctx = tflog.MaskFieldValuesWithFieldKeys(ctx, "superset_username")
	ctx = tflog.MaskFieldValuesWithFieldKeys(ctx, "superset_password")

	tflog.Debug(ctx, "Creating Superset client")

	// Create a new Superset client using the configuration values
	client, err := client.NewClient(host, username, password)
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
	resp.DataSourceData = client
	resp.ResourceData = client

	tflog.Info(ctx, "Configured Superset client", map[string]any{"success": true})
}

// DataSources defines the data sources implemented in the provider.
func (p *supersetProvider) DataSources(_ context.Context) []func() datasource.DataSource {
	return []func() datasource.DataSource{
		NewRolesDataSource,           // Existing data source
		NewRolePermissionsDataSource, // New data source
	}
}

// Resources defines the resources implemented in the provider.
func (p *supersetProvider) Resources(_ context.Context) []func() resource.Resource {
	return []func() resource.Resource{
		NewRoleResource,            // New resource
		NewRolePermissionsResource, // New resource
	}
}
