package provider

import (
	"os"
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/tfsdk"
	"github.com/hashicorp/terraform-plugin-go/tftypes"
	"github.com/jarcoal/httpmock"
	"github.com/platacard/terraform-provider-superset/internal/client"
)

type testProviderConfig struct {
	host, username, password *string
	validateCredentials      *bool
	csrfEnabled              *bool
	authProvider             *string
}

func strPtr(v string) *string { return &v }
func boolPtr(v bool) *bool    { return &v }

// configureProvider runs Configure with the given attribute values (nil means the attribute is not set).
func configureProvider(t *testing.T, cfg testProviderConfig) *provider.ConfigureResponse {
	t.Helper()
	ctx := t.Context()

	// Make sure environment variables do not leak into the test
	t.Setenv("SUPERSET_HOST", "")
	t.Setenv("SUPERSET_USERNAME", "")
	t.Setenv("SUPERSET_PASSWORD", "")
	if os.Getenv("SUPERSET_AUTH_PROVIDER") == "" {
		t.Setenv("SUPERSET_AUTH_PROVIDER", "")
	}

	p := New("test")()
	schemaResp := &provider.SchemaResponse{}
	p.Schema(ctx, provider.SchemaRequest{}, schemaResp)

	str := func(v *string) tftypes.Value {
		if v == nil {
			return tftypes.NewValue(tftypes.String, nil)
		}
		return tftypes.NewValue(tftypes.String, *v)
	}
	validate := tftypes.NewValue(tftypes.Bool, nil)
	if cfg.validateCredentials != nil {
		validate = tftypes.NewValue(tftypes.Bool, *cfg.validateCredentials)
	}
	csrf := tftypes.NewValue(tftypes.Bool, nil)
	if cfg.csrfEnabled != nil {
		csrf = tftypes.NewValue(tftypes.Bool, *cfg.csrfEnabled)
	}
	raw := tftypes.NewValue(schemaResp.Schema.Type().TerraformType(ctx), map[string]tftypes.Value{
		"host":                 str(cfg.host),
		"username":             str(cfg.username),
		"password":             str(cfg.password),
		"validate_credentials": validate,
		"csrf_enabled":         csrf,
		"auth_provider":        str(cfg.authProvider),
	})

	resp := &provider.ConfigureResponse{}
	p.Configure(ctx, provider.ConfigureRequest{Config: tfsdk.Config{Schema: schemaResp.Schema, Raw: raw}}, resp)
	return resp
}

// configuredClient returns the client that Configure stored for resources and data sources.
func configuredClient(t *testing.T, resp *provider.ConfigureResponse) *client.Client {
	t.Helper()
	c, ok := resp.ResourceData.(*client.Client)
	if !ok {
		t.Fatalf("expected a *client.Client as resource data, got %T", resp.ResourceData)
	}
	return c
}

func registerTestLogin() {
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))
}

func TestConfigureValidatesCredentialsByDefault(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	registerTestLogin()

	resp := configureProvider(t, testProviderConfig{
		host: strPtr("http://superset-host"), username: strPtr("user"), password: strPtr("pass"),
	})

	if resp.Diagnostics.HasError() {
		t.Fatalf("expected no errors, got: %v", resp.Diagnostics)
	}
	if n := httpmock.GetTotalCallCount(); n != 1 {
		t.Fatalf("expected a login request on configure, got %d requests", n)
	}
}

func TestConfigureFailsOnMissingSettingsByDefault(t *testing.T) {
	resp := configureProvider(t, testProviderConfig{host: strPtr("http://superset-host")})

	if !resp.Diagnostics.HasError() {
		t.Fatal("expected errors for missing username and password")
	}
}

func TestConfigureWithoutValidationSendsNoRequests(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	registerTestLogin()

	for name, cfg := range map[string]testProviderConfig{
		"empty settings": {validateCredentials: boolPtr(false)},
		"full settings": {
			host: strPtr("http://superset-host"), username: strPtr("user"), password: strPtr("pass"),
			validateCredentials: boolPtr(false),
		},
	} {
		t.Run(name, func(t *testing.T) {
			httpmock.ZeroCallCounters()
			resp := configureProvider(t, cfg)

			if resp.Diagnostics.HasError() {
				t.Fatalf("expected no errors, got: %v", resp.Diagnostics)
			}
			if resp.ResourceData == nil || resp.DataSourceData == nil {
				t.Fatal("expected the client to be passed to resources and data sources")
			}
			if n := httpmock.GetTotalCallCount(); n != 0 {
				t.Fatalf("expected no requests on configure, got %d", n)
			}
		})
	}
}

func TestConfigureCSRFEnabled(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	registerTestLogin()

	for name, tc := range map[string]struct {
		csrfEnabled *bool
		want        bool
	}{
		"default":  {nil, true},
		"enabled":  {boolPtr(true), true},
		"disabled": {boolPtr(false), false},
	} {
		t.Run(name, func(t *testing.T) {
			resp := configureProvider(t, testProviderConfig{
				host: strPtr("http://superset-host"), username: strPtr("user"), password: strPtr("pass"),
				csrfEnabled: tc.csrfEnabled,
			})

			if resp.Diagnostics.HasError() {
				t.Fatalf("expected no errors, got: %v", resp.Diagnostics)
			}
			c, ok := resp.ResourceData.(*client.Client)
			if !ok {
				t.Fatalf("expected *client.Client, got %T", resp.ResourceData)
			}
			if c.CSRFEnabled != tc.want {
				t.Fatalf("expected CSRFEnabled=%v, got %v", tc.want, c.CSRFEnabled)
			}
		})
	}
}

func TestConfigureAuthProvider(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	registerTestLogin()
	base := testProviderConfig{host: strPtr("http://superset-host"), username: strPtr("user"), password: strPtr("pass")}

	resp := configureProvider(t, base)
	if resp.Diagnostics.HasError() {
		t.Fatalf("expected no errors, got: %v", resp.Diagnostics)
	}
	if got := configuredClient(t, resp).AuthProvider; got != "" {
		t.Fatalf("expected the default provider, got %q", got)
	}

	cfg := base
	cfg.authProvider = strPtr("ldap")
	resp = configureProvider(t, cfg)
	if resp.Diagnostics.HasError() {
		t.Fatalf("expected no errors, got: %v", resp.Diagnostics)
	}
	if got := configuredClient(t, resp).AuthProvider; got != "ldap" {
		t.Fatalf("expected the ldap provider, got %q", got)
	}

	t.Setenv("SUPERSET_AUTH_PROVIDER", "oauth")
	resp = configureProvider(t, base)
	if !resp.Diagnostics.HasError() {
		t.Fatal("expected an error for an unsupported provider in the environment")
	}

	t.Setenv("SUPERSET_AUTH_PROVIDER", "ldap")
	resp = configureProvider(t, base)
	if resp.Diagnostics.HasError() {
		t.Fatalf("expected no errors, got: %v", resp.Diagnostics)
	}
	if got := configuredClient(t, resp).AuthProvider; got != "ldap" {
		t.Fatalf("expected the ldap provider from the environment, got %q", got)
	}
}
