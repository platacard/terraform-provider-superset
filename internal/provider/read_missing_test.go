package provider

import (
	"context"
	"math/big"
	"net/http"
	"regexp"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/providerserver"
	"github.com/hashicorp/terraform-plugin-go/tfprotov6"
	"github.com/hashicorp/terraform-plugin-go/tftypes"
	"github.com/jarcoal/httpmock"
)

// bundleResources import a bundle rather than manage one object. Their read checks the
// objects of the last import and plans another import when one is missing, so they keep a
// state; TestAccDashboardImportResource covers that.
var bundleResources = map[string]bool{
	"superset_assets_import":    true,
	"superset_dashboard_import": true,
	"superset_chart_import":     true,
	"superset_dataset_import":   true,
}

// serveNothing makes the fake Superset host answer like an instance that has no objects:
// filtered lists are empty and every object is "Not found".
func serveNothing() {
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewJsonResponderOrPanic(200, map[string]string{"access_token": "token"}))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/csrf_token/",
		httpmock.NewJsonResponderOrPanic(200, map[string]string{"result": "csrf"}))
	httpmock.RegisterRegexpResponder("GET", regexp.MustCompile(`^http://superset-host/api/v1/.*/(\?.*)?$`),
		httpmock.NewJsonResponderOrPanic(200, map[string]interface{}{"count": 0, "ids": []int{}, "result": []interface{}{}}))
	httpmock.RegisterNoResponder(func(req *http.Request) (*http.Response, error) {
		return httpmock.NewJsonResponse(http.StatusNotFound, map[string]string{"message": "Not found"})
	})
}

// placeholder returns a known value of the type, for the required attributes of a state.
func placeholder(typ tftypes.Type) tftypes.Value {
	switch {
	case typ.Is(tftypes.String):
		return tftypes.NewValue(typ, "{}") // also a valid JSON document
	case typ.Is(tftypes.Number):
		return tftypes.NewValue(typ, big.NewFloat(7))
	case typ.Is(tftypes.Bool):
		return tftypes.NewValue(typ, false)
	case typ.Is(tftypes.List{}), typ.Is(tftypes.Set{}), typ.Is(tftypes.Map{}):
		return tftypes.NewValue(typ, nil)
	default:
		return tftypes.NewValue(typ, nil)
	}
}

// Every resource that manages one Superset object must report the object as gone when
// Superset answers "Not found", instead of failing. Terraform then plans to create the object
// again after it was deleted outside Terraform, and programs that read an object before they
// create it, such as Crossplane, work at all.
func TestReadOfAMissingObjectRemovesTheResource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	serveNothing()

	ctx := context.Background()
	server := providerserver.NewProtocol6(New("test")())()
	schemas, err := server.GetProviderSchema(ctx, &tfprotov6.GetProviderSchemaRequest{})
	if err != nil {
		t.Fatal(err)
	}
	providerConfig, err := tfprotov6.NewDynamicValue(schemas.Provider.ValueType(), tftypes.NewValue(schemas.Provider.ValueType(), map[string]tftypes.Value{
		"host":                 tftypes.NewValue(tftypes.String, "http://superset-host"),
		"username":             tftypes.NewValue(tftypes.String, "user"),
		"password":             tftypes.NewValue(tftypes.String, "pass"),
		"validate_credentials": tftypes.NewValue(tftypes.Bool, true),
		"csrf_enabled":         tftypes.NewValue(tftypes.Bool, true),
		"auth_provider":        tftypes.NewValue(tftypes.String, nil),
	}))
	if err != nil {
		t.Fatal(err)
	}
	configured, err := server.ConfigureProvider(ctx, &tfprotov6.ConfigureProviderRequest{Config: &providerConfig})
	if err != nil || hasErrorDiagnostic(configured.Diagnostics) {
		t.Fatalf("configure: %v %v", err, configured.Diagnostics)
	}

	for name, s := range schemas.ResourceSchemas {
		if bundleResources[name] {
			continue
		}
		t.Run(name, func(t *testing.T) {
			objectType := s.ValueType().(tftypes.Object)
			values := map[string]tftypes.Value{}
			for attr, typ := range objectType.AttributeTypes {
				values[attr] = tftypes.NewValue(typ, nil)
			}
			for _, a := range s.Block.Attributes {
				if a.Required || a.Name == "id" {
					values[a.Name] = placeholder(objectType.AttributeTypes[a.Name])
				}
			}
			state, err := tfprotov6.NewDynamicValue(objectType, tftypes.NewValue(objectType, values))
			if err != nil {
				t.Fatal(err)
			}
			resp, err := server.ReadResource(ctx, &tfprotov6.ReadResourceRequest{TypeName: name, CurrentState: &state})
			if err != nil {
				t.Fatal(err)
			}
			if hasErrorDiagnostic(resp.Diagnostics) {
				t.Fatalf("Read failed instead of reporting the object as gone: %s", diagnosticsText(resp.Diagnostics))
			}
			got, err := resp.NewState.Unmarshal(objectType)
			if err != nil {
				t.Fatal(err)
			}
			if !got.IsNull() {
				t.Fatalf("Read kept the resource in the state although Superset answered Not found")
			}
		})
	}
}

func hasErrorDiagnostic(diags []*tfprotov6.Diagnostic) bool {
	for _, d := range diags {
		if d != nil && d.Severity == tfprotov6.DiagnosticSeverityError {
			return true
		}
	}
	return false
}

func diagnosticsText(diags []*tfprotov6.Diagnostic) string {
	var parts []string
	for _, d := range diags {
		if d != nil {
			parts = append(parts, d.Summary+": "+d.Detail)
		}
	}
	return strings.Join(parts, "; ")
}
