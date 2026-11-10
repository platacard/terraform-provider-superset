package provider_test

import (
	"context"
	"testing"

	fwprovider "github.com/hashicorp/terraform-plugin-framework/provider"

	"github.com/platacard/terraform-provider-superset/provider"
)

func TestNew(t *testing.T) {
	p := provider.New("test")()

	var resp fwprovider.MetadataResponse
	p.Metadata(context.Background(), fwprovider.MetadataRequest{}, &resp)
	if resp.TypeName != "superset" {
		t.Errorf("TypeName = %q, want superset", resp.TypeName)
	}
	if resp.Version != "test" {
		t.Errorf("Version = %q, want test", resp.Version)
	}
	if n := len(p.Resources(context.Background())); n == 0 {
		t.Error("the provider has no resources")
	}
}
