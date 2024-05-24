package provider

import (
	"github.com/hashicorp/terraform-plugin-framework/providerserver"
	"github.com/hashicorp/terraform-plugin-go/tfprotov6"
	"os"
	"testing"
)

const providerConfig = `
provider "superset" {
  host     = "http://superset-host"
  username = "fake-username"
  password = "fake-password"
}
`

var (
	testAccProtoV6ProviderFactories = map[string]func() (tfprotov6.ProviderServer, error){
		"superset": providerserver.NewProtocol6WithError(New("test")()),
	}
)

func testAccPreCheck(t *testing.T) {
	if v := os.Getenv("SUPERSET_USERNAME"); v == "" {
		t.Fatal("SUPERSET_USERNAME must be set for acceptance tests")
	}
	if v := os.Getenv("SUPERSET_PASSWORD"); v == "" {
		t.Fatal("SUPERSET_PASSWORD must be set for acceptance tests")
	}
	if v := os.Getenv("SUPERSET_HOST"); v == "" {
		t.Fatal("SUPERSET_HOST must be set for acceptance tests")
	}
}
