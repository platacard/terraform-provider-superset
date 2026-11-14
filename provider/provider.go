// Package provider exposes the Superset Terraform provider to other Go programs, for example
// to serve it in-process or to embed it in a tool that drives Terraform providers through the
// plugin protocol. The implementation lives in internal/provider.
package provider

import (
	fwprovider "github.com/hashicorp/terraform-plugin-framework/provider"

	"github.com/platacard/terraform-provider-superset/internal/provider"
)

// Address is the registry address of the provider.
const Address = "registry.terraform.io/platacard/superset"

// New returns a function that creates the provider with the given version.
func New(version string) func() fwprovider.Provider {
	return provider.New(version)
}
