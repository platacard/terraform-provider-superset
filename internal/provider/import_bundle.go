package provider

import (
	"context"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/types"

	"github.com/platacard/terraform-provider-superset/internal/bundle"
	"github.com/platacard/terraform-provider-superset/internal/client"
)

// attributeGetter reads one attribute of a configuration, a plan or a state.
type attributeGetter interface {
	GetAttribute(ctx context.Context, p path.Path, target interface{}) diag.Diagnostics
}

// bundleFilesAttribute is the files attribute of the import resources: the bundle given inline,
// for runs that have no file system to read an export from.
func bundleFilesAttribute() schema.MapAttribute {
	return schema.MapAttribute{
		Description: "The files of the bundle, keyed by their path in the export layout, such as `dashboards/sales.yaml` or `databases/warehouse.yaml`, " +
			"with their YAML content as values. The provider zips them with a `metadata.yaml` of the right type, as it does for a directory in `source`. " +
			"Use it instead of `source` where the provider cannot read files, for example when it runs in a controller. Exactly one of `source` and `files` must be set.",
		ElementType: types.StringType,
		Optional:    true,
	}
}

// bundleSecretDescription is the key description of the secret maps of the import resources.
const bundleSecretDescription = " Keyed by the path of the database file inside the bundle, such as `databases/warehouse.yaml`, or by the file name alone, such as `warehouse.yaml`."

// loadConfiguredBundle loads the bundle that the source or files attribute describes. It returns
// a nil bundle without an error when the attribute is not known yet.
func loadConfiguredBundle(ctx context.Context, from attributeGetter, kind bundle.Kind) (*bundle.Bundle, diag.Diagnostics) {
	var source types.String
	var files types.Map
	diags := from.GetAttribute(ctx, path.Root("source"), &source)
	diags.Append(from.GetAttribute(ctx, path.Root("files"), &files)...)
	if diags.HasError() || source.IsUnknown() || files.IsUnknown() {
		return nil, diags
	}

	switch {
	case !source.IsNull() && !files.IsNull():
		diags.AddAttributeError(path.Root("files"), "Conflicting Bundle Attributes", "Set either source or files, not both.")
	case !source.IsNull():
		loaded, err := bundle.Load(source.ValueString(), kind)
		if err != nil {
			diags.AddAttributeError(path.Root("source"), "Unable to Load the Bundle", err.Error())
			return nil, diags
		}
		return loaded, diags
	case !files.IsNull():
		contents := map[string]string{}
		if diags.Append(files.ElementsAs(ctx, &contents, false)...); diags.HasError() {
			return nil, diags
		}
		loaded, err := bundle.FromFiles(contents, kind)
		if err != nil {
			diags.AddAttributeError(path.Root("files"), "Unable to Build the Bundle", err.Error())
			return nil, diags
		}
		return loaded, diags
	default:
		diags.AddAttributeError(path.Root("source"), "Missing Bundle", "Set either source, a directory or zip file with the export, or files, the files of the export.")
	}
	return nil, diags
}

// validateBundleAttributes reports a configuration that sets both or neither of source and files.
func validateBundleAttributes(ctx context.Context, config attributeGetter) diag.Diagnostics {
	var source types.String
	var files types.Map
	diags := config.GetAttribute(ctx, path.Root("source"), &source)
	diags.Append(config.GetAttribute(ctx, path.Root("files"), &files)...)
	if diags.HasError() || source.IsUnknown() || files.IsUnknown() {
		return diags
	}
	switch {
	case !source.IsNull() && !files.IsNull():
		diags.AddAttributeError(path.Root("files"), "Conflicting Bundle Attributes", "Set either source or files, not both.")
	case source.IsNull() && files.IsNull():
		diags.AddAttributeError(path.Root("source"), "Missing Bundle", "Set either source, a directory or zip file with the export, or files, the files of the export.")
	}
	return diags
}

// importSecrets reads the passwords and SSH tunnel secrets of the plan. A key that names a file
// without its directory, such as warehouse.yaml, is the database file databases/warehouse.yaml,
// which allows keys that cannot contain a slash, such as the keys of a Kubernetes Secret.
func importSecrets(ctx context.Context, plan attributeGetter) (client.ImportOptions, diag.Diagnostics) {
	var diags diag.Diagnostics
	var options client.ImportOptions
	secrets := map[string]*map[string]string{
		"database_passwords":               &options.DatabasePasswords,
		"ssh_tunnel_passwords":             &options.SSHTunnelPasswords,
		"ssh_tunnel_private_keys":          &options.SSHTunnelPrivateKeys,
		"ssh_tunnel_private_key_passwords": &options.SSHTunnelPrivateKeyPasswords,
	}
	for name, target := range secrets {
		var value types.Map
		diags.Append(plan.GetAttribute(ctx, path.Root(name), &value)...)
		if value.IsNull() || value.IsUnknown() {
			continue
		}
		values := map[string]string{}
		if diags.Append(value.ElementsAs(ctx, &values, false)...); diags.HasError() {
			continue
		}
		*target = make(map[string]string, len(values))
		for key, secret := range values {
			(*target)[databaseFileKey(key)] = secret
		}
	}
	return options, diags
}

// databaseFileKey returns the path in the bundle of a database file given by its path or by its
// file name alone.
func databaseFileKey(key string) string {
	if strings.Contains(key, "/") {
		return key
	}
	return "databases/" + key
}

// outdatedContentHash replaces the content hash of an import in the state when one of its
// imported objects no longer exists in Superset. It differs from every real hash, so the next
// plan imports the bundle again. The hash is not cleared, and the identifiers of the other objects
// stay, so that the resource still counts as created and a destroy still deletes them.
const outdatedContentHash = "outdated"
