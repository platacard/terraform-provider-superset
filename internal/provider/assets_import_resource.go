package provider

import (
	"context"
	"errors"
	"fmt"

	"github.com/hashicorp/terraform-plugin-framework/attr"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/booldefault"
	"github.com/hashicorp/terraform-plugin-framework/tfsdk"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
	"github.com/platacard/terraform-provider-superset/internal/bundle"
	"github.com/platacard/terraform-provider-superset/internal/client"
)

// Ensure the implementation satisfies the expected interfaces.
var (
	_ resource.Resource                   = &assetsImportResource{}
	_ resource.ResourceWithConfigure      = &assetsImportResource{}
	_ resource.ResourceWithModifyPlan     = &assetsImportResource{}
	_ resource.ResourceWithValidateConfig = &assetsImportResource{}
)

// assetKinds are the kinds of objects whose identifiers the resource records, in the order they
// are deleted: dashboards first, since Superset refuses to delete a dataset that charts use.
var assetKinds = []struct {
	client  client.ImportKind
	idsName string
}{
	{client: client.DashboardImport, idsName: "dashboard_ids"},
	{client: client.ChartImport, idsName: "chart_ids"},
	{client: client.DatasetImport, idsName: "dataset_ids"},
}

// NewAssetsImportResource is a helper function to simplify the provider implementation.
func NewAssetsImportResource() resource.Resource {
	return &assetsImportResource{}
}

// assetsImportResource imports a whole export bundle through the assets endpoint.
type assetsImportResource struct {
	client *client.Client
}

// Metadata returns the resource type name.
func (r *assetsImportResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_assets_import"
}

// Schema defines the schema for the resource.
func (r *assetsImportResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	secretMap := func(description string) schema.MapAttribute {
		return schema.MapAttribute{
			Description: description + bundleSecretDescription,
			ElementType: types.StringType,
			Optional:    true,
			Sensitive:   true,
		}
	}
	idsMap := func(description string) schema.MapAttribute {
		return schema.MapAttribute{
			Description: description,
			ElementType: types.Int64Type,
			Computed:    true,
		}
	}
	resp.Schema = schema.Schema{
		Description: "Imports a whole export bundle at once through the assets endpoint: its databases, datasets, charts, dashboards and saved queries, overwriting the ones that exist, and imports it again whenever its files change. Destroying the resource deletes the imported dashboards, charts and datasets, in that order; the databases stay.",
		Attributes: map[string]schema.Attribute{
			"source": schema.StringAttribute{
				Description: "Directory in the export layout of Superset (`dashboards/`, `charts/`, `datasets/`, `databases/`, `queries/`), or an export zip file. A directory is bundled by the provider with a `metadata.yaml` of the `assets` type. Exactly one of `source` and `files` must be set.",
				Optional:    true,
			},
			"files": bundleFilesAttribute(),
			"sparse": schema.BoolAttribute{
				Description: "Whether the dashboards of the bundle may reference charts that exist in Superset but are not in the bundle. Recent versions of Superset only. Defaults to `false`.",
				Optional:    true,
				Computed:    true,
				Default:     booldefault.StaticBool(false),
			},
			"database_passwords":               secretMap("Passwords of the databases of the bundle, needed when a database is created by the import."),
			"ssh_tunnel_passwords":             secretMap("Passwords of the SSH tunnels of the databases of the bundle."),
			"ssh_tunnel_private_keys":          secretMap("Private keys of the SSH tunnels of the databases of the bundle."),
			"ssh_tunnel_private_key_passwords": secretMap("Passphrases of the private keys of the SSH tunnels of the databases of the bundle."),
			"content_hash": schema.StringAttribute{
				Description: "Hash of the files of the bundle. A change of the files plans an import.",
				Computed:    true,
			},
			"dashboard_ids": idsMap("Identifiers of the imported dashboards, keyed by the uuid of their export."),
			"chart_ids":     idsMap("Identifiers of the imported charts, keyed by the uuid of their export."),
			"dataset_ids":   idsMap("Identifiers of the imported datasets, keyed by the uuid of their export."),
		},
	}
}

// ModifyPlan computes the hash of the bundle, so that a change of its files plans an import, and
// marks the identifiers unknown when an import is planned.
func (r *assetsImportResource) ModifyPlan(ctx context.Context, req resource.ModifyPlanRequest, resp *resource.ModifyPlanResponse) {
	if req.Plan.Raw.IsNull() {
		return
	}

	loaded, diags := loadConfiguredBundle(ctx, req.Config, bundle.Assets)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() || loaded == nil {
		return
	}
	resp.Diagnostics.Append(resp.Plan.SetAttribute(ctx, path.Root("content_hash"), loaded.Hash)...)

	var priorHash types.String
	if !req.State.Raw.IsNull() {
		resp.Diagnostics.Append(req.State.GetAttribute(ctx, path.Root("content_hash"), &priorHash)...)
	}
	if priorHash.ValueString() != loaded.Hash {
		for _, kind := range assetKinds {
			resp.Diagnostics.Append(resp.Plan.SetAttribute(ctx, path.Root(kind.idsName), types.MapUnknown(types.Int64Type))...)
		}
	}
}

// ValidateConfig checks that the configuration sets exactly one of source and files.
func (r *assetsImportResource) ValidateConfig(ctx context.Context, req resource.ValidateConfigRequest, resp *resource.ValidateConfigResponse) {
	resp.Diagnostics.Append(validateBundleAttributes(ctx, req.Config)...)
}

// Create imports the bundle and sets the initial Terraform state.
func (r *assetsImportResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	r.apply(ctx, req.Plan, &resp.State, &resp.Diagnostics)
}

// Read checks that the imported objects still exist. A missing object marks the content hash as
// outdated, so the next plan imports the bundle again.
func (r *assetsImportResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	for _, kind := range assetKinds {
		ids, diags := importedIDs(ctx, req.State, kind.idsName)
		resp.Diagnostics.Append(diags...)
		if resp.Diagnostics.HasError() {
			return
		}
		for uuid, id := range ids {
			exists, err := r.client.ObjectExists(kind.client, id)
			if err != nil {
				resp.Diagnostics.AddError("Unable to Read the Imported Objects", err.Error())
				return
			}
			if !exists {
				tflog.Warn(ctx, "an imported object no longer exists in Superset, the bundle will be imported again", map[string]any{"kind": kind.client.Path, "uuid": uuid, "id": id})
				resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("content_hash"), outdatedContentHash)...)
				return
			}
		}
	}
}

// Update imports the bundle again.
func (r *assetsImportResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	r.apply(ctx, req.Plan, &resp.State, &resp.Diagnostics)
}

// Delete deletes the imported dashboards, charts and datasets, in that order; objects that are
// already gone are not an error.
func (r *assetsImportResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	for _, kind := range assetKinds {
		ids, diags := importedIDs(ctx, req.State, kind.idsName)
		resp.Diagnostics.Append(diags...)
		if resp.Diagnostics.HasError() {
			return
		}
		for _, id := range ids {
			err := r.client.DeleteObject(kind.client, id)
			if err != nil && !errors.Is(err, client.ErrNotFound) {
				resp.Diagnostics.AddError("Unable to Delete the Imported Objects", err.Error())
				return
			}
		}
	}
}

// Configure adds the provider configured client to the resource.
func (r *assetsImportResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}

	client, ok := req.ProviderData.(*client.Client)
	if !ok {
		resp.Diagnostics.AddError(
			"Unexpected Resource Configure Type",
			fmt.Sprintf("Expected *client.Client, got: %T. Please report this issue to the provider developers.", req.ProviderData),
		)
		return
	}

	r.client = client
}

// apply imports the bundle described by the plan, locates the imported objects of every kind
// and stores the plan with the hash and the identifiers in the state.
func (r *assetsImportResource) apply(ctx context.Context, plan tfsdk.Plan, state *tfsdk.State, diags *diag.Diagnostics) {
	var sparse types.Bool
	diags.Append(plan.GetAttribute(ctx, path.Root("sparse"), &sparse)...)
	options, d := importSecrets(ctx, plan)
	diags.Append(d...)
	if diags.HasError() {
		return
	}

	loaded, d := loadConfiguredBundle(ctx, plan, bundle.Assets)
	diags.Append(d...)
	if diags.HasError() {
		return
	}
	if loaded == nil {
		diags.AddError("Unable to Load the Bundle", "The source or files of the bundle are not known.")
		return
	}
	if err := r.client.ImportAssets(loaded.Content, options, sparse.ValueBool()); err != nil {
		diags.AddError("Unable to Import the Bundle", err.Error())
		return
	}

	diags.Append(state.Set(ctx, plan.Raw)...)
	diags.Append(state.SetAttribute(ctx, path.Root("content_hash"), loaded.Hash)...)
	for _, kind := range assetKinds {
		objects, err := bundle.Objects(loaded.Content, kind.client.Bundle)
		if err != nil {
			diags.AddAttributeError(path.Root("source"), "Unable to Read the Bundle", err.Error())
			return
		}
		ids := map[string]attr.Value{}
		for _, object := range objects {
			id, err := r.client.LocateImported(kind.client, object)
			if err != nil {
				diags.AddError("Unable to Locate an Imported Object", err.Error())
				return
			}
			ids[object.UUID] = types.Int64Value(id)
		}
		idsValue, d := types.MapValue(types.Int64Type, ids)
		diags.Append(d...)
		if diags.HasError() {
			return
		}
		diags.Append(state.SetAttribute(ctx, path.Root(kind.idsName), idsValue)...)
	}
}

// importedIDs reads the identifiers of the imported objects of one kind from the state.
func importedIDs(ctx context.Context, state tfsdk.State, idsName string) (map[string]int64, diag.Diagnostics) {
	var value types.Map
	diags := state.GetAttribute(ctx, path.Root(idsName), &value)
	ids := map[string]int64{}
	if !value.IsNull() && !value.IsUnknown() {
		diags.Append(value.ElementsAs(ctx, &ids, false)...)
	}
	return ids, diags
}
