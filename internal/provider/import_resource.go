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
	"github.com/platacard/terraform-provider-superset/internal/client"
)

// Ensure the implementation satisfies the expected interfaces.
var (
	_ resource.Resource                   = &importResource{}
	_ resource.ResourceWithConfigure      = &importResource{}
	_ resource.ResourceWithModifyPlan     = &importResource{}
	_ resource.ResourceWithValidateConfig = &importResource{}
)

// importKind describes one of the import resources.
type importKind struct {
	client      client.ImportKind
	typeName    string
	description string
	idsName     string
	idsDesc     string
	datasets    bool
}

var (
	dashboardImportKind = importKind{
		client:      client.DashboardImport,
		typeName:    "_dashboard_import",
		description: "Imports the dashboards of an export bundle together with the charts, datasets and databases they depend on, and imports them again whenever the bundle changes. Destroying the resource deletes the dashboards only. When one export feeds several import resources, order them with `depends_on` (datasets, then charts, then dashboards): Superset refuses to delete a dataset that charts still use.",
		idsName:     "dashboard_ids",
		idsDesc:     "Identifiers of the imported dashboards, keyed by the uuid of their export.",
	}
	chartImportKind = importKind{
		client:      client.ChartImport,
		typeName:    "_chart_import",
		description: "Imports the charts of an export bundle together with the datasets and databases they depend on, and imports them again whenever the bundle changes. Destroying the resource deletes the charts only. When one export feeds several import resources, order them with `depends_on` (datasets, then charts, then dashboards): Superset refuses to delete a dataset that charts still use.",
		idsName:     "chart_ids",
		idsDesc:     "Identifiers of the imported charts, keyed by the uuid of their export.",
	}
	datasetImportKind = importKind{
		client:      client.DatasetImport,
		typeName:    "_dataset_import",
		description: "Imports the datasets of an export bundle together with the databases they depend on, and imports them again whenever the bundle changes. Destroying the resource deletes the datasets only. When one export feeds several import resources, order them with `depends_on` (datasets, then charts, then dashboards): Superset refuses to delete a dataset that charts still use.",
		idsName:     "dataset_ids",
		idsDesc:     "Identifiers of the imported datasets, keyed by the uuid of their export.",
		datasets:    true,
	}
)

// NewDashboardImportResource is a helper function to simplify the provider implementation.
func NewDashboardImportResource() resource.Resource {
	return &importResource{kind: dashboardImportKind}
}

// NewChartImportResource is a helper function to simplify the provider implementation.
func NewChartImportResource() resource.Resource {
	return &importResource{kind: chartImportKind}
}

// NewDatasetImportResource is a helper function to simplify the provider implementation.
func NewDatasetImportResource() resource.Resource {
	return &importResource{kind: datasetImportKind}
}

// importResource imports an export bundle of one kind of object.
type importResource struct {
	client *client.Client
	kind   importKind
}

// Metadata returns the resource type name.
func (r *importResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + r.kind.typeName
}

// Schema defines the schema for the resource.
func (r *importResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	secretMap := func(description string) schema.MapAttribute {
		return schema.MapAttribute{
			Description: description + bundleSecretDescription,
			ElementType: types.StringType,
			Optional:    true,
			Sensitive:   true,
		}
	}
	attributes := map[string]schema.Attribute{
		"source": schema.StringAttribute{
			Description: "Directory in the export layout of Superset (`dashboards/`, `charts/`, `datasets/`, `databases/`), or an export zip file. A directory is bundled by the provider with a `metadata.yaml` of the right type, so one export can feed every import resource. Exactly one of `source` and `files` must be set.",
			Optional:    true,
		},
		"files": bundleFilesAttribute(),
		"overwrite": schema.BoolAttribute{
			Description: "Whether objects that already exist in Superset are replaced. Without it, Superset refuses to import an object that exists. Defaults to `true`.",
			Optional:    true,
			Computed:    true,
			Default:     booldefault.StaticBool(true),
		},
		"database_passwords":               secretMap("Passwords of the databases of the bundle, needed when a database is created by the import."),
		"ssh_tunnel_passwords":             secretMap("Passwords of the SSH tunnels of the databases of the bundle."),
		"ssh_tunnel_private_keys":          secretMap("Private keys of the SSH tunnels of the databases of the bundle."),
		"ssh_tunnel_private_key_passwords": secretMap("Passphrases of the private keys of the SSH tunnels of the databases of the bundle."),
		"content_hash": schema.StringAttribute{
			Description: "Hash of the files of the bundle. A change of the files plans an import.",
			Computed:    true,
		},
		r.kind.idsName: schema.MapAttribute{
			Description: r.kind.idsDesc,
			ElementType: types.Int64Type,
			Computed:    true,
		},
	}
	if r.kind.datasets {
		attributes["sync_columns"] = schema.BoolAttribute{
			Description: "Whether the columns of datasets that already exist are replaced by the columns of the bundle. Defaults to `false`.",
			Optional:    true,
			Computed:    true,
			Default:     booldefault.StaticBool(false),
		}
		attributes["sync_metrics"] = schema.BoolAttribute{
			Description: "Whether the metrics of datasets that already exist are replaced by the metrics of the bundle. Defaults to `false`.",
			Optional:    true,
			Computed:    true,
			Default:     booldefault.StaticBool(false),
		}
	}
	resp.Schema = schema.Schema{Description: r.kind.description, Attributes: attributes}
}

// ModifyPlan computes the hash of the bundle, so that a change of its files plans an import, and
// marks the identifiers unknown when an import is planned.
func (r *importResource) ModifyPlan(ctx context.Context, req resource.ModifyPlanRequest, resp *resource.ModifyPlanResponse) {
	if req.Plan.Raw.IsNull() {
		return
	}

	loaded, diags := loadConfiguredBundle(ctx, req.Config, r.kind.client.Bundle)
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
		resp.Diagnostics.Append(resp.Plan.SetAttribute(ctx, path.Root(r.kind.idsName), types.MapUnknown(types.Int64Type))...)
	}
}

// ValidateConfig checks that the configuration sets exactly one of source and files.
func (r *importResource) ValidateConfig(ctx context.Context, req resource.ValidateConfigRequest, resp *resource.ValidateConfigResponse) {
	resp.Diagnostics.Append(validateBundleAttributes(ctx, req.Config)...)
}

// Create imports the bundle and sets the initial Terraform state.
func (r *importResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	r.apply(ctx, req.Plan, &resp.State, &resp.Diagnostics)
}

// Read checks that the imported objects still exist. A missing object marks the content hash as
// outdated, so the next plan imports the bundle again.
func (r *importResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	ids, diags := r.ids(ctx, req.State)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	for uuid, id := range ids {
		exists, err := r.client.ObjectExists(r.kind.client, id)
		if err != nil {
			resp.Diagnostics.AddError("Unable to Read the Imported Objects", err.Error())
			return
		}
		if !exists {
			tflog.Warn(ctx, "an imported object no longer exists in Superset, the bundle will be imported again", map[string]any{"kind": r.kind.client.Path, "uuid": uuid, "id": id})
			resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("content_hash"), outdatedContentHash)...)
			return
		}
	}
}

// Update imports the bundle again.
func (r *importResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	r.apply(ctx, req.Plan, &resp.State, &resp.Diagnostics)
}

// Delete deletes the imported objects of the kind of the resource; objects that are already gone
// are not an error.
func (r *importResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	ids, diags := r.ids(ctx, req.State)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	for _, id := range ids {
		err := r.client.DeleteObject(r.kind.client, id)
		if err != nil && !errors.Is(err, client.ErrNotFound) {
			resp.Diagnostics.AddError("Unable to Delete the Imported Objects", err.Error())
			return
		}
	}
}

// Configure adds the provider configured client to the resource.
func (r *importResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

// apply imports the bundle described by the plan, locates the imported objects and stores the
// plan with the hash and the identifiers in the state.
func (r *importResource) apply(ctx context.Context, plan tfsdk.Plan, state *tfsdk.State, diags *diag.Diagnostics) {
	options, d := r.options(ctx, plan)
	diags.Append(d...)
	if diags.HasError() {
		return
	}

	loaded, d := loadConfiguredBundle(ctx, plan, r.kind.client.Bundle)
	diags.Append(d...)
	if diags.HasError() {
		return
	}
	if loaded == nil {
		diags.AddError("Unable to Load the Bundle", "The source or files of the bundle are not known.")
		return
	}
	if err := r.client.ImportBundle(r.kind.client, loaded.Content, options); err != nil {
		diags.AddError("Unable to Import the Bundle", err.Error())
		return
	}

	ids := map[string]attr.Value{}
	for _, object := range loaded.Objects {
		id, err := r.client.LocateImported(r.kind.client, object)
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

	diags.Append(state.Set(ctx, plan.Raw)...)
	diags.Append(state.SetAttribute(ctx, path.Root("content_hash"), loaded.Hash)...)
	diags.Append(state.SetAttribute(ctx, path.Root(r.kind.idsName), idsValue)...)
}

// options reads the import options of the plan.
func (r *importResource) options(ctx context.Context, plan tfsdk.Plan) (client.ImportOptions, diag.Diagnostics) {
	options, diags := importSecrets(ctx, plan)
	var overwrite types.Bool
	diags.Append(plan.GetAttribute(ctx, path.Root("overwrite"), &overwrite)...)
	options.Overwrite = overwrite.ValueBool()

	if r.kind.datasets {
		var syncColumns, syncMetrics types.Bool
		diags.Append(plan.GetAttribute(ctx, path.Root("sync_columns"), &syncColumns)...)
		diags.Append(plan.GetAttribute(ctx, path.Root("sync_metrics"), &syncMetrics)...)
		options.SyncColumns, options.SyncMetrics = syncColumns.ValueBool(), syncMetrics.ValueBool()
	}
	return options, diags
}

// ids reads the identifiers of the imported objects from the state.
func (r *importResource) ids(ctx context.Context, state tfsdk.State) (map[string]int64, diag.Diagnostics) {
	var value types.Map
	diags := state.GetAttribute(ctx, path.Root(r.kind.idsName), &value)
	ids := map[string]int64{}
	if !value.IsNull() && !value.IsUnknown() {
		diags.Append(value.ElementsAs(ctx, &ids, false)...)
	}
	return ids, diags
}
