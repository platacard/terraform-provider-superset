package provider

import (
	"context"
	"errors"
	"fmt"

	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/int64planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
	"github.com/platacard/terraform-provider-superset/internal/client"
)

// Ensure the implementation satisfies the expected interfaces.
var (
	_ resource.Resource              = &datasetRefreshResource{}
	_ resource.ResourceWithConfigure = &datasetRefreshResource{}
)

// NewDatasetRefreshResource is a helper function to simplify the provider implementation.
func NewDatasetRefreshResource() resource.Resource {
	return &datasetRefreshResource{}
}

// datasetRefreshResource syncs the columns of a dataset from its source.
type datasetRefreshResource struct {
	client *client.Client
}

// datasetRefreshResourceModel maps the resource schema data.
type datasetRefreshResourceModel struct {
	ID        types.Int64 `tfsdk:"id"`
	DatasetID types.Int64 `tfsdk:"dataset_id"`
	Triggers  types.Map   `tfsdk:"triggers"`
}

// Metadata returns the resource type name.
func (r *datasetRefreshResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_dataset_refresh"
}

// Schema defines the schema for the resource.
func (r *datasetRefreshResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Syncs the columns of a dataset from its source, as the *Sync columns from source* button of the dataset editor does: Superset adds the columns the source gained, removes the ones it lost, updates the type of the ones it has, and recreates the `count` metric when it is missing. Calculated columns and the labels of the columns are kept, while `groupby` and `filterable` are reset to `true`. The sync runs when the resource is created and again whenever `dataset_id` or `triggers` change; it is not undone when the resource is destroyed.",
		Attributes: map[string]schema.Attribute{
			"id": schema.Int64Attribute{
				Description: "Numeric identifier of the dataset.",
				Computed:    true,
				PlanModifiers: []planmodifier.Int64{
					int64planmodifier.UseStateForUnknown(),
				},
			},
			"dataset_id": schema.Int64Attribute{
				Description: "Numeric identifier of the dataset to sync.",
				Required:    true,
				PlanModifiers: []planmodifier.Int64{
					int64planmodifier.RequiresReplace(),
				},
			},
			"triggers": schema.MapAttribute{
				Description: "Arbitrary values whose change runs the sync again, for example the version of the source table.",
				ElementType: types.StringType,
				Optional:    true,
			},
		},
	}
}

// Create runs the sync.
func (r *datasetRefreshResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan datasetRefreshResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	if err := r.client.RefreshDataset(plan.DatasetID.ValueInt64()); err != nil {
		resp.Diagnostics.AddError("Unable to Sync the Columns of the Superset Dataset", err.Error())
		return
	}

	plan.ID = plan.DatasetID
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

// Read checks that the dataset still exists, so the sync runs again for a dataset that was recreated.
func (r *datasetRefreshResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state datasetRefreshResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	_, err := r.client.GetDataset(state.DatasetID.ValueInt64())
	if errors.Is(err, client.ErrNotFound) {
		tflog.Warn(ctx, "dataset no longer exists in Superset, removing its sync from the state", map[string]any{"id": state.DatasetID.ValueInt64()})
		resp.State.RemoveResource(ctx)
		return
	}
	if err != nil {
		resp.Diagnostics.AddError("Unable to Read Superset Dataset", err.Error())
		return
	}
}

// Update runs the sync again: only a change of triggers updates the resource in place.
func (r *datasetRefreshResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan datasetRefreshResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	if err := r.client.RefreshDataset(plan.DatasetID.ValueInt64()); err != nil {
		resp.Diagnostics.AddError("Unable to Sync the Columns of the Superset Dataset", err.Error())
		return
	}

	plan.ID = plan.DatasetID
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

// Delete only forgets the resource: a sync cannot be undone.
func (r *datasetRefreshResource) Delete(_ context.Context, _ resource.DeleteRequest, _ *resource.DeleteResponse) {
}

// Configure adds the provider configured client to the resource.
func (r *datasetRefreshResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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
