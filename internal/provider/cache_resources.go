package provider

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/int64planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/setplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
	"github.com/platacard/terraform-provider-superset/internal/client"
)

// The cache resources run an action against the cache of Superset when they are created and
// whenever their triggers change: warming up the cache of a chart, or dropping the cached
// results of datasets. There is nothing to destroy.

// Chart cache warm-up

var (
	_ resource.Resource              = &chartCacheWarmupResource{}
	_ resource.ResourceWithConfigure = &chartCacheWarmupResource{}
)

// NewChartCacheWarmupResource is a helper function to simplify the provider implementation.
func NewChartCacheWarmupResource() resource.Resource {
	return &chartCacheWarmupResource{}
}

// chartCacheWarmupResource computes a chart and stores its result in the cache.
type chartCacheWarmupResource struct {
	client *client.Client
}

type chartCacheWarmupResourceModel struct {
	ID          types.Int64  `tfsdk:"id"`
	ChartID     types.Int64  `tfsdk:"chart_id"`
	DashboardID types.Int64  `tfsdk:"dashboard_id"`
	Triggers    types.Map    `tfsdk:"triggers"`
	VizStatus   types.String `tfsdk:"viz_status"`
	VizError    types.String `tfsdk:"viz_error"`
}

func (r *chartCacheWarmupResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_chart_cache_warmup"
}

func (r *chartCacheWarmupResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Computes a chart and stores its result in the cache of Superset, so the first person to open it does not wait for the query. The warm-up runs when the resource is created and whenever `dashboard_id` or `triggers` change, for example after the data was loaded. The chart needs a query context, which charts saved in the UI have and `superset_chart.query_context` sets; a chart that cannot be computed does not fail the apply: the reason is reported as a warning and in `viz_error`. Destroying the resource does nothing.",
		Attributes: map[string]schema.Attribute{
			"id": schema.Int64Attribute{
				Description: "Identifier of the chart, which identifies the resource as well.",
				Computed:    true,
				PlanModifiers: []planmodifier.Int64{
					int64planmodifier.UseStateForUnknown(),
				},
			},
			"chart_id": schema.Int64Attribute{
				Description: "Identifier of the chart to compute. A change makes a new resource.",
				Required:    true,
				PlanModifiers: []planmodifier.Int64{
					int64planmodifier.RequiresReplace(),
				},
			},
			"dashboard_id": schema.Int64Attribute{
				Description: "Identifier of a dashboard whose filters apply to the chart, so the cached result is the one the dashboard shows.",
				Optional:    true,
			},
			"triggers": schema.MapAttribute{
				Description: "Arbitrary values whose change runs the warm-up again, for example the date of the last load.",
				ElementType: types.StringType,
				Optional:    true,
			},
			"viz_status": schema.StringAttribute{
				Description: "Status Superset reported for the last run, `success` when the result was cached; null when the chart could not be computed.",
				Computed:    true,
			},
			"viz_error": schema.StringAttribute{
				Description: "Reason the chart could not be computed in the last run; null when it was.",
				Computed:    true,
			},
		},
	}
}

func (r *chartCacheWarmupResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan chartCacheWarmupResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}
	if !r.warmUp(ctx, &plan, resp.Diagnostics.AddError, resp.Diagnostics.AddWarning) {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

// Read keeps the state while the chart exists; a chart that is gone removes the resource,
// so the warm-up runs again once the chart exists again.
func (r *chartCacheWarmupResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state chartCacheWarmupResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	if _, err := r.client.GetChart(state.ChartID.ValueInt64()); err != nil {
		if errors.Is(err, client.ErrNotFound) {
			tflog.Warn(ctx, "The chart no longer exists; the warm-up will run again once it does", map[string]any{"chart_id": state.ChartID.ValueInt64()})
			resp.State.RemoveResource(ctx)
			return
		}
		resp.Diagnostics.AddError("Unable to Read Superset Chart", fmt.Sprintf("Could not read chart %d: %s", state.ChartID.ValueInt64(), err))
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &state)...)
}

// Update runs the warm-up again when the dashboard or the triggers changed.
func (r *chartCacheWarmupResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state chartCacheWarmupResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	if plan.DashboardID.Equal(state.DashboardID) && plan.Triggers.Equal(state.Triggers) {
		plan.VizStatus, plan.VizError = state.VizStatus, state.VizError
	} else if !r.warmUp(ctx, &plan, resp.Diagnostics.AddError, resp.Diagnostics.AddWarning) {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

// Delete does nothing: a cached result cannot be un-warmed, and need not be.
func (r *chartCacheWarmupResource) Delete(context.Context, resource.DeleteRequest, *resource.DeleteResponse) {
}

// warmUp runs the warm-up and stores its outcome in the model.
func (r *chartCacheWarmupResource) warmUp(ctx context.Context, model *chartCacheWarmupResourceModel, addError, addWarning func(summary, detail string)) bool {
	chartID := model.ChartID.ValueInt64()
	outcome, err := r.client.WarmUpChartCache(chartID, model.DashboardID.ValueInt64Pointer())
	if err != nil {
		addError("Unable to Warm Up the Cache of the Superset Chart", fmt.Sprintf("Could not warm up the cache of chart %d: %s", chartID, err))
		return false
	}
	tflog.Debug(ctx, "Warmed up the cache of the chart", map[string]any{"chart_id": chartID, "viz_status": outcome.VizStatus, "viz_error": outcome.VizError})
	model.ID = types.Int64Value(chartID)
	model.VizStatus = types.StringPointerValue(outcome.VizStatus)
	model.VizError = types.StringPointerValue(outcome.VizError)
	if outcome.VizError != nil {
		addWarning("Superset Chart Not Computed", fmt.Sprintf("Chart %d could not be computed for the cache: %s", chartID, *outcome.VizError))
	}
	return true
}

func (r *chartCacheWarmupResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.client = resourceClient(req, &resp.Diagnostics)
}

// Cache invalidation

var (
	_ resource.Resource              = &cacheInvalidationResource{}
	_ resource.ResourceWithConfigure = &cacheInvalidationResource{}
)

// NewCacheInvalidationResource is a helper function to simplify the provider implementation.
func NewCacheInvalidationResource() resource.Resource {
	return &cacheInvalidationResource{}
}

// cacheInvalidationResource drops the cached results of datasets.
type cacheInvalidationResource struct {
	client *client.Client
}

type cacheInvalidationResourceModel struct {
	ID         types.String `tfsdk:"id"`
	DatasetIDs types.Set    `tfsdk:"dataset_ids"`
	Triggers   types.Map    `tfsdk:"triggers"`
}

func (r *cacheInvalidationResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_cache_invalidation"
}

func (r *cacheInvalidationResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Drops the cached results of datasets, so their charts query the database again, for example after the data was reloaded. The invalidation runs when the resource is created and whenever `triggers` change. Datasets that do not exist are ignored by Superset. Destroying the resource does nothing.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Description: "Identifier of the resource, made of the dataset ids.",
				Computed:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"dataset_ids": schema.SetAttribute{
				Description: "Identifiers of the datasets whose cached results are dropped. A change makes a new resource.",
				Required:    true,
				ElementType: types.Int64Type,
				PlanModifiers: []planmodifier.Set{
					setplanmodifier.RequiresReplace(),
				},
			},
			"triggers": schema.MapAttribute{
				Description: "Arbitrary values whose change runs the invalidation again, for example the date of the last load.",
				ElementType: types.StringType,
				Optional:    true,
			},
		},
	}
}

func (r *cacheInvalidationResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan cacheInvalidationResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}
	if !r.invalidate(ctx, &plan, resp.Diagnostics.AddError) {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

// Read keeps the state: there is nothing to read back about an invalidation.
func (r *cacheInvalidationResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state cacheInvalidationResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &state)...)
}

// Update runs the invalidation again when the triggers changed.
func (r *cacheInvalidationResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state cacheInvalidationResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	if plan.Triggers.Equal(state.Triggers) {
		plan.ID = state.ID
	} else if !r.invalidate(ctx, &plan, resp.Diagnostics.AddError) {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

// Delete does nothing: a dropped cache cannot be restored, and need not be.
func (r *cacheInvalidationResource) Delete(context.Context, resource.DeleteRequest, *resource.DeleteResponse) {
}

// invalidate drops the cached results of the datasets of the model and sets its id.
func (r *cacheInvalidationResource) invalidate(ctx context.Context, model *cacheInvalidationResourceModel, addError func(summary, detail string)) bool {
	var ids []int64
	if diags := model.DatasetIDs.ElementsAs(ctx, &ids, false); diags.HasError() {
		addError("Invalid Dataset Identifiers", "The dataset ids could not be read.")
		return false
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	uids := make([]string, 0, len(ids))
	for _, id := range ids {
		uids = append(uids, fmt.Sprintf("%d__table", id))
	}
	if err := r.client.InvalidateCache(uids); err != nil {
		addError("Unable to Invalidate the Superset Cache", fmt.Sprintf("Could not drop the cached results of datasets %s: %s", strings.Join(uids, ", "), err))
		return false
	}
	tflog.Debug(ctx, "Invalidated the cache of the datasets", map[string]any{"datasource_uids": uids})
	model.ID = types.StringValue(strings.Join(uids, ","))
	return true
}

func (r *cacheInvalidationResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.client = resourceClient(req, &resp.Diagnostics)
}

// resourceClient takes the client out of the provider data, for the resources.
func resourceClient(req resource.ConfigureRequest, diags interface {
	AddError(summary, detail string)
}) *client.Client {
	if req.ProviderData == nil {
		return nil
	}
	c, ok := req.ProviderData.(*client.Client)
	if !ok {
		diags.AddError("Unexpected Resource Configure Type", fmt.Sprintf("Expected *client.Client, got: %T. Please report this issue to the provider developers.", req.ProviderData))
		return nil
	}
	return c
}
