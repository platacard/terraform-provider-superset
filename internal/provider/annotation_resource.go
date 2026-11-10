package provider

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
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
	_ resource.Resource                   = &annotationResource{}
	_ resource.ResourceWithConfigure      = &annotationResource{}
	_ resource.ResourceWithImportState    = &annotationResource{}
	_ resource.ResourceWithValidateConfig = &annotationResource{}
)

// annotationDttm is the format Superset stores and returns the instants of an annotation in:
// ISO 8601 without a time zone, to the second.
var annotationDttm = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$`)

// NewAnnotationResource is a helper function to simplify the provider implementation.
func NewAnnotationResource() resource.Resource {
	return &annotationResource{}
}

// annotationResource manages an annotation of an annotation layer.
type annotationResource struct {
	client *client.Client
}

// annotationResourceModel maps the resource schema data.
type annotationResourceModel struct {
	ID           types.Int64  `tfsdk:"id"`
	LayerID      types.Int64  `tfsdk:"layer_id"`
	ShortDescr   types.String `tfsdk:"short_descr"`
	LongDescr    types.String `tfsdk:"long_descr"`
	StartDttm    types.String `tfsdk:"start_dttm"`
	EndDttm      types.String `tfsdk:"end_dttm"`
	JSONMetadata jsonValue    `tfsdk:"json_metadata"`
}

// Metadata returns the resource type name.
func (r *annotationResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_annotation"
}

// Schema defines the schema for the resource.
func (r *annotationResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Manages an annotation of an annotation layer: an event, or a period, that charts show on their time axis when they display the layer.",
		Attributes: map[string]schema.Attribute{
			"id": schema.Int64Attribute{
				Description: "Numeric identifier of the annotation.",
				Computed:    true,
				PlanModifiers: []planmodifier.Int64{
					int64planmodifier.UseStateForUnknown(),
				},
			},
			"layer_id": schema.Int64Attribute{
				Description: "Identifier of the annotation layer. Changing it replaces the annotation.",
				Required:    true,
				PlanModifiers: []planmodifier.Int64{
					int64planmodifier.RequiresReplace(),
				},
			},
			"short_descr": schema.StringAttribute{
				Description: "Title of the annotation, unique within the layer.",
				Required:    true,
			},
			"long_descr": schema.StringAttribute{
				Description: "Free-text description of the annotation.",
				Optional:    true,
			},
			"start_dttm": schema.StringAttribute{
				Description: "Start of the annotation, as `YYYY-MM-DDTHH:MM:SS` without a time zone, for example `2026-01-15T00:00:00`.",
				Required:    true,
			},
			"end_dttm": schema.StringAttribute{
				Description: "End of the annotation, in the same format, not before the start. Equal to the start for an instant.",
				Required:    true,
			},
			"json_metadata": schema.StringAttribute{
				Description: "JSON document with extra settings of the annotation.",
				Optional:    true,
				CustomType:  jsonDocument,
			},
		},
	}
}

// ValidateConfig checks the format of the instants, which Superset stores without a time zone
// and returns in exactly this form, so any other spelling would show up as a change.
func (r *annotationResource) ValidateConfig(ctx context.Context, req resource.ValidateConfigRequest, resp *resource.ValidateConfigResponse) {
	var config annotationResourceModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &config)...)
	if resp.Diagnostics.HasError() {
		return
	}

	for name, value := range map[string]types.String{"start_dttm": config.StartDttm, "end_dttm": config.EndDttm} {
		if value.IsNull() || value.IsUnknown() {
			continue
		}
		if !annotationDttm.MatchString(value.ValueString()) {
			resp.Diagnostics.AddAttributeError(path.Root(name), "Invalid Annotation Instant",
				fmt.Sprintf("%s must be written as YYYY-MM-DDTHH:MM:SS without a time zone, for example 2026-01-15T00:00:00, got %q.", name, value.ValueString()))
		}
	}
}

// Create creates the annotation and sets the initial Terraform state.
func (r *annotationResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan annotationResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	id, err := r.client.CreateAnnotation(annotationFromModel(plan))
	if err != nil {
		resp.Diagnostics.AddError("Unable to Create Superset Annotation", err.Error())
		return
	}

	r.readInto(ctx, plan.LayerID.ValueInt64(), id, &resp.State, &resp.Diagnostics, "Unable to Read Superset Annotation After Creation")
}

// Read refreshes the Terraform state with the annotation as it is in Superset.
func (r *annotationResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state annotationResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	annotation, err := r.client.GetAnnotation(state.LayerID.ValueInt64(), state.ID.ValueInt64())
	if errors.Is(err, client.ErrNotFound) {
		tflog.Warn(ctx, "annotation no longer exists in Superset, removing it from the state", map[string]any{"id": state.ID.ValueInt64()})
		resp.State.RemoveResource(ctx)
		return
	}
	if err != nil {
		resp.Diagnostics.AddError("Unable to Read Superset Annotation", err.Error())
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, annotationModel(annotation))...)
}

// Update replaces the attributes of the annotation.
func (r *annotationResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state annotationResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	if err := r.client.UpdateAnnotation(state.ID.ValueInt64(), annotationFromModel(plan)); err != nil {
		resp.Diagnostics.AddError("Unable to Update Superset Annotation", err.Error())
		return
	}

	r.readInto(ctx, plan.LayerID.ValueInt64(), state.ID.ValueInt64(), &resp.State, &resp.Diagnostics, "Unable to Read Superset Annotation After Update")
}

// Delete deletes the annotation; one that is already gone is not an error.
func (r *annotationResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state annotationResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	err := r.client.DeleteAnnotation(state.LayerID.ValueInt64(), state.ID.ValueInt64())
	if err != nil && !errors.Is(err, client.ErrNotFound) {
		resp.Diagnostics.AddError("Unable to Delete Superset Annotation", err.Error())
		return
	}
}

// ImportState imports an annotation by `<layer id>/<annotation id>`.
func (r *annotationResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	parts := strings.Split(req.ID, "/")
	var ids [2]int64
	valid := len(parts) == 2
	for i := 0; valid && i < 2; i++ {
		id, err := strconv.ParseInt(parts[i], 10, 64)
		valid, ids[i] = err == nil, id
	}
	if !valid {
		resp.Diagnostics.AddError("Invalid Import ID", fmt.Sprintf("The import ID %q must be <layer id>/<annotation id>, for example 2/5.", req.ID))
		return
	}
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("layer_id"), ids[0])...)
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("id"), ids[1])...)
}

// Configure adds the provider configured client to the resource.
func (r *annotationResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

// readInto fetches the annotation and stores it in the state after a create or an update.
func (r *annotationResource) readInto(ctx context.Context, layerID, id int64, state stateSetter, diags *diag.Diagnostics, summary string) {
	annotation, err := r.client.GetAnnotation(layerID, id)
	if err != nil {
		diags.AddError(summary, err.Error())
		return
	}
	diags.Append(state.Set(ctx, annotationModel(annotation))...)
}

func annotationFromModel(model annotationResourceModel) client.Annotation {
	return client.Annotation{
		LayerID:      model.LayerID.ValueInt64(),
		ShortDescr:   model.ShortDescr.ValueString(),
		LongDescr:    model.LongDescr.ValueString(),
		StartDttm:    model.StartDttm.ValueString(),
		EndDttm:      model.EndDttm.ValueString(),
		JSONMetadata: model.JSONMetadata.ValueString(),
	}
}

func annotationModel(annotation *client.Annotation) *annotationResourceModel {
	return &annotationResourceModel{
		ID:           types.Int64Value(annotation.ID),
		LayerID:      types.Int64Value(annotation.LayerID),
		ShortDescr:   types.StringValue(annotation.ShortDescr),
		LongDescr:    optionalString(annotation.LongDescr),
		StartDttm:    types.StringValue(annotation.StartDttm),
		EndDttm:      types.StringValue(annotation.EndDttm),
		JSONMetadata: jsonDocument.value(annotation.JSONMetadata),
	}
}
