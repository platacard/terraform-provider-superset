package provider

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/hashicorp/terraform-plugin-framework/attr"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/booldefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/int64planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/setplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
	"github.com/platacard/terraform-provider-superset/internal/client"
)

// Ensure the implementation satisfies the expected interfaces.
var (
	_ resource.Resource                   = &reportResource{}
	_ resource.ResourceWithConfigure      = &reportResource{}
	_ resource.ResourceWithImportState    = &reportResource{}
	_ resource.ResourceWithValidateConfig = &reportResource{}
)

// NewReportResource is a helper function to simplify the provider implementation.
func NewReportResource() resource.Resource {
	return &reportResource{}
}

// reportResource manages a report or an alert.
type reportResource struct {
	client *client.Client
}

// reportResourceModel maps the resource schema data.
type reportResourceModel struct {
	ID                 types.Int64   `tfsdk:"id"`
	Name               types.String  `tfsdk:"name"`
	Type               types.String  `tfsdk:"type"`
	Description        types.String  `tfsdk:"description"`
	Crontab            types.String  `tfsdk:"crontab"`
	Timezone           types.String  `tfsdk:"timezone"`
	ChartID            types.Int64   `tfsdk:"chart_id"`
	DashboardID        types.Int64   `tfsdk:"dashboard_id"`
	ReportFormat       types.String  `tfsdk:"report_format"`
	Active             types.Bool    `tfsdk:"active"`
	ContextMarkdown    types.String  `tfsdk:"context_markdown"`
	EmailSubject       types.String  `tfsdk:"email_subject"`
	CustomWidth        types.Int64   `tfsdk:"custom_width"`
	ForceScreenshot    types.Bool    `tfsdk:"force_screenshot"`
	WorkingTimeout     types.Int64   `tfsdk:"working_timeout"`
	GracePeriod        types.Int64   `tfsdk:"grace_period"`
	LogRetention       types.Int64   `tfsdk:"log_retention"`
	Owners             types.Set     `tfsdk:"owners"`
	Extra              jsonValue     `tfsdk:"extra"`
	Recipients         types.Set     `tfsdk:"recipients"`
	DatabaseName       types.String  `tfsdk:"database_name"`
	SQL                types.String  `tfsdk:"sql"`
	ValidatorType      types.String  `tfsdk:"validator_type"`
	ValidatorOp        types.String  `tfsdk:"validator_op"`
	ValidatorThreshold types.Float64 `tfsdk:"validator_threshold"`
}

// reportRecipientModel maps one element of the recipients attribute.
type reportRecipientModel struct {
	Type   types.String `tfsdk:"type"`
	Target types.String `tfsdk:"target"`
	CC     types.String `tfsdk:"cc"`
	BCC    types.String `tfsdk:"bcc"`
}

var reportRecipientAttributeTypes = map[string]attr.Type{
	"type":   types.StringType,
	"target": types.StringType,
	"cc":     types.StringType,
	"bcc":    types.StringType,
}

// Metadata returns the resource type name.
func (r *reportResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_report"
}

// Schema defines the schema for the resource.
func (r *reportResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Manages a report or an alert of the Alerts & Reports page: a chart or a dashboard sent to recipients on a schedule, or, for an alert, when a SQL query meets a condition. The `ALERT_REPORTS` feature flag must be on, and Superset needs its workers to actually send anything.",
		Attributes: map[string]schema.Attribute{
			"id": schema.Int64Attribute{
				Description: "Numeric identifier of the report.",
				Computed:    true,
				PlanModifiers: []planmodifier.Int64{
					int64planmodifier.UseStateForUnknown(),
				},
			},
			"name": schema.StringAttribute{
				Description: "Name of the report, unique among the reports or among the alerts.",
				Required:    true,
			},
			"type": schema.StringAttribute{
				Description: "`Report` sends the content on every run; `Alert` runs the SQL query and sends the content when the condition is met.",
				Required:    true,
			},
			"description": schema.StringAttribute{
				Description: "Free-text description of the report.",
				Optional:    true,
			},
			"crontab": schema.StringAttribute{
				Description: "Schedule as a cron expression, for example `0 9 * * 1` for every Monday at 09:00.",
				Required:    true,
			},
			"timezone": schema.StringAttribute{
				Description: "Time zone of the schedule, for example `Europe/Berlin`. Defaults to the one Superset picks, usually `UTC`.",
				Optional:    true,
				Computed:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"chart_id": schema.Int64Attribute{
				Description: "Identifier of the chart to send. Set either `chart_id` or `dashboard_id`.",
				Optional:    true,
			},
			"dashboard_id": schema.Int64Attribute{
				Description: "Identifier of the dashboard to send. Set either `chart_id` or `dashboard_id`.",
				Optional:    true,
			},
			"report_format": schema.StringAttribute{
				Description: "Format of the content: `PNG`, `CSV`, `TEXT`, or `PDF` on recent versions of Superset. Defaults to `PNG`.",
				Optional:    true,
				Computed:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"active": schema.BoolAttribute{
				Description: "Whether the schedule runs. Defaults to `true`.",
				Optional:    true,
				Computed:    true,
				Default:     booldefault.StaticBool(true),
			},
			"context_markdown": schema.StringAttribute{
				Description: "Markdown text sent with the content.",
				Optional:    true,
			},
			"email_subject": schema.StringAttribute{
				Description: "Subject of the email, on recent versions of Superset.",
				Optional:    true,
			},
			"custom_width": schema.Int64Attribute{
				Description: "Width in pixels of the screenshot of the content.",
				Optional:    true,
			},
			"force_screenshot": schema.BoolAttribute{
				Description: "Whether the screenshot is always taken fresh instead of served from the cache. Defaults to `false`.",
				Optional:    true,
				Computed:    true,
				Default:     booldefault.StaticBool(false),
			},
			"working_timeout": schema.Int64Attribute{
				Description: "Seconds a run may take before it is marked as failed. Defaults to the value Superset picks, usually 3600.",
				Optional:    true,
				Computed:    true,
				PlanModifiers: []planmodifier.Int64{
					int64planmodifier.UseStateForUnknown(),
				},
			},
			"grace_period": schema.Int64Attribute{
				Description: "Seconds during which an alert that was already sent is not sent again. Defaults to the value Superset picks, usually 14400.",
				Optional:    true,
				Computed:    true,
				PlanModifiers: []planmodifier.Int64{
					int64planmodifier.UseStateForUnknown(),
				},
			},
			"log_retention": schema.Int64Attribute{
				Description: "Days the execution logs are kept. Defaults to the value Superset picks, usually 90.",
				Optional:    true,
				Computed:    true,
				PlanModifiers: []planmodifier.Int64{
					int64planmodifier.UseStateForUnknown(),
				},
			},
			"owners": schema.SetAttribute{
				Description: "Identifiers of the users who own the report. Defaults to the user the provider authenticates as.",
				ElementType: types.Int64Type,
				Optional:    true,
				Computed:    true,
				PlanModifiers: []planmodifier.Set{
					setplanmodifier.UseStateForUnknown(),
				},
			},
			"extra": schema.StringAttribute{
				Description: "JSON document with extra settings, such as the tabs of a dashboard to send: `{\"dashboard\": {\"activeTabs\": [\"TAB-id\"]}}`.",
				Optional:    true,
				CustomType:  jsonDocument,
			},
			"recipients": schema.SetNestedAttribute{
				Description: "Destinations of the report, at least one. Superset 3.1 ignores an empty list, so the recipients can only be replaced, not all removed.",
				Required:    true,
				NestedObject: schema.NestedAttributeObject{
					Attributes: map[string]schema.Attribute{
						"type": schema.StringAttribute{
							Description: "`Email` or `Slack`; recent versions of Superset also take `SlackV2` and `Webhook`.",
							Required:    true,
						},
						"target": schema.StringAttribute{
							Description: "Addresses separated by commas, a Slack channel, or a webhook URL.",
							Required:    true,
						},
						"cc": schema.StringAttribute{
							Description: "Addresses in copy of an email, on recent versions of Superset.",
							Optional:    true,
						},
						"bcc": schema.StringAttribute{
							Description: "Addresses in blind copy of an email, on recent versions of Superset.",
							Optional:    true,
						},
					},
				},
			},
			"database_name": schema.StringAttribute{
				Description: "Name of the database connection an alert runs its query on. Alerts only.",
				Optional:    true,
			},
			"sql": schema.StringAttribute{
				Description: "SQL query of an alert, which must return a single value. Alerts only.",
				Optional:    true,
			},
			"validator_type": schema.StringAttribute{
				Description: "Condition of an alert: `not null` triggers when the query returns a value, `operator` compares it with `validator_threshold`. Alerts only.",
				Optional:    true,
			},
			"validator_op": schema.StringAttribute{
				Description: "Comparison of the `operator` condition: `<`, `<=`, `>`, `>=`, `==` or `!=`.",
				Optional:    true,
			},
			"validator_threshold": schema.Float64Attribute{
				Description: "Value the result of the query is compared with by the `operator` condition.",
				Optional:    true,
			},
		},
	}
}

// ValidateConfig checks the rules that tie the attributes together: one content, and the
// fields of an alert set for an alert only.
func (r *reportResource) ValidateConfig(ctx context.Context, req resource.ValidateConfigRequest, resp *resource.ValidateConfigResponse) {
	var config reportResourceModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &config)...)
	if resp.Diagnostics.HasError() {
		return
	}

	if !config.ChartID.IsUnknown() && !config.DashboardID.IsUnknown() && config.ChartID.IsNull() == config.DashboardID.IsNull() {
		resp.Diagnostics.AddAttributeError(path.Root("chart_id"), "Invalid Report Content", "Set either chart_id or dashboard_id.")
	}
	if !config.Recipients.IsUnknown() && len(config.Recipients.Elements()) == 0 {
		resp.Diagnostics.AddAttributeError(path.Root("recipients"), "Missing Recipients", "Set at least one recipient.")
	}

	if config.Type.IsUnknown() {
		return
	}
	alertFields := map[string]attr.Value{
		"database_name":       config.DatabaseName,
		"sql":                 config.SQL,
		"validator_type":      config.ValidatorType,
		"validator_op":        config.ValidatorOp,
		"validator_threshold": config.ValidatorThreshold,
	}
	switch config.Type.ValueString() {
	case "Report":
		for name, value := range alertFields {
			if !value.IsNull() {
				resp.Diagnostics.AddAttributeError(path.Root(name), "Invalid Report Attribute", name+" only applies to an alert.")
			}
		}
	case "Alert":
		for _, name := range []string{"database_name", "sql", "validator_type"} {
			if alertFields[name].IsNull() {
				resp.Diagnostics.AddAttributeError(path.Root(name), "Missing Alert Attribute", name+" is required for an alert.")
			}
		}
		if config.ValidatorType.IsUnknown() {
			return
		}
		operator := config.ValidatorType.ValueString() == "operator"
		for _, name := range []string{"validator_op", "validator_threshold"} {
			if operator && alertFields[name].IsNull() {
				resp.Diagnostics.AddAttributeError(path.Root(name), "Missing Alert Attribute", name+" is required for the operator condition.")
			}
			if !operator && !alertFields[name].IsNull() {
				resp.Diagnostics.AddAttributeError(path.Root(name), "Invalid Alert Attribute", name+" only applies to the operator condition.")
			}
		}
	default:
		resp.Diagnostics.AddAttributeError(path.Root("type"), "Invalid Report Type", "type must be Report or Alert.")
	}
}

// Create creates the report and sets the initial Terraform state.
func (r *reportResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan reportResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	report, diags := r.reportFromModel(ctx, plan, reportResourceModel{})
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	id, err := r.client.CreateReport(report)
	if err != nil {
		resp.Diagnostics.AddError("Unable to Create Superset Report", err.Error())
		return
	}

	r.readInto(ctx, id, &resp.State, &resp.Diagnostics, "Unable to Read Superset Report After Creation")
}

// Read refreshes the Terraform state with the report as it is in Superset.
func (r *reportResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state reportResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	report, err := r.client.GetReport(state.ID.ValueInt64())
	if errors.Is(err, client.ErrNotFound) {
		tflog.Warn(ctx, "report no longer exists in Superset, removing it from the state", map[string]any{"id": state.ID.ValueInt64()})
		resp.State.RemoveResource(ctx)
		return
	}
	if err != nil {
		resp.Diagnostics.AddError("Unable to Read Superset Report", err.Error())
		return
	}

	model, diags := reportModel(ctx, report)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &model)...)
}

// Update replaces the settings of the report.
func (r *reportResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state reportResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	report, diags := r.reportFromModel(ctx, plan, state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	if err := r.client.UpdateReport(state.ID.ValueInt64(), report); err != nil {
		resp.Diagnostics.AddError("Unable to Update Superset Report", err.Error())
		return
	}

	r.readInto(ctx, state.ID.ValueInt64(), &resp.State, &resp.Diagnostics, "Unable to Read Superset Report After Update")
}

// Delete deletes the report; a report that is already gone is not an error.
func (r *reportResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state reportResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	err := r.client.DeleteReport(state.ID.ValueInt64())
	if err != nil && !errors.Is(err, client.ErrNotFound) {
		resp.Diagnostics.AddError("Unable to Delete Superset Report", err.Error())
		return
	}
}

// ImportState imports a report by its numeric id.
func (r *reportResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	id, err := strconv.ParseInt(req.ID, 10, 64)
	if err != nil {
		resp.Diagnostics.AddError("Invalid Import ID", fmt.Sprintf("The import ID %q is not a numeric report id.", req.ID))
		return
	}
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("id"), id)...)
}

// Configure adds the provider configured client to the resource.
func (r *reportResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

// readInto fetches the report and stores it in the state after a create or an update, so the
// state carries the values Superset computed, such as the owners or the defaults.
func (r *reportResource) readInto(ctx context.Context, id int64, state stateSetter, diags *diag.Diagnostics, summary string) {
	report, err := r.client.GetReport(id)
	if err != nil {
		diags.AddError(summary, err.Error())
		return
	}

	model, d := reportModel(ctx, report)
	diags.Append(d...)
	if diags.HasError() {
		return
	}
	diags.Append(state.Set(ctx, &model)...)
}

// reportFromModel builds the report of the plan. The email subject is only sent when the plan
// sets it or the state still has one to clear, so that Superset 3.1, which does not know the
// field, accepts the request. The database of an alert is looked up by its name, and the state
// tells whether an alert is becoming a report.
func (r *reportResource) reportFromModel(ctx context.Context, plan, state reportResourceModel) (client.Report, diag.Diagnostics) {
	var diags diag.Diagnostics
	report := client.Report{
		Name:               plan.Name.ValueString(),
		Type:               plan.Type.ValueString(),
		Description:        plan.Description.ValueString(),
		Crontab:            plan.Crontab.ValueString(),
		Timezone:           plan.Timezone.ValueString(),
		ChartID:            plan.ChartID.ValueInt64Pointer(),
		DashboardID:        plan.DashboardID.ValueInt64Pointer(),
		ReportFormat:       plan.ReportFormat.ValueString(),
		Active:             plan.Active.ValueBool(),
		ContextMarkdown:    plan.ContextMarkdown.ValueString(),
		CustomWidth:        plan.CustomWidth.ValueInt64Pointer(),
		ForceScreenshot:    plan.ForceScreenshot.ValueBool(),
		WorkingTimeout:     plan.WorkingTimeout.ValueInt64(),
		GracePeriod:        plan.GracePeriod.ValueInt64(),
		LogRetention:       plan.LogRetention.ValueInt64(),
		Extra:              plan.Extra.ValueString(),
		SQL:                plan.SQL.ValueString(),
		ValidatorType:      plan.ValidatorType.ValueString(),
		ValidatorOp:        plan.ValidatorOp.ValueString(),
		ValidatorThreshold: plan.ValidatorThreshold.ValueFloat64Pointer(),
		WasAlert:           state.Type.ValueString() == "Alert",
	}
	if !plan.EmailSubject.IsNull() || !state.EmailSubject.IsNull() {
		subject := plan.EmailSubject.ValueString()
		report.EmailSubject = &subject
	}
	if !plan.Owners.IsNull() && !plan.Owners.IsUnknown() {
		diags.Append(plan.Owners.ElementsAs(ctx, &report.Owners, false)...)
	}
	if !plan.Recipients.IsNull() && !plan.Recipients.IsUnknown() {
		var recipients []reportRecipientModel
		diags.Append(plan.Recipients.ElementsAs(ctx, &recipients, false)...)
		for _, recipient := range recipients {
			report.Recipients = append(report.Recipients, client.ReportRecipient{
				Type:   recipient.Type.ValueString(),
				Target: recipient.Target.ValueString(),
				CC:     recipient.CC.ValueString(),
				BCC:    recipient.BCC.ValueString(),
			})
		}
	}
	if report.Type == "Alert" {
		databaseID, err := r.client.GetDatabaseIDByName(plan.DatabaseName.ValueString())
		if err != nil {
			diags.AddAttributeError(path.Root("database_name"), "Unable to Find the Database of the Alert", err.Error())
			return client.Report{}, diags
		}
		report.DatabaseID = databaseID
	}
	return report, diags
}

// reportModel maps a report to the state. The fields of an alert are null for a report, even
// when Superset still holds them from the time the report was an alert.
func reportModel(ctx context.Context, report *client.Report) (reportResourceModel, diag.Diagnostics) {
	owners, diags := types.SetValueFrom(ctx, types.Int64Type, report.Owners)
	recipients := make([]reportRecipientModel, 0, len(report.Recipients))
	for _, recipient := range report.Recipients {
		recipients = append(recipients, reportRecipientModel{
			Type:   types.StringValue(recipient.Type),
			Target: types.StringValue(recipient.Target),
			CC:     optionalString(recipient.CC),
			BCC:    optionalString(recipient.BCC),
		})
	}
	recipientsValue, d := types.SetValueFrom(ctx, types.ObjectType{AttrTypes: reportRecipientAttributeTypes}, recipients)
	diags.Append(d...)

	model := reportResourceModel{
		ID:                 types.Int64Value(report.ID),
		Name:               types.StringValue(report.Name),
		Type:               types.StringValue(report.Type),
		Description:        optionalString(report.Description),
		Crontab:            types.StringValue(report.Crontab),
		Timezone:           optionalString(report.Timezone),
		ChartID:            types.Int64PointerValue(report.ChartID),
		DashboardID:        types.Int64PointerValue(report.DashboardID),
		ReportFormat:       optionalString(report.ReportFormat),
		Active:             types.BoolValue(report.Active),
		ContextMarkdown:    optionalString(report.ContextMarkdown),
		EmailSubject:       types.StringNull(),
		CustomWidth:        types.Int64PointerValue(report.CustomWidth),
		ForceScreenshot:    types.BoolValue(report.ForceScreenshot),
		WorkingTimeout:     types.Int64Value(report.WorkingTimeout),
		GracePeriod:        types.Int64Value(report.GracePeriod),
		LogRetention:       types.Int64Value(report.LogRetention),
		Owners:             owners,
		Extra:              jsonDocument.value(report.Extra),
		Recipients:         recipientsValue,
		DatabaseName:       types.StringNull(),
		SQL:                types.StringNull(),
		ValidatorType:      types.StringNull(),
		ValidatorOp:        types.StringNull(),
		ValidatorThreshold: types.Float64Null(),
	}
	if report.EmailSubject != nil {
		model.EmailSubject = optionalString(*report.EmailSubject)
	}
	if report.Type == "Alert" {
		model.DatabaseName = optionalString(report.DatabaseName)
		model.SQL = optionalString(report.SQL)
		model.ValidatorType = optionalString(report.ValidatorType)
		if report.ValidatorType == "operator" {
			model.ValidatorOp = optionalString(report.ValidatorOp)
			model.ValidatorThreshold = types.Float64PointerValue(report.ValidatorThreshold)
		}
	}
	return model, diags
}
