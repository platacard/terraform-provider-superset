package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

const reportsPath = "/api/v1/report/"

// alertReportsHint explains the 404 that the report endpoints answer when the feature is off.
const alertReportsHint = "Superset answers 404 to every request of the report API when the ALERT_REPORTS feature flag " +
	"is off: enable it in FEATURE_FLAGS in superset_config.py."

// Report is a scheduled report or alert: a chart or a dashboard sent to recipients on a
// schedule, or, for an alert, only when a SQL query meets a condition.
type Report struct {
	ID              int64
	Name            string
	Type            string
	Description     string
	Crontab         string
	Timezone        string
	ChartID         *int64
	DashboardID     *int64
	ReportFormat    string
	Active          bool
	ContextMarkdown string
	// EmailSubject is only sent when it is not nil; an empty value clears it. Superset 3.1 does
	// not know the field.
	EmailSubject    *string
	CustomWidth     *int64
	ForceScreenshot bool
	WorkingTimeout  int64
	GracePeriod     int64
	LogRetention    int64
	Owners          []int64
	Extra           string
	Recipients      []ReportRecipient
	// The fields of an alert.
	DatabaseID         int64
	DatabaseName       string
	SQL                string
	ValidatorType      string
	ValidatorOp        string
	ValidatorThreshold *float64
	// WasAlert marks the update of an alert that becomes a report: recent versions of Superset
	// then require the fields of the alert to be cleared, Superset 3.1 refuses it.
	WasAlert bool
}

// ReportRecipient is one destination of a report. The copies are only known to recent
// versions of Superset and only sent when set.
type ReportRecipient struct {
	Type   string
	Target string
	CC     string
	BCC    string
}

// reportPayload builds the body of the create and update requests. Superset keeps the fields an
// update does not carry, so the optional ones are either sent as null, which clears them, or
// left out when they do not apply. The creation method is always the one of the Alerts &
// Reports page: without it, Superset refuses a second report on the same chart or dashboard.
func reportPayload(report Report) map[string]any {
	recipients := make([]map[string]any, 0, len(report.Recipients))
	for _, recipient := range report.Recipients {
		config := map[string]any{"target": recipient.Target}
		if recipient.CC != "" {
			config["ccTarget"] = recipient.CC
		}
		if recipient.BCC != "" {
			config["bccTarget"] = recipient.BCC
		}
		recipients = append(recipients, map[string]any{"type": recipient.Type, "recipient_config_json": config})
	}

	extra := json.RawMessage("{}")
	if report.Extra != "" {
		extra = json.RawMessage(report.Extra)
	}

	payload := map[string]any{
		"name":             report.Name,
		"type":             report.Type,
		"creation_method":  "alerts_reports",
		"description":      nullIfEmpty(report.Description),
		"crontab":          report.Crontab,
		"chart":            report.ChartID,
		"dashboard":        report.DashboardID,
		"active":           report.Active,
		"context_markdown": nullIfEmpty(report.ContextMarkdown),
		"custom_width":     report.CustomWidth,
		"force_screenshot": report.ForceScreenshot,
		"extra":            extra,
		"recipients":       recipients,
	}
	if report.Timezone != "" {
		payload["timezone"] = report.Timezone
	}
	if report.ReportFormat != "" {
		payload["report_format"] = report.ReportFormat
	}
	if report.EmailSubject != nil {
		payload["email_subject"] = nullIfEmpty(*report.EmailSubject)
	}
	if report.WorkingTimeout != 0 {
		payload["working_timeout"] = report.WorkingTimeout
	}
	if report.GracePeriod != 0 {
		payload["grace_period"] = report.GracePeriod
	}
	if report.LogRetention != 0 {
		payload["log_retention"] = report.LogRetention
	}
	if report.Owners != nil {
		payload["owners"] = report.Owners
	}
	switch {
	case report.Type == "Alert":
		payload["database"] = report.DatabaseID
		payload["sql"] = report.SQL
		payload["validator_type"] = report.ValidatorType
		config := map[string]any{}
		if report.ValidatorType == "operator" {
			config["op"] = report.ValidatorOp
			config["threshold"] = report.ValidatorThreshold
		}
		payload["validator_config_json"] = config
	case report.WasAlert:
		payload["database"] = nil
		payload["sql"] = nil
		payload["validator_type"] = nil
	}
	return payload
}

// jsonObject is a JSON object that Superset returns either inline or as a JSON-encoded string,
// depending on the field and the version.
type jsonObject map[string]any

func (o *jsonObject) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || string(trimmed) == "null" {
		*o = nil
		return nil
	}
	if trimmed[0] == '"' {
		var text string
		if err := json.Unmarshal(trimmed, &text); err != nil {
			return err
		}
		if text == "" {
			*o = nil
			return nil
		}
		trimmed = []byte(text)
	}
	var object map[string]any
	if err := json.Unmarshal(trimmed, &object); err != nil {
		return err
	}
	*o = object
	return nil
}

// document renders the object as a compact JSON document, empty when the object is empty.
func (o jsonObject) document() (string, error) {
	if len(o) == 0 {
		return "", nil
	}
	data, err := json.Marshal(map[string]any(o))
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (o jsonObject) text(key string) string {
	value, _ := o[key].(string)
	return value
}

// reportResult is the report as the API returns it.
type reportResult struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Description string `json:"description"`
	Crontab     string `json:"crontab"`
	Timezone    string `json:"timezone"`
	Chart       *struct {
		ID int64 `json:"id"`
	} `json:"chart"`
	Dashboard *struct {
		ID int64 `json:"id"`
	} `json:"dashboard"`
	Database *struct {
		ID   int64  `json:"id"`
		Name string `json:"database_name"`
	} `json:"database"`
	ReportFormat    string      `json:"report_format"`
	Active          bool        `json:"active"`
	ContextMarkdown string      `json:"context_markdown"`
	EmailSubject    string      `json:"email_subject"`
	CustomWidth     looseInt    `json:"custom_width"`
	ForceScreenshot bool        `json:"force_screenshot"`
	WorkingTimeout  int64       `json:"working_timeout"`
	GracePeriod     int64       `json:"grace_period"`
	LogRetention    int64       `json:"log_retention"`
	Owners          []namedItem `json:"owners"`
	Extra           jsonObject  `json:"extra"`
	Recipients      []struct {
		Type   string     `json:"type"`
		Config jsonObject `json:"recipient_config_json"`
	} `json:"recipients"`
	SQL             string     `json:"sql"`
	ValidatorType   string     `json:"validator_type"`
	ValidatorConfig jsonObject `json:"validator_config_json"`
}

func reportPath(id int64) string {
	return fmt.Sprintf("%s%d", reportsPath, id)
}

// CreateReport creates a report and returns its id.
func (c *Client) CreateReport(report Report) (int64, error) {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return 0, err
	}

	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodPost, reportsPath, reportPayload(report), headers, cookies)
	if err != nil {
		return 0, err
	}

	var created struct {
		ID int64 `json:"id"`
	}
	if err := decodeResponse(resp, &created, "create report"); err != nil {
		if errors.Is(err, ErrNotFound) {
			return 0, errors.New("failed to create report: " + alertReportsHint)
		}
		return 0, err
	}
	return created.ID, nil
}

// GetReport returns the report with the given id, or ErrNotFound.
func (c *Client) GetReport(id int64) (*Report, error) {
	resp, err := c.DoRequest(http.MethodGet, reportPath(id), nil)
	if err != nil {
		return nil, err
	}

	var body struct {
		Result reportResult `json:"result"`
	}
	if err := decodeResponse(resp, &body, "fetch report"); err != nil {
		return nil, err
	}

	result := body.Result
	extra, err := result.Extra.document()
	if err != nil {
		return nil, fmt.Errorf("report %d extra: %w", id, err)
	}
	report := &Report{
		ID:              id,
		Name:            result.Name,
		Type:            result.Type,
		Description:     result.Description,
		Crontab:         result.Crontab,
		Timezone:        result.Timezone,
		ReportFormat:    result.ReportFormat,
		Active:          result.Active,
		ContextMarkdown: result.ContextMarkdown,
		EmailSubject:    &result.EmailSubject,
		CustomWidth:     result.CustomWidth.value,
		ForceScreenshot: result.ForceScreenshot,
		WorkingTimeout:  result.WorkingTimeout,
		GracePeriod:     result.GracePeriod,
		LogRetention:    result.LogRetention,
		Owners:          ids(result.Owners),
		Extra:           extra,
		Recipients:      make([]ReportRecipient, 0, len(result.Recipients)),
		SQL:             result.SQL,
		ValidatorType:   result.ValidatorType,
		ValidatorOp:     result.ValidatorConfig.text("op"),
	}
	if result.Chart != nil {
		report.ChartID = &result.Chart.ID
	}
	if result.Dashboard != nil {
		report.DashboardID = &result.Dashboard.ID
	}
	if result.Database != nil {
		report.DatabaseID, report.DatabaseName = result.Database.ID, result.Database.Name
		if report.DatabaseName == "" {
			name, err := c.GetDatabaseNameByID(report.DatabaseID)
			if err != nil {
				return nil, err
			}
			report.DatabaseName = name
		}
	}
	if threshold, ok := result.ValidatorConfig["threshold"].(float64); ok {
		report.ValidatorThreshold = &threshold
	}
	for _, recipient := range result.Recipients {
		report.Recipients = append(report.Recipients, ReportRecipient{
			Type:   recipient.Type,
			Target: recipient.Config.text("target"),
			CC:     recipient.Config.text("ccTarget"),
			BCC:    recipient.Config.text("bccTarget"),
		})
	}
	return report, nil
}

// UpdateReport replaces the settings of a report; the recipients are replaced as a whole. When
// an alert becomes a report, its fields are cleared, and left out again when the server
// refuses null for them, as Superset 3.1 does.
func (c *Client) UpdateReport(id int64, report Report) error {
	err := c.putReport(id, report)
	if err != nil && report.WasAlert && strings.Contains(err.Error(), "may not be null") {
		report.WasAlert = false
		err = c.putReport(id, report)
	}
	return err
}

func (c *Client) putReport(id int64, report Report) error {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodPut, reportPath(id), reportPayload(report), headers, cookies)
	if err != nil {
		return err
	}
	return decodeResponse(resp, nil, "update report")
}

// DeleteReport deletes a report; a report that no longer exists is reported as ErrNotFound.
func (c *Client) DeleteReport(id int64) error {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodDelete, reportPath(id), nil, headers, cookies)
	if err != nil {
		return err
	}
	return decodeResponse(resp, nil, "delete report")
}

// ReportSummary is a report or an alert as the list endpoint describes it.
type ReportSummary struct {
	ID          int64  `json:"id"`
	Name        string `json:"name"`
	Type        string `json:"type"`
	Description string `json:"description"`
	Crontab     string `json:"crontab"`
	Timezone    string `json:"timezone"`
	Active      bool   `json:"active"`
	ChartID     *int64 `json:"chart_id"`
	DashboardID *int64 `json:"dashboard_id"`
	LastState   string `json:"last_state"`
}

// FindReports returns the reports and alerts with exactly the given name, of the given type
// (Alert or Report) when one is set. The name is compared again because the server comparison
// can be case-insensitive. The 404 of the report API when the feature is off gets its hint.
func (c *Client) FindReports(name, reportType string) ([]ReportSummary, error) {
	filters := []listFilter{equals("name", name)}
	if reportType != "" {
		filters = append(filters, equals("type", reportType))
	}
	rows, err := listInto[ReportSummary](c, reportsPath, filters...)
	if err != nil {
		if strings.Contains(err.Error(), "status code: 404") {
			return nil, fmt.Errorf("reports named %q: %w\n%s", name, err, alertReportsHint)
		}
		return nil, fmt.Errorf("reports named %q: %w", name, err)
	}
	matches := make([]ReportSummary, 0, len(rows))
	for _, row := range rows {
		if row.Name == name {
			matches = append(matches, row)
		}
	}
	return matches, nil
}
