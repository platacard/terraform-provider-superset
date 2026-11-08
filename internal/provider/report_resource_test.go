package provider

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"sync"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
	"github.com/jarcoal/httpmock"
)

// fakeReport serves the report endpoints of the fake Superset host for one report with id 7.
// Like Superset, an update merges the fields it carries into the report, the recipients are
// replaced as a whole, and the settings that are not sent get the defaults of Superset. With
// disabled set, every endpoint answers the 404 of the ALERT_REPORTS feature flag being off.
type fakeReport struct {
	mu       sync.Mutex
	report   map[string]any
	deleted  bool
	disabled bool
	// legacy refuses a null database like Superset 3.1, and keeps the alert fields of a report.
	legacy bool
	// last is the payload of the last create or update request.
	last    map[string]any
	updates int
}

func (f *fakeReport) serve() {
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/csrf_token/",
		httpmock.NewStringResponder(200, `{"result": "fake-csrf-token"}`))
	httpmock.RegisterResponder("GET", listEndpoint("/api/v1/database/"),
		httpmock.NewStringResponder(200, fakeDatabases))

	notFound := func() *http.Response {
		return httpmock.NewStringResponse(404, `{"message": "Not found"}`)
	}
	decode := func(req *http.Request) map[string]any {
		var payload map[string]any
		_ = json.NewDecoder(req.Body).Decode(&payload)
		return payload
	}
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/report/",
		func(req *http.Request) (*http.Response, error) {
			payload := decode(req)
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.disabled {
				return notFound(), nil
			}
			if _, ok := payload["recipients"]; !ok {
				return httpmock.NewStringResponse(400, `{"message": {"recipients": ["Missing data for required field."]}}`), nil
			}
			f.report, f.last, f.deleted = payload, payload, false
			return httpmock.NewStringResponse(201, `{"id": 7}`), nil
		})
	httpmock.RegisterResponder("PUT", "http://superset-host/api/v1/report/7",
		func(req *http.Request) (*http.Response, error) {
			payload := decode(req)
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.disabled || f.deleted {
				return notFound(), nil
			}
			f.last = payload
			f.updates++
			if database, present := payload["database"]; present && database == nil {
				if f.legacy {
					return httpmock.NewStringResponse(400, `{"message": {"database": ["Field may not be null."]}}`), nil
				}
			} else if payload["type"] == "Report" && !f.legacy && f.report["database"] != nil {
				return httpmock.NewStringResponse(422, `{"message": {"database": "Database reference is not allowed on a report"}}`), nil
			}
			for key, value := range payload {
				f.report[key] = value
			}
			return httpmock.NewStringResponse(200, `{"id": 7}`), nil
		})
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/report/7",
		func(req *http.Request) (*http.Response, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.disabled || f.deleted || f.report == nil {
				return notFound(), nil
			}
			result := map[string]any{
				"id":               7,
				"name":             f.report["name"],
				"type":             f.report["type"],
				"description":      f.report["description"],
				"crontab":          f.report["crontab"],
				"timezone":         "UTC",
				"chart":            nil,
				"dashboard":        nil,
				"database":         nil,
				"report_format":    "PNG",
				"active":           f.report["active"],
				"context_markdown": f.report["context_markdown"],
				"email_subject":    f.report["email_subject"],
				"custom_width":     f.report["custom_width"],
				"force_screenshot": f.report["force_screenshot"],
				"working_timeout":  3600,
				"grace_period":     14400,
				"log_retention":    90,
				"owners":           []map[string]any{{"id": 1, "first_name": "Admin"}},
				"extra":            f.report["extra"],
				"sql":              f.report["sql"],
				"validator_type":   f.report["validator_type"],
				// Superset returns the validator settings as a JSON string.
				"validator_config_json": "",
			}
			for _, key := range []string{"timezone", "report_format", "working_timeout", "grace_period", "log_retention"} {
				if v, ok := f.report[key]; ok {
					result[key] = v
				}
			}
			if configured, ok := f.report["owners"].([]any); ok {
				var owners []map[string]any
				for _, id := range configured {
					owners = append(owners, map[string]any{"id": id})
				}
				result["owners"] = owners
			}
			if id, ok := f.report["chart"].(float64); ok {
				result["chart"] = map[string]any{"id": id, "slice_name": "Sales"}
			}
			if id, ok := f.report["dashboard"].(float64); ok {
				result["dashboard"] = map[string]any{"id": id, "dashboard_title": "Sales"}
			}
			if id, ok := f.report["database"].(float64); ok {
				result["database"] = map[string]any{"id": id, "database_name": map[float64]string{1: "PostgreSQL Database", 2: "MySQL Database"}[id]}
			}
			if config, ok := f.report["validator_config_json"]; ok {
				text, _ := json.Marshal(config)
				result["validator_config_json"] = string(text)
			}
			var recipients []map[string]any
			if entries, ok := f.report["recipients"].([]any); ok {
				for i, entry := range entries {
					fields, _ := entry.(map[string]any)
					config, _ := json.Marshal(fields["recipient_config_json"])
					recipients = append(recipients, map[string]any{"id": i + 1, "type": fields["type"], "recipient_config_json": string(config)})
				}
			}
			result["recipients"] = recipients
			return httpmock.NewJsonResponse(200, map[string]any{"id": 7, "result": result})
		})
	httpmock.RegisterResponder("DELETE", "http://superset-host/api/v1/report/7",
		func(req *http.Request) (*http.Response, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.disabled || f.deleted {
				return notFound(), nil
			}
			f.deleted = true
			return httpmock.NewStringResponse(200, `{"message": "OK"}`), nil
		})
}

// sent returns a value of the last create or update request.
func (f *fakeReport) sent(key string) any {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.last[key]
}

const chartReport = providerConfig + `
resource "superset_report" "test" {
  name     = "Weekly sales"
  type     = "Report"
  crontab  = "0 9 * * 1"
  chart_id = 42
  recipients = [
    { type = "Email", target = "a@example.com, b@example.com", cc = "c@example.com" },
    { type = "Slack", target = "#sales" },
  ]
}
`

const dashboardReport = providerConfig + `
resource "superset_report" "test" {
  name             = "Weekly sales"
  type             = "Report"
  description      = "Sales of the week"
  crontab          = "30 8 * * *"
  timezone         = "Europe/Berlin"
  dashboard_id     = 5
  report_format    = "CSV"
  active           = false
  context_markdown = "Have a look"
  email_subject    = "Sales"
  custom_width     = 1200
  force_screenshot = true
  working_timeout  = 1800
  grace_period     = 7200
  log_retention    = 30
  owners           = [1, 2]
  extra            = jsonencode({ dashboard = { activeTabs = ["TAB-1"] } })
  recipients = [
    { type = "Email", target = "c@example.com" },
  ]
}
`

const minimalReport = providerConfig + `
resource "superset_report" "test" {
  name         = "Weekly sales"
  type         = "Report"
  crontab      = "30 8 * * *"
  dashboard_id = 5
  recipients = [
    { type = "Email", target = "c@example.com" },
  ]
}
`

func TestAccReportResource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeReport{}
	fake.serve()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: chartReport,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_report.test", "id", "7"),
					resource.TestCheckResourceAttr("superset_report.test", "name", "Weekly sales"),
					resource.TestCheckResourceAttr("superset_report.test", "chart_id", "42"),
					resource.TestCheckNoResourceAttr("superset_report.test", "dashboard_id"),
					resource.TestCheckResourceAttr("superset_report.test", "timezone", "UTC"),
					resource.TestCheckResourceAttr("superset_report.test", "report_format", "PNG"),
					resource.TestCheckResourceAttr("superset_report.test", "active", "true"),
					resource.TestCheckResourceAttr("superset_report.test", "force_screenshot", "false"),
					resource.TestCheckResourceAttr("superset_report.test", "working_timeout", "3600"),
					resource.TestCheckResourceAttr("superset_report.test", "grace_period", "14400"),
					resource.TestCheckResourceAttr("superset_report.test", "log_retention", "90"),
					resource.TestCheckResourceAttr("superset_report.test", "owners.#", "1"),
					resource.TestCheckNoResourceAttr("superset_report.test", "extra"),
					resource.TestCheckNoResourceAttr("superset_report.test", "sql"),
					resource.TestCheckResourceAttr("superset_report.test", "recipients.#", "2"),
					resource.TestCheckTypeSetElemNestedAttrs("superset_report.test", "recipients.*", map[string]string{"type": "Email", "target": "a@example.com, b@example.com", "cc": "c@example.com"}),
					resource.TestCheckTypeSetElemNestedAttrs("superset_report.test", "recipients.*", map[string]string{"type": "Slack", "target": "#sales"}),
					func(*terraform.State) error {
						if fake.sent("creation_method") != "alerts_reports" {
							return errors.New("the creation method of the Alerts & Reports page must be sent")
						}
						if _, present := fake.last["email_subject"]; present {
							return errors.New("an unset email subject must not be sent")
						}
						if _, present := fake.last["sql"]; present {
							return errors.New("the alert fields must not be sent for a report")
						}
						recipients, _ := fake.sent("recipients").([]any)
						if len(recipients) != 2 {
							return fmt.Errorf("unexpected recipients sent: %v", recipients)
						}
						for _, entry := range recipients {
							fields, _ := entry.(map[string]any)
							config, _ := fields["recipient_config_json"].(map[string]any)
							if fields["type"] == "Email" && (config["target"] != "a@example.com, b@example.com" || config["ccTarget"] != "c@example.com") {
								return fmt.Errorf("unexpected email recipient sent: %v", fields)
							}
							if fields["type"] == "Slack" && (config["target"] != "#sales" || len(config) != 1) {
								return fmt.Errorf("unexpected slack recipient sent: %v", fields)
							}
						}
						return nil
					},
				),
			},
			{
				ResourceName:      "superset_report.test",
				ImportState:       true,
				ImportStateVerify: true,
			},
			{
				Config: dashboardReport,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckNoResourceAttr("superset_report.test", "chart_id"),
					resource.TestCheckResourceAttr("superset_report.test", "dashboard_id", "5"),
					resource.TestCheckResourceAttr("superset_report.test", "description", "Sales of the week"),
					resource.TestCheckResourceAttr("superset_report.test", "timezone", "Europe/Berlin"),
					resource.TestCheckResourceAttr("superset_report.test", "report_format", "CSV"),
					resource.TestCheckResourceAttr("superset_report.test", "active", "false"),
					resource.TestCheckResourceAttr("superset_report.test", "context_markdown", "Have a look"),
					resource.TestCheckResourceAttr("superset_report.test", "email_subject", "Sales"),
					resource.TestCheckResourceAttr("superset_report.test", "custom_width", "1200"),
					resource.TestCheckResourceAttr("superset_report.test", "force_screenshot", "true"),
					resource.TestCheckResourceAttr("superset_report.test", "working_timeout", "1800"),
					resource.TestCheckResourceAttr("superset_report.test", "grace_period", "7200"),
					resource.TestCheckResourceAttr("superset_report.test", "log_retention", "30"),
					resource.TestCheckResourceAttr("superset_report.test", "owners.#", "2"),
					resource.TestCheckResourceAttr("superset_report.test", "extra", `{"dashboard":{"activeTabs":["TAB-1"]}}`),
					resource.TestCheckResourceAttr("superset_report.test", "recipients.#", "1"),
					func(*terraform.State) error {
						if fake.sent("chart") != nil || fake.sent("dashboard") != float64(5) {
							return errors.New("switching the content must send chart as null and the dashboard id")
						}
						if extra, _ := fake.sent("extra").(map[string]any); extra["dashboard"] == nil {
							return fmt.Errorf("unexpected extra sent: %v", fake.sent("extra"))
						}
						return nil
					},
				),
			},
			{
				// Removing the optional attributes clears them; the computed ones keep their value.
				Config: minimalReport,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckNoResourceAttr("superset_report.test", "description"),
					resource.TestCheckNoResourceAttr("superset_report.test", "email_subject"),
					resource.TestCheckNoResourceAttr("superset_report.test", "custom_width"),
					resource.TestCheckNoResourceAttr("superset_report.test", "extra"),
					resource.TestCheckResourceAttr("superset_report.test", "recipients.#", "1"),
					resource.TestCheckResourceAttr("superset_report.test", "active", "true"),
					resource.TestCheckResourceAttr("superset_report.test", "timezone", "Europe/Berlin"),
					resource.TestCheckResourceAttr("superset_report.test", "working_timeout", "1800"),
					func(*terraform.State) error {
						if subject, present := fake.last["email_subject"]; !present || subject != nil {
							return errors.New("an email subject that was set must be cleared with null")
						}
						if fake.sent("description") != nil || fake.sent("custom_width") != nil {
							return errors.New("cleared attributes must be sent as null")
						}
						if extra, _ := fake.sent("extra").(map[string]any); len(extra) != 0 {
							return fmt.Errorf("a cleared extra must be sent as an empty object, got %v", fake.sent("extra"))
						}
						return nil
					},
				),
			},
			{
				// A report deleted outside Terraform is recreated.
				PreConfig: func() {
					fake.mu.Lock()
					fake.deleted = true
					fake.mu.Unlock()
				},
				Config: minimalReport,
				Check:  resource.TestCheckResourceAttr("superset_report.test", "id", "7"),
			},
		},
	})
}

const operatorAlert = providerConfig + `
resource "superset_report" "test" {
  name                = "Low sales"
  type                = "Alert"
  crontab             = "0 * * * *"
  chart_id            = 42
  database_name       = "PostgreSQL Database"
  sql                 = "SELECT sum(amount) FROM sales"
  validator_type      = "operator"
  validator_op        = "<"
  validator_threshold = 1000.5
  recipients = [
    { type = "Email", target = "ops@example.com" },
  ]
}
`

const notNullAlert = providerConfig + `
resource "superset_report" "test" {
  name           = "Low sales"
  type           = "Alert"
  crontab        = "0 * * * *"
  chart_id       = 42
  database_name  = "MySQL Database"
  sql            = "SELECT 1"
  validator_type = "not null"
  recipients = [
    { type = "Slack", target = "#ops" },
  ]
}
`

func TestAccReportResourceAlert(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeReport{}
	fake.serve()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: operatorAlert,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_report.test", "type", "Alert"),
					resource.TestCheckResourceAttr("superset_report.test", "database_name", "PostgreSQL Database"),
					resource.TestCheckResourceAttr("superset_report.test", "sql", "SELECT sum(amount) FROM sales"),
					resource.TestCheckResourceAttr("superset_report.test", "validator_type", "operator"),
					resource.TestCheckResourceAttr("superset_report.test", "validator_op", "<"),
					resource.TestCheckResourceAttr("superset_report.test", "validator_threshold", "1000.5"),
					func(*terraform.State) error {
						if fake.sent("database") != float64(1) {
							return errors.New("the database must be looked up by its name")
						}
						config, _ := fake.sent("validator_config_json").(map[string]any)
						if config["op"] != "<" || config["threshold"] != 1000.5 {
							return fmt.Errorf("unexpected validator settings sent: %v", config)
						}
						return nil
					},
				),
			},
			{
				ResourceName:      "superset_report.test",
				ImportState:       true,
				ImportStateVerify: true,
			},
			{
				Config: notNullAlert,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_report.test", "database_name", "MySQL Database"),
					resource.TestCheckResourceAttr("superset_report.test", "validator_type", "not null"),
					resource.TestCheckNoResourceAttr("superset_report.test", "validator_op"),
					resource.TestCheckNoResourceAttr("superset_report.test", "validator_threshold"),
					func(*terraform.State) error {
						if config, _ := fake.sent("validator_config_json").(map[string]any); len(config) != 0 {
							return fmt.Errorf("the not null condition must be sent with empty settings, got %v", config)
						}
						return nil
					},
				),
			},
			{
				// Turning the alert into a report clears the alert fields, which the state leaves null.
				Config: chartReport,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_report.test", "type", "Report"),
					resource.TestCheckNoResourceAttr("superset_report.test", "database_name"),
					resource.TestCheckNoResourceAttr("superset_report.test", "sql"),
					resource.TestCheckNoResourceAttr("superset_report.test", "validator_type"),
					func(*terraform.State) error {
						for _, key := range []string{"database", "sql", "validator_type"} {
							if value, present := fake.last[key]; !present || value != nil {
								return fmt.Errorf("%s must be cleared with null, got %v", key, fake.last)
							}
						}
						return nil
					},
				),
			},
			{
				// A report that was never an alert does not carry the alert fields.
				Config: dashboardReport,
				Check: func(*terraform.State) error {
					if _, present := fake.last["database"]; present {
						return errors.New("the alert fields must not be sent for a report")
					}
					return nil
				},
			},
		},
	})
}

func TestAccReportResourceAlertToReportOnLegacySuperset(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeReport{legacy: true}
	fake.serve()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: notNullAlert,
			},
			{
				// Superset 3.1 refuses the cleared alert fields, so they are left out on a second try.
				Config: chartReport,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_report.test", "type", "Report"),
					resource.TestCheckNoResourceAttr("superset_report.test", "database_name"),
					func(*terraform.State) error {
						fake.mu.Lock()
						defer fake.mu.Unlock()
						if _, present := fake.last["database"]; present {
							return errors.New("the alert fields must be left out when the server refuses null")
						}
						if fake.updates != 2 {
							return fmt.Errorf("the update must be retried once, got %d updates", fake.updates)
						}
						return nil
					},
				),
			},
		},
	})
}

func TestAccReportResourceValidation(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeReport{}
	fake.serve()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
resource "superset_report" "test" {
  name         = "Weekly sales"
  type         = "Report"
  crontab      = "0 9 * * 1"
  chart_id     = 42
  dashboard_id = 5
  recipients   = [{ type = "Email", target = "a@example.com" }]
}
`,
				ExpectError: regexp.MustCompile(`Set either chart_id or dashboard_id`),
			},
			{
				Config: providerConfig + `
resource "superset_report" "test" {
  name    = "Weekly sales"
  type    = "Report"
  crontab = "0 9 * * 1"
  sql     = "SELECT 1"
  recipients = [{ type = "Email", target = "a@example.com" }]
}
`,
				ExpectError: regexp.MustCompile(`sql only applies to an alert`),
			},
			{
				Config: providerConfig + `
resource "superset_report" "test" {
  name           = "Low sales"
  type           = "Alert"
  crontab        = "0 * * * *"
  chart_id       = 42
  database_name  = "PostgreSQL Database"
  sql            = "SELECT 1"
  validator_type = "operator"
  recipients     = [{ type = "Email", target = "a@example.com" }]
}
`,
				ExpectError: regexp.MustCompile(`validator_op is required for the operator condition`),
			},
			{
				Config: providerConfig + `
resource "superset_report" "test" {
  name     = "Weekly sales"
  type     = "Digest"
  crontab  = "0 9 * * 1"
  chart_id = 42
  recipients = [{ type = "Email", target = "a@example.com" }]
}
`,
				ExpectError: regexp.MustCompile(`type must be Report or Alert`),
			},
			{
				Config: providerConfig + `
resource "superset_report" "test" {
  name       = "Weekly sales"
  type       = "Report"
  crontab    = "0 9 * * 1"
  chart_id   = 42
  recipients = []
}
`,
				ExpectError: regexp.MustCompile(`Set at least one recipient`),
			},
		},
	})
}

func TestAccReportResourceFeatureFlagOff(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeReport{disabled: true}
	fake.serve()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config:      chartReport,
				ExpectError: regexp.MustCompile(`ALERT_REPORTS feature flag`),
			},
		},
	})
}
