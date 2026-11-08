package client

import (
	"fmt"
	"net/http"
	"strings"
)

const datasetsPath = "/api/v1/dataset/"

// Dataset is a dataset as the API manages it: a physical table or a virtual dataset defined by
// SQL, with the settings Superset keeps about it.
type Dataset struct {
	ID                   int64
	DatabaseID           int64
	DatabaseName         string
	TableName            string
	Schema               string
	SQL                  string
	Description          string
	Owners               []int64
	CacheTimeout         *int64
	MainDttmCol          string
	TemplateParams       string
	Extra                string
	FetchValuesPredicate string
	FilterSelectEnabled  bool
	NormalizeColumns     bool
	AlwaysFilterMainDttm bool
	DefaultEndpoint      string
	// Metrics are the metrics of the dataset. A nil slice leaves the metrics as they are in
	// Superset; a non-nil slice, even an empty one, replaces them.
	Metrics []DatasetMetric
}

// DatasetMetric is a metric of a dataset, an aggregate expression over its columns.
type DatasetMetric struct {
	ID          int64
	Name        string
	Expression  string
	VerboseName string
	Description string
	D3Format    string
	WarningText string
	MetricType  string
}

// datasetMetricPayload is a metric in an update request. An entry with an id updates the
// metric, one without creates it, and Superset deletes the metrics that are not listed.
// Optional fields are sent as null when empty, so an update can clear them.
type datasetMetricPayload struct {
	ID          *int64  `json:"id,omitempty"`
	Name        string  `json:"metric_name"`
	Expression  string  `json:"expression"`
	VerboseName *string `json:"verbose_name"`
	Description *string `json:"description"`
	D3Format    *string `json:"d3format"`
	WarningText *string `json:"warning_text"`
	MetricType  *string `json:"metric_type"`
}

// datasetCreatePayload is the body of the create request, which only accepts the fields that
// identify the dataset; the other settings are written by an update right after. The template
// parameters are part of it because Superset renders the SQL of a virtual dataset when it
// creates the dataset; Superset 3.1 does not accept them there and reports an unknown field.
type datasetCreatePayload struct {
	Database             int64   `json:"database"`
	TableName            string  `json:"table_name"`
	Schema               *string `json:"schema,omitempty"`
	SQL                  *string `json:"sql,omitempty"`
	Owners               []int64 `json:"owners,omitempty"`
	NormalizeColumns     bool    `json:"normalize_columns"`
	AlwaysFilterMainDttm bool    `json:"always_filter_main_dttm"`
	TemplateParams       *string `json:"template_params,omitempty"`
}

// datasetUpdatePayload is the body of the update request. Nullable fields are sent as null when
// empty, so an update can clear them. The main datetime column and the owners are only sent
// when set, because Superset fills them in on its own. Columns and metrics are left out, which
// keeps them as they are.
type datasetUpdatePayload struct {
	TableName            string                  `json:"table_name"`
	Schema               *string                 `json:"schema"`
	SQL                  *string                 `json:"sql"`
	Description          *string                 `json:"description"`
	Owners               []int64                 `json:"owners,omitempty"`
	CacheTimeout         *int64                  `json:"cache_timeout"`
	MainDttmCol          *string                 `json:"main_dttm_col,omitempty"`
	TemplateParams       *string                 `json:"template_params"`
	Extra                *string                 `json:"extra"`
	FetchValuesPredicate *string                 `json:"fetch_values_predicate"`
	FilterSelectEnabled  bool                    `json:"filter_select_enabled"`
	NormalizeColumns     bool                    `json:"normalize_columns"`
	AlwaysFilterMainDttm bool                    `json:"always_filter_main_dttm"`
	DefaultEndpoint      *string                 `json:"default_endpoint"`
	Metrics              *[]datasetMetricPayload `json:"metrics,omitempty"`
}

func newDatasetUpdatePayload(dataset Dataset) datasetUpdatePayload {
	return datasetUpdatePayload{
		TableName:            dataset.TableName,
		Schema:               nullIfEmpty(dataset.Schema),
		SQL:                  nullIfEmpty(dataset.SQL),
		Description:          nullIfEmpty(dataset.Description),
		Owners:               dataset.Owners,
		CacheTimeout:         dataset.CacheTimeout,
		MainDttmCol:          nullIfEmpty(dataset.MainDttmCol),
		TemplateParams:       nullIfEmpty(dataset.TemplateParams),
		Extra:                nullIfEmpty(dataset.Extra),
		FetchValuesPredicate: nullIfEmpty(dataset.FetchValuesPredicate),
		FilterSelectEnabled:  dataset.FilterSelectEnabled,
		NormalizeColumns:     dataset.NormalizeColumns,
		AlwaysFilterMainDttm: dataset.AlwaysFilterMainDttm,
		DefaultEndpoint:      nullIfEmpty(dataset.DefaultEndpoint),
	}
}

// metricPayloads builds the metrics of an update, giving the metrics that exist in Superset
// their id, so they are updated rather than recreated.
func metricPayloads(metrics []DatasetMetric, current []DatasetMetric) []datasetMetricPayload {
	ids := make(map[string]int64, len(current))
	for _, metric := range current {
		ids[metric.Name] = metric.ID
	}
	payloads := make([]datasetMetricPayload, 0, len(metrics))
	for _, metric := range metrics {
		payload := datasetMetricPayload{
			Name:        metric.Name,
			Expression:  metric.Expression,
			VerboseName: nullIfEmpty(metric.VerboseName),
			Description: nullIfEmpty(metric.Description),
			D3Format:    nullIfEmpty(metric.D3Format),
			WarningText: nullIfEmpty(metric.WarningText),
			MetricType:  nullIfEmpty(metric.MetricType),
		}
		if id, ok := ids[metric.Name]; ok {
			payload.ID = &id
		}
		payloads = append(payloads, payload)
	}
	return payloads
}

// datasetResult is the dataset as the API returns it.
type datasetResult struct {
	Database struct {
		ID   int64  `json:"id"`
		Name string `json:"database_name"`
	} `json:"database"`
	TableName            string      `json:"table_name"`
	Schema               string      `json:"schema"`
	SQL                  string      `json:"sql"`
	Description          string      `json:"description"`
	Owners               []namedItem `json:"owners"`
	CacheTimeout         looseInt    `json:"cache_timeout"`
	MainDttmCol          string      `json:"main_dttm_col"`
	TemplateParams       string      `json:"template_params"`
	Extra                string      `json:"extra"`
	FetchValuesPredicate string      `json:"fetch_values_predicate"`
	FilterSelectEnabled  bool        `json:"filter_select_enabled"`
	NormalizeColumns     bool        `json:"normalize_columns"`
	AlwaysFilterMainDttm bool        `json:"always_filter_main_dttm"`
	DefaultEndpoint      string      `json:"default_endpoint"`
	Metrics              []struct {
		ID          int64  `json:"id"`
		Name        string `json:"metric_name"`
		Expression  string `json:"expression"`
		VerboseName string `json:"verbose_name"`
		Description string `json:"description"`
		D3Format    string `json:"d3format"`
		WarningText string `json:"warning_text"`
		MetricType  string `json:"metric_type"`
	} `json:"metrics"`
}

func datasetPath(id int64) string {
	return fmt.Sprintf("%s%d", datasetsPath, id)
}

// CreateDataset creates a dataset and returns its id. The create endpoint only takes the fields
// that identify the dataset, so the other settings are written by an update right after; when
// that update fails, the id of the created dataset is returned with the error, so the caller can
// still record the dataset. Template parameters are sent with the creation when the server
// accepts them, and left to the update on Superset 3.1, which does not.
func (c *Client) CreateDataset(dataset Dataset) (int64, error) {
	payload := datasetCreatePayload{
		Database:             dataset.DatabaseID,
		TableName:            dataset.TableName,
		Schema:               nullIfEmpty(dataset.Schema),
		SQL:                  nullIfEmpty(dataset.SQL),
		Owners:               dataset.Owners,
		NormalizeColumns:     dataset.NormalizeColumns,
		AlwaysFilterMainDttm: dataset.AlwaysFilterMainDttm,
		TemplateParams:       nullIfEmpty(dataset.TemplateParams),
	}
	id, err := c.postDataset(payload)
	if err != nil && payload.TemplateParams != nil && unknownField(err, "template_params") {
		payload.TemplateParams = nil
		id, err = c.postDataset(payload)
	}
	if err != nil {
		return 0, err
	}
	if err := c.UpdateDataset(id, dataset); err != nil {
		return id, fmt.Errorf("write the settings of the dataset: %w", err)
	}
	return id, nil
}

func (c *Client) postDataset(payload datasetCreatePayload) (int64, error) {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return 0, err
	}

	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodPost, datasetsPath, payload, headers, cookies)
	if err != nil {
		return 0, err
	}

	var created struct {
		ID int64 `json:"id"`
	}
	if err := decodeResponse(resp, &created, "create dataset"); err != nil {
		return 0, err
	}
	return created.ID, nil
}

// unknownField reports whether the error is the rejection of the named field by the request
// schema of the server, as older versions of Superset answer for fields they do not know.
func unknownField(err error, field string) bool {
	message := err.Error()
	return strings.Contains(message, "status code: 400") && strings.Contains(message, field) && strings.Contains(message, "Unknown field")
}

// GetDataset returns the dataset with the given id, or ErrNotFound.
func (c *Client) GetDataset(id int64) (*Dataset, error) {
	resp, err := c.DoRequest(http.MethodGet, datasetPath(id), nil)
	if err != nil {
		return nil, err
	}

	var body struct {
		Result datasetResult `json:"result"`
	}
	if err := decodeResponse(resp, &body, "fetch dataset"); err != nil {
		return nil, err
	}

	result := body.Result
	dataset := &Dataset{
		ID:                   id,
		DatabaseID:           result.Database.ID,
		DatabaseName:         result.Database.Name,
		TableName:            result.TableName,
		Schema:               result.Schema,
		SQL:                  result.SQL,
		Description:          result.Description,
		Owners:               ids(result.Owners),
		CacheTimeout:         result.CacheTimeout.value,
		MainDttmCol:          result.MainDttmCol,
		TemplateParams:       result.TemplateParams,
		Extra:                result.Extra,
		FetchValuesPredicate: result.FetchValuesPredicate,
		FilterSelectEnabled:  result.FilterSelectEnabled,
		NormalizeColumns:     result.NormalizeColumns,
		AlwaysFilterMainDttm: result.AlwaysFilterMainDttm,
		DefaultEndpoint:      result.DefaultEndpoint,
		Metrics:              make([]DatasetMetric, 0, len(result.Metrics)),
	}
	for _, metric := range result.Metrics {
		dataset.Metrics = append(dataset.Metrics, DatasetMetric{
			ID:          metric.ID,
			Name:        metric.Name,
			Expression:  metric.Expression,
			VerboseName: metric.VerboseName,
			Description: metric.Description,
			D3Format:    metric.D3Format,
			WarningText: metric.WarningText,
			MetricType:  metric.MetricType,
		})
	}
	if dataset.DatabaseName == "" && dataset.DatabaseID != 0 {
		name, err := c.GetDatabaseNameByID(dataset.DatabaseID)
		if err != nil {
			return nil, err
		}
		dataset.DatabaseName = name
	}
	return dataset, nil
}

// UpdateDataset replaces the settings of a dataset. The metrics are replaced as well when the
// dataset carries a non-nil list of them: the ones that exist keep their id, new ones are
// created and the ones that are not listed are deleted. The columns are left as they are.
func (c *Client) UpdateDataset(id int64, dataset Dataset) error {
	payload := newDatasetUpdatePayload(dataset)
	if dataset.Metrics != nil {
		current, err := c.GetDataset(id)
		if err != nil {
			return err
		}
		metrics := metricPayloads(dataset.Metrics, current.Metrics)
		payload.Metrics = &metrics
	}

	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodPut, datasetPath(id), payload, headers, cookies)
	if err != nil {
		return err
	}
	return decodeResponse(resp, nil, "update dataset")
}

// DeleteDataset deletes a dataset; a dataset that no longer exists is reported as ErrNotFound.
func (c *Client) DeleteDataset(id int64) error {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodDelete, datasetPath(id), nil, headers, cookies)
	if err != nil {
		return err
	}
	return decodeResponse(resp, nil, "delete dataset")
}
