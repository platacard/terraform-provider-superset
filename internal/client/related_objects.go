package client

import (
	"fmt"
	"net/http"
)

// RelatedObjects are the charts and dashboards that use a dataset or a database connection,
// as the related_objects endpoints describe them.
type RelatedObjects struct {
	Charts     []RelatedChart
	Dashboards []RelatedDashboard
}

// RelatedChart is a chart that uses the object.
type RelatedChart struct {
	ID      int64  `json:"id"`
	Name    string `json:"slice_name"`
	VizType string `json:"viz_type"`
}

// RelatedDashboard is a dashboard with a chart that uses the object.
type RelatedDashboard struct {
	ID    int64  `json:"id"`
	Title string `json:"title"`
	Slug  string `json:"slug"`
}

// relatedObjectsResult is the body of a related_objects endpoint. The database endpoint also
// lists the SQL Lab tabs that use the connection, which are not kept.
type relatedObjectsResult struct {
	Charts struct {
		Result []RelatedChart `json:"result"`
	} `json:"charts"`
	Dashboards struct {
		Result []RelatedDashboard `json:"result"`
	} `json:"dashboards"`
}

// GetDatasetRelatedObjects returns the charts and dashboards that use the dataset, or
// ErrNotFound when the dataset does not exist.
func (c *Client) GetDatasetRelatedObjects(id int64) (*RelatedObjects, error) {
	return c.relatedObjects(datasetPath(id)+"/related_objects", fmt.Sprintf("fetch the related objects of dataset %d", id))
}

// GetDatabaseRelatedObjects returns the charts and dashboards that use the database
// connection, or ErrNotFound when the connection does not exist.
func (c *Client) GetDatabaseRelatedObjects(id int64) (*RelatedObjects, error) {
	return c.relatedObjects(databasePath(id)+"/related_objects", fmt.Sprintf("fetch the related objects of database %d", id))
}

func (c *Client) relatedObjects(path, action string) (*RelatedObjects, error) {
	resp, err := c.DoRequest(http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}

	var body relatedObjectsResult
	if err := decodeResponse(resp, &body, action); err != nil {
		return nil, err
	}
	return &RelatedObjects{Charts: body.Charts.Result, Dashboards: body.Dashboards.Result}, nil
}
