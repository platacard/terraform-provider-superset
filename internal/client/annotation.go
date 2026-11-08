package client

import (
	"fmt"
	"net/http"
)

// Annotation is an event marker of an annotation layer: a period, or an instant when the
// start and the end are the same, that charts show on their time axis.
type Annotation struct {
	ID           int64
	LayerID      int64
	ShortDescr   string
	LongDescr    string
	StartDttm    string
	EndDttm      string
	JSONMetadata string
}

// annotationPayload is the body of the create and update requests. The nullable fields are
// sent as null when empty, so an update can clear them.
type annotationPayload struct {
	ShortDescr   string  `json:"short_descr"`
	LongDescr    *string `json:"long_descr"`
	StartDttm    string  `json:"start_dttm"`
	EndDttm      string  `json:"end_dttm"`
	JSONMetadata *string `json:"json_metadata"`
}

func newAnnotationPayload(annotation Annotation) annotationPayload {
	return annotationPayload{
		ShortDescr:   annotation.ShortDescr,
		LongDescr:    nullIfEmpty(annotation.LongDescr),
		StartDttm:    annotation.StartDttm,
		EndDttm:      annotation.EndDttm,
		JSONMetadata: nullIfEmpty(annotation.JSONMetadata),
	}
}

// annotationResult is the annotation as the API returns it.
type annotationResult struct {
	ShortDescr   string `json:"short_descr"`
	LongDescr    string `json:"long_descr"`
	StartDttm    string `json:"start_dttm"`
	EndDttm      string `json:"end_dttm"`
	JSONMetadata string `json:"json_metadata"`
}

func annotationsPath(layerID int64) string {
	return annotationLayerPath(layerID) + "/annotation/"
}

func annotationPath(layerID, id int64) string {
	return fmt.Sprintf("%s%d", annotationsPath(layerID), id)
}

// CreateAnnotation creates an annotation in its layer and returns its id.
func (c *Client) CreateAnnotation(annotation Annotation) (int64, error) {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return 0, err
	}

	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodPost, annotationsPath(annotation.LayerID), newAnnotationPayload(annotation), headers, cookies)
	if err != nil {
		return 0, err
	}

	var created struct {
		ID int64 `json:"id"`
	}
	if err := decodeResponse(resp, &created, "create annotation"); err != nil {
		return 0, err
	}
	return created.ID, nil
}

// GetAnnotation returns the annotation with the given id in the given layer, or ErrNotFound
// when either is gone.
func (c *Client) GetAnnotation(layerID, id int64) (*Annotation, error) {
	resp, err := c.DoRequest(http.MethodGet, annotationPath(layerID, id), nil)
	if err != nil {
		return nil, err
	}

	var body struct {
		Result annotationResult `json:"result"`
	}
	if err := decodeResponse(resp, &body, "fetch annotation"); err != nil {
		return nil, err
	}
	return &Annotation{
		ID:           id,
		LayerID:      layerID,
		ShortDescr:   body.Result.ShortDescr,
		LongDescr:    body.Result.LongDescr,
		StartDttm:    body.Result.StartDttm,
		EndDttm:      body.Result.EndDttm,
		JSONMetadata: body.Result.JSONMetadata,
	}, nil
}

// UpdateAnnotation replaces the attributes of an annotation.
func (c *Client) UpdateAnnotation(id int64, annotation Annotation) error {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodPut, annotationPath(annotation.LayerID, id), newAnnotationPayload(annotation), headers, cookies)
	if err != nil {
		return err
	}
	return decodeResponse(resp, nil, "update annotation")
}

// DeleteAnnotation deletes an annotation; one that no longer exists is reported as ErrNotFound.
func (c *Client) DeleteAnnotation(layerID, id int64) error {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodDelete, annotationPath(layerID, id), nil, headers, cookies)
	if err != nil {
		return err
	}
	return decodeResponse(resp, nil, "delete annotation")
}
