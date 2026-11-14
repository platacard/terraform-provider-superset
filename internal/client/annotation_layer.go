package client

import (
	"fmt"
	"net/http"
)

const annotationLayersPath = "/api/v1/annotation_layer/"

// AnnotationLayer groups annotations that charts can display as event markers.
type AnnotationLayer struct {
	ID          int64
	Name        string
	Description string
}

// annotationLayerPayload is the body of the create and update requests. The update schema
// rejects a null description, so a cleared description is sent as an empty string.
type annotationLayerPayload struct {
	Name        string `json:"name"`
	Description string `json:"descr"`
}

// annotationLayerResult is the layer as the API returns it.
type annotationLayerResult struct {
	Name        string `json:"name"`
	Description string `json:"descr"`
}

func annotationLayerPath(id int64) string {
	return fmt.Sprintf("%s%d", annotationLayersPath, id)
}

// CreateAnnotationLayer creates an annotation layer and returns its id.
func (c *Client) CreateAnnotationLayer(layer AnnotationLayer) (int64, error) {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return 0, err
	}

	payload := annotationLayerPayload{Name: layer.Name, Description: layer.Description}
	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodPost, annotationLayersPath, payload, headers, cookies)
	if err != nil {
		return 0, err
	}

	var created struct {
		ID int64 `json:"id"`
	}
	if err := decodeResponse(resp, &created, "create annotation layer"); err != nil {
		return 0, err
	}
	return created.ID, nil
}

// GetAnnotationLayer returns the annotation layer with the given id, or ErrNotFound.
func (c *Client) GetAnnotationLayer(id int64) (*AnnotationLayer, error) {
	resp, err := c.DoRequest(http.MethodGet, annotationLayerPath(id), nil)
	if err != nil {
		return nil, err
	}

	var body struct {
		Result annotationLayerResult `json:"result"`
	}
	if err := decodeResponse(resp, &body, "fetch annotation layer"); err != nil {
		return nil, err
	}
	return &AnnotationLayer{ID: id, Name: body.Result.Name, Description: body.Result.Description}, nil
}

// UpdateAnnotationLayer replaces the name and the description of a layer.
func (c *Client) UpdateAnnotationLayer(id int64, layer AnnotationLayer) error {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	payload := annotationLayerPayload{Name: layer.Name, Description: layer.Description}
	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodPut, annotationLayerPath(id), payload, headers, cookies)
	if err != nil {
		return err
	}
	return decodeResponse(resp, nil, "update annotation layer")
}

// DeleteAnnotationLayer deletes a layer; a layer that no longer exists is reported as ErrNotFound.
func (c *Client) DeleteAnnotationLayer(id int64) error {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodDelete, annotationLayerPath(id), nil, headers, cookies)
	if err != nil {
		return err
	}
	return decodeResponse(resp, nil, "delete annotation layer")
}

// FindAnnotationLayers returns the layers with exactly the given name. The name is compared
// again because the server comparison can be case-insensitive.
func (c *Client) FindAnnotationLayers(name string) ([]AnnotationLayer, error) {
	type row struct {
		ID int64 `json:"id"`
		annotationLayerResult
	}
	rows, err := listInto[row](c, annotationLayersPath, equals("name", name))
	if err != nil {
		return nil, fmt.Errorf("annotation layers named %q: %w", name, err)
	}
	matches := make([]AnnotationLayer, 0, len(rows))
	for _, r := range rows {
		if r.Name == name {
			matches = append(matches, AnnotationLayer{ID: r.ID, Name: r.Name, Description: r.Description})
		}
	}
	return matches, nil
}
