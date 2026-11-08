package client

import (
	"fmt"
	"net/http"
)

const cssTemplatesPath = "/api/v1/css_template/"

// CSSTemplate is a reusable stylesheet that dashboards can apply.
type CSSTemplate struct {
	ID           int64  `json:"id"`
	TemplateName string `json:"template_name"`
	CSS          string `json:"css"`
}

// cssTemplatePayload is the body of the create and update requests.
type cssTemplatePayload struct {
	TemplateName string `json:"template_name"`
	CSS          string `json:"css"`
}

func cssTemplatePath(id int64) string {
	return fmt.Sprintf("%s%d", cssTemplatesPath, id)
}

// CreateCSSTemplate creates a CSS template and returns its id.
func (c *Client) CreateCSSTemplate(templateName, css string) (int64, error) {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return 0, err
	}

	payload := cssTemplatePayload{TemplateName: templateName, CSS: css}
	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodPost, cssTemplatesPath, payload, headers, cookies)
	if err != nil {
		return 0, err
	}

	var created struct {
		ID int64 `json:"id"`
	}
	if err := decodeResponse(resp, http.StatusCreated, &created, "create CSS template"); err != nil {
		return 0, err
	}
	return created.ID, nil
}

// GetCSSTemplate returns the CSS template with the given id, or ErrNotFound.
func (c *Client) GetCSSTemplate(id int64) (*CSSTemplate, error) {
	resp, err := c.DoRequest(http.MethodGet, cssTemplatePath(id), nil)
	if err != nil {
		return nil, err
	}

	var body struct {
		Result CSSTemplate `json:"result"`
	}
	if err := decodeResponse(resp, http.StatusOK, &body, "fetch CSS template"); err != nil {
		return nil, err
	}
	body.Result.ID = id
	return &body.Result, nil
}

// UpdateCSSTemplate replaces the name and the CSS of a template.
func (c *Client) UpdateCSSTemplate(id int64, templateName, css string) error {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	payload := cssTemplatePayload{TemplateName: templateName, CSS: css}
	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodPut, cssTemplatePath(id), payload, headers, cookies)
	if err != nil {
		return err
	}
	return decodeResponse(resp, http.StatusOK, nil, "update CSS template")
}

// DeleteCSSTemplate deletes a template; a template that no longer exists is reported as ErrNotFound.
func (c *Client) DeleteCSSTemplate(id int64) error {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodDelete, cssTemplatePath(id), nil, headers, cookies)
	if err != nil {
		return err
	}
	return decodeResponse(resp, http.StatusOK, nil, "delete CSS template")
}

// FindCSSTemplateByName returns the template with exactly the given name, or ErrNotFound.
func (c *Client) FindCSSTemplateByName(templateName string) (*CSSTemplate, error) {
	templates, err := listInto[CSSTemplate](c, cssTemplatesPath, equals("template_name", templateName))
	if err != nil {
		return nil, err
	}
	for _, template := range templates {
		if template.TemplateName == templateName {
			return &template, nil
		}
	}
	return nil, ErrNotFound
}
