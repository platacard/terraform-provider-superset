package client

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

const tagsPath = "/api/v1/tag/"

// taggingSystemHint explains the 404 that the tag endpoints answer when the feature is off.
const taggingSystemHint = "Superset answers 404 to every request of the tag API when the TAGGING_SYSTEM feature flag " +
	"is off: enable it in FEATURE_FLAGS in superset_config.py."

// customTagType is the type of the tags people create; Superset keeps the owner, type and
// favorite relations as tags of other types.
const customTagType = 1

// Object types as the tagging endpoints number them.
const (
	ChartObject     = 2
	DashboardObject = 3
)

// Tag is a label people attach to charts, dashboards and saved queries.
type Tag struct {
	ID          int64
	Name        string
	Description string
}

// TaggedObject is an object a tag is attached to, as the tag endpoint lists it.
type TaggedObject struct {
	Type string `json:"type"`
	ID   int64  `json:"id"`
}

// tagPayload is the body of the create and update requests. The objects are always sent: an
// update replaces the objects the tag is attached to, and Superset 3.1 fails without the list.
type tagPayload struct {
	Name         string  `json:"name"`
	Description  *string `json:"description"`
	ObjectsToTag [][]any `json:"objects_to_tag"`
}

// tagSummary is a tag as the list endpoint and the single-tag endpoint describe it.
type tagSummary struct {
	ID          int64  `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Type        int    `json:"type"`
}

func tagPath(id int64) string {
	return fmt.Sprintf("%s%d", tagsPath, id)
}

// FindTagByName returns the tag with exactly the given name, or ErrNotFound. The server
// filters on equality; the name is compared again because the comparison can be
// case-insensitive depending on the metadata database.
func (c *Client) FindTagByName(name string) (*Tag, error) {
	rows, err := listInto[tagSummary](c, tagsPath, equals("name", name))
	if err != nil {
		return nil, fmt.Errorf("tag %q: %w", name, err)
	}
	for _, row := range rows {
		if row.Name == name {
			return &Tag{ID: row.ID, Name: row.Name, Description: row.Description}, nil
		}
	}
	return nil, fmt.Errorf("tag %q: %w", name, ErrNotFound)
}

// CreateTag creates a tag and returns its id. The create endpoint answers without the id, so
// the tag is looked up by its name right after; it is an error when a tag with that name
// already exists, since the endpoint would silently keep it.
func (c *Client) CreateTag(tag Tag) (int64, error) {
	if existing, err := c.FindTagByName(tag.Name); err == nil {
		return 0, fmt.Errorf("a tag named %q already exists with id %d: import it instead", tag.Name, existing.ID)
	} else if !errors.Is(err, ErrNotFound) {
		if missingTaggingSystem(err) {
			return 0, errors.New("failed to create tag: " + taggingSystemHint)
		}
		return 0, err
	}

	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return 0, err
	}

	payload := tagPayload{Name: tag.Name, Description: nullIfEmpty(tag.Description), ObjectsToTag: [][]any{}}
	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodPost, tagsPath, payload, headers, cookies)
	if err != nil {
		return 0, err
	}
	if err := decodeResponse(resp, nil, "create tag"); err != nil {
		if errors.Is(err, ErrNotFound) {
			return 0, errors.New("failed to create tag: " + taggingSystemHint)
		}
		return 0, err
	}

	created, err := c.FindTagByName(tag.Name)
	if err != nil {
		return 0, fmt.Errorf("the created tag could not be found: %w", err)
	}
	return created.ID, nil
}

// missingTaggingSystem reports whether a list request failed with the 404 of the tag API.
func missingTaggingSystem(err error) bool {
	return err != nil && strings.Contains(err.Error(), "status code: 404")
}

// GetTag returns the tag with the given id, or ErrNotFound.
func (c *Client) GetTag(id int64) (*Tag, error) {
	resp, err := c.DoRequest(http.MethodGet, tagPath(id), nil)
	if err != nil {
		return nil, err
	}

	var body struct {
		Result tagSummary `json:"result"`
	}
	if err := decodeResponse(resp, &body, "fetch tag"); err != nil {
		return nil, err
	}
	return &Tag{ID: id, Name: body.Result.Name, Description: body.Result.Description}, nil
}

// TagObjects returns the objects the tag with the given name is attached to.
func (c *Client) TagObjects(name string) ([]TaggedObject, error) {
	resp, err := c.DoRequest(http.MethodGet, tagsPath+"get_objects/?tags="+url.QueryEscape(name), nil)
	if err != nil {
		return nil, err
	}

	var body struct {
		Result []TaggedObject `json:"result"`
	}
	if err := decodeResponse(resp, &body, "list the objects of tag "+name); err != nil {
		return nil, err
	}
	return body.Result, nil
}

// UpdateTag replaces the name and the description of a tag. The update endpoint also replaces
// the objects the tag is attached to, so the current ones are sent back with it.
func (c *Client) UpdateTag(id int64, current Tag, tag Tag) error {
	objects, err := c.TagObjects(current.Name)
	if err != nil {
		return err
	}
	payload := tagPayload{Name: tag.Name, Description: nullIfEmpty(tag.Description), ObjectsToTag: make([][]any, 0, len(objects))}
	for _, object := range objects {
		payload.ObjectsToTag = append(payload.ObjectsToTag, []any{object.Type, object.ID})
	}

	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodPut, tagPath(id), payload, headers, cookies)
	if err != nil {
		return err
	}
	return decodeResponse(resp, nil, "update tag")
}

// DeleteTag detaches a tag from its objects and deletes it; a tag that no longer exists is
// reported as ErrNotFound. Superset 3.1 deletes an attached tag in a way that later prevents
// the deletion of its objects, which the detaching avoids. An object may be deleted while the
// tag is being detached from it, so a failed detaching only counts when the object is still
// attached afterwards.
func (c *Client) DeleteTag(id int64, name string) error {
	objects, err := c.TagObjects(name)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	var detachErr error
	for _, object := range objects {
		if err := c.UntagObject(objectTypeNumber(object.Type), object.ID, name); err != nil && !errors.Is(err, ErrNotFound) {
			detachErr = err
		}
	}
	if detachErr != nil {
		remaining, err := c.TagObjects(name)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return err
		}
		if len(remaining) > 0 {
			return fmt.Errorf("detach the tag from %s %d: %w", remaining[0].Type, remaining[0].ID, detachErr)
		}
	}

	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodDelete, tagPath(id), nil, headers, cookies)
	if err != nil {
		return err
	}
	return decodeResponse(resp, nil, "delete tag")
}

// objectTypeNumber maps the type names of the tag endpoints to the numbers of the tagging
// endpoints.
func objectTypeNumber(name string) int {
	switch name {
	case "query":
		return 1
	case "chart":
		return ChartObject
	case "dashboard":
		return DashboardObject
	default:
		return 4
	}
}

// TagObject attaches tags to an object by their names, creating the tags that do not exist.
func (c *Client) TagObject(objectType int, id int64, names []string) error {
	if len(names) == 0 {
		return nil
	}
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	payload := map[string]any{"properties": map[string]any{"tags": names}}
	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodPost, fmt.Sprintf("%s%d/%d/", tagsPath, objectType, id), payload, headers, cookies)
	if err != nil {
		return err
	}
	if err := decodeResponse(resp, nil, "tag object"); err != nil {
		if errors.Is(err, ErrNotFound) {
			return errors.New("failed to tag object: " + taggingSystemHint)
		}
		return err
	}
	return nil
}

// UntagObject detaches a tag from an object.
func (c *Client) UntagObject(objectType int, id int64, name string) error {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodDelete, fmt.Sprintf("%s%d/%d/%s/", tagsPath, objectType, id, url.PathEscape(name)), nil, headers, cookies)
	if err != nil {
		return err
	}
	return decodeResponse(resp, nil, "untag object")
}

// SetObjectTags attaches the desired tags an object does not have yet and detaches the ones it
// has beyond them. Only the difference is sent: Superset 3.1 attaches a tag twice when asked
// twice, and the duplicate relation later breaks the deletion of the object.
func (c *Client) SetObjectTags(objectType int, id int64, current, desired []string) error {
	has := make(map[string]bool, len(current))
	for _, name := range current {
		has[name] = true
	}
	wanted := make(map[string]bool, len(desired))
	var missing []string
	for _, name := range desired {
		wanted[name] = true
		if !has[name] {
			missing = append(missing, name)
		}
	}
	if err := c.TagObject(objectType, id, missing); err != nil {
		return err
	}
	for _, name := range current {
		if !wanted[name] {
			if err := c.UntagObject(objectType, id, name); err != nil && !errors.Is(err, ErrNotFound) {
				return err
			}
		}
	}
	return nil
}

// customTagNames keeps the names of the tags people created out of the tags of an object,
// leaving the owner, type and favorite relations aside.
func customTagNames(tags []tagSummary) []string {
	names := make([]string, 0, len(tags))
	for _, tag := range tags {
		if tag.Type == customTagType {
			names = append(names, tag.Name)
		}
	}
	return names
}
