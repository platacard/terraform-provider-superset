package provider

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
	"github.com/jarcoal/httpmock"
)

// fakeTagEntry is one tag of the fake tagging system, with the objects it is attached to.
type fakeTagEntry struct {
	name        string
	description any
	objects     map[string]bool
}

// fakeTags serves the tag endpoints of the fake Superset host: the tag list with its name
// filter, the single-tag endpoints, the objects of a tag and the tagging of an object. Like
// Superset, the create endpoint answers without the id and the update endpoint detaches the
// tag from its objects; attaching a tag twice counts, as Superset 3.1 does, so the tests can
// prove it never happens. With disabled set, every endpoint answers the 404 of the feature
// flag being off.
type fakeTags struct {
	mu       sync.Mutex
	tags     map[int64]*fakeTagEntry
	nextID   int64
	disabled bool
	attaches int
	detaches int
}

var (
	fakeTagObjectPath = regexp.MustCompile(`^http://superset-host/api/v1/tag/(\d+)/(\d+)/$`)
	fakeTagUntagPath  = regexp.MustCompile(`^http://superset-host/api/v1/tag/(\d+)/(\d+)/([^/]+)/$`)
	fakeTagPath       = regexp.MustCompile(`^http://superset-host/api/v1/tag/(\d+)$`)
)

func newFakeTags() *fakeTags {
	return &fakeTags{tags: map[int64]*fakeTagEntry{}, nextID: 10}
}

// objectKey names an object the way the fake stores it, from the numbers of the tagging endpoints.
func objectKey(objectType string, id string) string {
	return map[string]string{"1": "query", "2": "chart", "3": "dashboard", "4": "dataset"}[objectType] + ":" + id
}

// byName returns the tag with the given name, or nil.
func (f *fakeTags) byName(name string) *fakeTagEntry {
	for _, tag := range f.tags {
		if tag.name == name {
			return tag
		}
	}
	return nil
}

// summaries returns the custom tags of an object as the chart and dashboard endpoints list
// them, after a system tag that the provider must leave aside.
func (f *fakeTags) summaries(objectType int, id int64) []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	result := []map[string]any{{"id": 1, "name": "owner:1", "type": 3}}
	key := objectKey(strconv.Itoa(objectType), strconv.FormatInt(id, 10))
	for tagID, tag := range f.tags {
		if tag.objects[key] {
			result = append(result, map[string]any{"id": tagID, "name": tag.name, "type": 1})
		}
	}
	return result
}

func (f *fakeTags) serve() {
	notFound := func() *http.Response {
		return httpmock.NewStringResponse(404, `{"message": "Not found"}`)
	}
	decode := func(req *http.Request) map[string]any {
		var payload map[string]any
		_ = json.NewDecoder(req.Body).Decode(&payload)
		return payload
	}
	httpmock.RegisterResponder("GET", listEndpoint("/api/v1/tag/"),
		func(req *http.Request) (*http.Response, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.disabled {
				return notFound(), nil
			}
			var rows []map[string]any
			for id, tag := range f.tags {
				if strings.Contains(req.URL.Query().Get("q"), "(col:name,opr:eq,value:'"+tag.name+"')") {
					rows = append(rows, map[string]any{"id": id, "name": tag.name, "description": tag.description, "type": 1})
				}
			}
			return httpmock.NewJsonResponse(200, map[string]any{"count": len(rows), "result": rows})
		})
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/tag/get_objects/",
		func(req *http.Request) (*http.Response, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.disabled {
				return notFound(), nil
			}
			var rows []map[string]any
			if tag := f.byName(req.URL.Query().Get("tags")); tag != nil {
				for key := range tag.objects {
					parts := strings.SplitN(key, ":", 2)
					id, _ := strconv.Atoi(parts[1])
					rows = append(rows, map[string]any{"type": parts[0], "id": id, "name": key})
				}
			}
			return httpmock.NewJsonResponse(200, map[string]any{"result": rows})
		})
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/tag/",
		func(req *http.Request) (*http.Response, error) {
			payload := decode(req)
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.disabled {
				return notFound(), nil
			}
			if _, present := payload["objects_to_tag"]; !present {
				return httpmock.NewStringResponse(500, `{"message": "Fatal error"}`), nil
			}
			name, _ := payload["name"].(string)
			if existing := f.byName(name); existing == nil {
				f.tags[f.nextID] = &fakeTagEntry{name: name, description: payload["description"], objects: map[string]bool{}}
				f.nextID++
			}
			return httpmock.NewStringResponse(201, `{}`), nil
		})
	httpmock.RegisterRegexpResponder("GET", fakeTagPath,
		func(req *http.Request) (*http.Response, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			id, _ := strconv.ParseInt(fakeTagPath.FindStringSubmatch(req.URL.String())[1], 10, 64)
			tag, ok := f.tags[id]
			if f.disabled || !ok {
				return notFound(), nil
			}
			return httpmock.NewJsonResponse(200, map[string]any{"id": id, "result": map[string]any{"id": id, "name": tag.name, "description": tag.description, "type": 1}})
		})
	httpmock.RegisterRegexpResponder("PUT", fakeTagPath,
		func(req *http.Request) (*http.Response, error) {
			payload := decode(req)
			f.mu.Lock()
			defer f.mu.Unlock()
			id, _ := strconv.ParseInt(fakeTagPath.FindStringSubmatch(req.URL.String())[1], 10, 64)
			tag, ok := f.tags[id]
			if f.disabled || !ok {
				return notFound(), nil
			}
			if _, present := payload["objects_to_tag"]; !present {
				return httpmock.NewStringResponse(500, `{"message": "Fatal error"}`), nil
			}
			tag.name, _ = payload["name"].(string)
			tag.description = payload["description"]
			// Like Superset: the update replaces the objects of the tag.
			tag.objects = map[string]bool{}
			if entries, ok := payload["objects_to_tag"].([]any); ok {
				for _, entry := range entries {
					pair, _ := entry.([]any)
					if len(pair) == 2 {
						tag.objects[fmt.Sprintf("%v:%v", pair[0], pair[1])] = true
					}
				}
			}
			return httpmock.NewStringResponse(200, `{"id": `+strconv.FormatInt(id, 10)+`}`), nil
		})
	httpmock.RegisterRegexpResponder("DELETE", fakeTagPath,
		func(req *http.Request) (*http.Response, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			id, _ := strconv.ParseInt(fakeTagPath.FindStringSubmatch(req.URL.String())[1], 10, 64)
			tag, ok := f.tags[id]
			if f.disabled || !ok {
				return notFound(), nil
			}
			if len(tag.objects) > 0 {
				return httpmock.NewStringResponse(422, `{"message": "the tag is still attached, which Superset 3.1 turns into broken relations"}`), nil
			}
			delete(f.tags, id)
			return httpmock.NewStringResponse(200, `{"message": "OK"}`), nil
		})
	httpmock.RegisterRegexpResponder("POST", fakeTagObjectPath,
		func(req *http.Request) (*http.Response, error) {
			payload := decode(req)
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.disabled {
				return notFound(), nil
			}
			match := fakeTagObjectPath.FindStringSubmatch(req.URL.String())
			key := objectKey(match[1], match[2])
			properties, _ := payload["properties"].(map[string]any)
			names, _ := properties["tags"].([]any)
			for _, entry := range names {
				name, _ := entry.(string)
				tag := f.byName(name)
				if tag == nil {
					tag = &fakeTagEntry{name: name, objects: map[string]bool{}}
					f.tags[f.nextID] = tag
					f.nextID++
				}
				if tag.objects[key] {
					return httpmock.NewStringResponse(422, `{"message": "attached twice, which Superset 3.1 turns into a duplicate relation"}`), nil
				}
				tag.objects[key] = true
				f.attaches++
			}
			return httpmock.NewStringResponse(201, `{}`), nil
		})
	httpmock.RegisterRegexpResponder("DELETE", fakeTagUntagPath,
		func(req *http.Request) (*http.Response, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.disabled {
				return notFound(), nil
			}
			match := fakeTagUntagPath.FindStringSubmatch(req.URL.String())
			key := objectKey(match[1], match[2])
			tag := f.byName(match[3])
			if tag == nil || !tag.objects[key] {
				return httpmock.NewStringResponse(404, `{"message": "Not found"}`), nil
			}
			delete(tag.objects, key)
			f.detaches++
			return httpmock.NewStringResponse(200, `{"message": "OK"}`), nil
		})
}

// tag returns the stored tag with the given name, or nil.
func (f *fakeTags) tag(name string) *fakeTagEntry {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.byName(name)
}

func TestAccTagResource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/csrf_token/",
		httpmock.NewStringResponder(200, `{"result": "fake-csrf-token"}`))
	fake := newFakeTags()
	fake.serve()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
resource "superset_tag" "test" {
  name        = "finance"
  description = "Finance team"
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_tag.test", "id", "10"),
					resource.TestCheckResourceAttr("superset_tag.test", "name", "finance"),
					resource.TestCheckResourceAttr("superset_tag.test", "description", "Finance team"),
				),
			},
			{
				ResourceName:      "superset_tag.test",
				ImportState:       true,
				ImportStateVerify: true,
			},
			{
				// A rename keeps the objects the tag is attached to: they are sent back with the update.
				PreConfig: func() {
					fake.mu.Lock()
					fake.tags[10].objects["chart:7"] = true
					fake.tags[10].objects["dashboard:3"] = true
					fake.mu.Unlock()
				},
				Config: providerConfig + `
resource "superset_tag" "test" {
  name = "finance-team"
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_tag.test", "id", "10"),
					resource.TestCheckResourceAttr("superset_tag.test", "name", "finance-team"),
					resource.TestCheckNoResourceAttr("superset_tag.test", "description"),
					func(*terraform.State) error {
						tag := fake.tag("finance-team")
						if tag == nil || !tag.objects["chart:7"] || !tag.objects["dashboard:3"] || len(tag.objects) != 2 {
							return fmt.Errorf("the renamed tag must keep its objects, got %v", tag)
						}
						fake.mu.Lock()
						defer fake.mu.Unlock()
						if fake.attaches != 0 {
							return fmt.Errorf("the objects must not be attached again, got %d attaches", fake.attaches)
						}
						return nil
					},
				),
			},
			{
				// A tag deleted outside Terraform is recreated.
				PreConfig: func() {
					fake.mu.Lock()
					delete(fake.tags, 10)
					fake.mu.Unlock()
				},
				Config: providerConfig + `
resource "superset_tag" "test" {
  name = "finance-team"
}
`,
				Check: resource.TestCheckResourceAttr("superset_tag.test", "id", "11"),
			},
			{
				// The destroy detaches the tag first: the fake refuses to delete an attached tag.
				PreConfig: func() {
					fake.mu.Lock()
					fake.tags[11].objects["chart:7"] = true
					fake.mu.Unlock()
				},
				Config:  providerConfig,
				Destroy: true,
				Check: func(*terraform.State) error {
					if fake.tag("finance-team") != nil {
						return errors.New("the tag must be deleted")
					}
					return nil
				},
			},
		},
	})
}

func TestAccTagResourceExisting(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/csrf_token/",
		httpmock.NewStringResponder(200, `{"result": "fake-csrf-token"}`))
	fake := newFakeTags()
	fake.tags[4] = &fakeTagEntry{name: "finance", objects: map[string]bool{}}
	fake.serve()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
resource "superset_tag" "test" {
  name = "finance"
}
`,
				ExpectError: regexp.MustCompile(`a tag named "finance" already exists with id 4`),
			},
		},
	})
}

func TestAccTagResourceFeatureFlagOff(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/csrf_token/",
		httpmock.NewStringResponder(200, `{"result": "fake-csrf-token"}`))
	fake := newFakeTags()
	fake.disabled = true
	fake.serve()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
resource "superset_tag" "test" {
  name = "finance"
}
`,
				ExpectError: regexp.MustCompile(`TAGGING_SYSTEM feature flag`),
			},
		},
	})
}
