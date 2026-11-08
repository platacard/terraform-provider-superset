package provider

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"sync"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/plancheck"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
	"github.com/jarcoal/httpmock"
)

// fakeAnnotation serves the annotation endpoints of the fake Superset host for one annotation
// with id 5 in whichever layer it was created in. Like Superset, it refuses an end before the
// start and a duplicate title, and an update merges the fields it carries.
type fakeAnnotation struct {
	mu         sync.Mutex
	layer      int64
	annotation map[string]any
	deleted    bool
	// last is the payload of the last create or update request.
	last map[string]any
}

var fakeAnnotationPath = regexp.MustCompile(`^http://superset-host/api/v1/annotation_layer/(\d+)/annotation/(5)?$`)

func (f *fakeAnnotation) serve() {
	layerOf := func(req *http.Request) int64 {
		match := fakeAnnotationPath.FindStringSubmatch(req.URL.String())
		layer, _ := strconv.ParseInt(match[1], 10, 64)
		return layer
	}
	decode := func(req *http.Request) map[string]any {
		var payload map[string]any
		_ = json.NewDecoder(req.Body).Decode(&payload)
		return payload
	}
	invalid := func(payload map[string]any) *http.Response {
		start, _ := payload["start_dttm"].(string)
		end, _ := payload["end_dttm"].(string)
		if end != "" && start != "" && end < start {
			return httpmock.NewStringResponse(422, `{"message": {"start_dttm": ["End date must be after start date"]}}`)
		}
		return nil
	}
	httpmock.RegisterRegexpResponder("POST", fakeAnnotationPath,
		func(req *http.Request) (*http.Response, error) {
			payload := decode(req)
			f.mu.Lock()
			defer f.mu.Unlock()
			if resp := invalid(payload); resp != nil {
				return resp, nil
			}
			if !f.deleted && f.annotation != nil && f.annotation["short_descr"] == payload["short_descr"] && f.layer == layerOf(req) {
				return httpmock.NewStringResponse(422, `{"message": {"short_descr": ["Short description must be unique for this layer"]}}`), nil
			}
			f.layer, f.annotation, f.last, f.deleted = layerOf(req), payload, payload, false
			return httpmock.NewStringResponse(201, `{"id": 5}`), nil
		})
	httpmock.RegisterRegexpResponder("GET", fakeAnnotationPath,
		func(req *http.Request) (*http.Response, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.deleted || f.annotation == nil || f.layer != layerOf(req) {
				return httpmock.NewStringResponse(404, `{"message": "Not found"}`), nil
			}
			result := map[string]any{"id": 5, "layer": map[string]any{"id": f.layer, "name": "Releases"}}
			for _, key := range []string{"short_descr", "long_descr", "start_dttm", "end_dttm", "json_metadata"} {
				result[key] = f.annotation[key]
			}
			return httpmock.NewJsonResponse(200, map[string]any{"id": 5, "result": result})
		})
	httpmock.RegisterRegexpResponder("PUT", fakeAnnotationPath,
		func(req *http.Request) (*http.Response, error) {
			payload := decode(req)
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.deleted || f.layer != layerOf(req) {
				return httpmock.NewStringResponse(404, `{"message": "Not found"}`), nil
			}
			if resp := invalid(payload); resp != nil {
				return resp, nil
			}
			for key, value := range payload {
				f.annotation[key] = value
			}
			f.last = payload
			return httpmock.NewStringResponse(200, `{"id": 5}`), nil
		})
	httpmock.RegisterRegexpResponder("DELETE", fakeAnnotationPath,
		func(req *http.Request) (*http.Response, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.deleted || f.layer != layerOf(req) {
				return httpmock.NewStringResponse(404, `{"message": "Not found"}`), nil
			}
			f.deleted = true
			return httpmock.NewStringResponse(200, `{"message": "OK"}`), nil
		})
}

// sent returns a value of the last create or update request.
func (f *fakeAnnotation) sent(key string) any {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.last[key]
}

const annotationLayerConfig = providerConfig + `
resource "superset_annotation_layer" "test" {
  name = "Releases"
}
`

const minimalAnnotation = annotationLayerConfig + `
resource "superset_annotation" "test" {
  layer_id    = superset_annotation_layer.test.id
  short_descr = "v2.0.0"
  start_dttm  = "2026-01-15T09:00:00"
  end_dttm    = "2026-01-15T09:00:00"
}
`

const fullAnnotation = annotationLayerConfig + `
resource "superset_annotation" "test" {
  layer_id      = superset_annotation_layer.test.id
  short_descr   = "v2.0.1"
  long_descr    = "Hotfix"
  start_dttm    = "2026-01-16T09:00:00"
  end_dttm      = "2026-01-16T18:00:00"
  json_metadata = jsonencode({ color = "#999999" })
}
`

func TestAccAnnotationResource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	layer := &fakeAnnotationLayer{}
	layer.serve()
	fake := &fakeAnnotation{}
	fake.serve()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: minimalAnnotation,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_annotation.test", "id", "5"),
					resource.TestCheckResourceAttr("superset_annotation.test", "layer_id", "2"),
					resource.TestCheckResourceAttr("superset_annotation.test", "short_descr", "v2.0.0"),
					resource.TestCheckResourceAttr("superset_annotation.test", "start_dttm", "2026-01-15T09:00:00"),
					resource.TestCheckResourceAttr("superset_annotation.test", "end_dttm", "2026-01-15T09:00:00"),
					resource.TestCheckNoResourceAttr("superset_annotation.test", "long_descr"),
					resource.TestCheckNoResourceAttr("superset_annotation.test", "json_metadata"),
					func(*terraform.State) error {
						if fake.sent("long_descr") != nil || fake.sent("json_metadata") != nil {
							return errors.New("unset attributes must be sent as null")
						}
						return nil
					},
				),
			},
			{
				ResourceName:      "superset_annotation.test",
				ImportState:       true,
				ImportStateId:     "2/5",
				ImportStateVerify: true,
			},
			{
				Config: fullAnnotation,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_annotation.test", "id", "5"),
					resource.TestCheckResourceAttr("superset_annotation.test", "short_descr", "v2.0.1"),
					resource.TestCheckResourceAttr("superset_annotation.test", "long_descr", "Hotfix"),
					resource.TestCheckResourceAttr("superset_annotation.test", "end_dttm", "2026-01-16T18:00:00"),
					resource.TestCheckResourceAttr("superset_annotation.test", "json_metadata", `{"color":"#999999"}`),
					func(*terraform.State) error {
						if fake.sent("json_metadata") != `{"color":"#999999"}` {
							return fmt.Errorf("the metadata must be sent as a JSON string, got %v", fake.sent("json_metadata"))
						}
						return nil
					},
				),
			},
			{
				// Removing the optional attributes clears them with null.
				Config: minimalAnnotation,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckNoResourceAttr("superset_annotation.test", "long_descr"),
					resource.TestCheckNoResourceAttr("superset_annotation.test", "json_metadata"),
					func(*terraform.State) error {
						if value, present := fake.last["long_descr"]; !present || value != nil {
							return errors.New("a cleared description must be sent as null")
						}
						return nil
					},
				),
			},
			{
				// Another layer means a replacement.
				Config: providerConfig + `
resource "superset_annotation" "test" {
  layer_id    = 3
  short_descr = "v2.0.0"
  start_dttm  = "2026-01-15T09:00:00"
  end_dttm    = "2026-01-15T09:00:00"
}
`,
				ConfigPlanChecks: resource.ConfigPlanChecks{
					PreApply: []plancheck.PlanCheck{
						plancheck.ExpectResourceAction("superset_annotation.test", plancheck.ResourceActionReplace),
					},
				},
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_annotation.test", "layer_id", "3"),
					func(*terraform.State) error {
						fake.mu.Lock()
						defer fake.mu.Unlock()
						if fake.layer != 3 {
							return fmt.Errorf("the annotation must be recreated in layer 3, got %d", fake.layer)
						}
						return nil
					},
				),
			},
			{
				// An annotation deleted outside Terraform is recreated.
				PreConfig: func() {
					fake.mu.Lock()
					fake.deleted = true
					fake.mu.Unlock()
				},
				Config: providerConfig + `
resource "superset_annotation" "test" {
  layer_id    = 3
  short_descr = "v2.0.0"
  start_dttm  = "2026-01-15T09:00:00"
  end_dttm    = "2026-01-15T09:00:00"
}
`,
				Check: resource.TestCheckResourceAttr("superset_annotation.test", "id", "5"),
			},
		},
	})
}

func TestAccAnnotationResourceValidation(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	layer := &fakeAnnotationLayer{}
	layer.serve()
	fake := &fakeAnnotation{}
	fake.serve()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: annotationLayerConfig + `
resource "superset_annotation" "test" {
  layer_id    = superset_annotation_layer.test.id
  short_descr = "v2.0.0"
  start_dttm  = "2026-01-15"
  end_dttm    = "2026-01-15T09:00:00Z"
}
`,
				ExpectError: regexp.MustCompile(`start_dttm must be written as YYYY-MM-DDTHH:MM:SS`),
			},
			{
				Config: annotationLayerConfig + `
resource "superset_annotation" "test" {
  layer_id    = superset_annotation_layer.test.id
  short_descr = "v2.0.0"
  start_dttm  = "2026-01-15T09:00:00"
  end_dttm    = "2026-01-15T09:00:00"
  json_metadata = "{not json"
}
`,
				ExpectError: regexp.MustCompile(`(?s)json_metadata.*JSON`),
			},
			{
				Config: annotationLayerConfig + `
resource "superset_annotation" "test" {
  layer_id    = superset_annotation_layer.test.id
  short_descr = "v2.0.0"
  start_dttm  = "2026-01-16T09:00:00"
  end_dttm    = "2026-01-15T09:00:00"
}
`,
				ExpectError: regexp.MustCompile(`End date must be after start date`),
			},
		},
	})
}
