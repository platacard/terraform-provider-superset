package provider

import (
	"encoding/json"
	"net/http"
	"sync"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/jarcoal/httpmock"
)

// fakeAnnotationLayer serves the annotation layer endpoints of the fake Superset host for one
// layer with id 2.
type fakeAnnotationLayer struct {
	mu      sync.Mutex
	name    string
	descr   *string
	deleted bool
}

func (f *fakeAnnotationLayer) serve() {
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/csrf_token/",
		httpmock.NewStringResponder(200, `{"result": "fake-csrf-token"}`))

	// store keeps the submitted layer and reports whether the description was null: the update
	// schema of Superset rejects a null description, so the fake does the same.
	store := func(req *http.Request) (nullDescr bool) {
		var payload struct {
			Name  string  `json:"name"`
			Descr *string `json:"descr"`
		}
		if err := json.NewDecoder(req.Body).Decode(&payload); err != nil {
			return false
		}
		f.mu.Lock()
		f.name, f.descr, f.deleted = payload.Name, payload.Descr, false
		f.mu.Unlock()
		return payload.Descr == nil
	}
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/annotation_layer/",
		func(req *http.Request) (*http.Response, error) {
			store(req)
			return httpmock.NewStringResponse(201, `{"id": 2}`), nil
		})
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/annotation_layer/2",
		func(req *http.Request) (*http.Response, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.deleted {
				return httpmock.NewStringResponse(404, `{"message": "Not found"}`), nil
			}
			return httpmock.NewJsonResponse(200, map[string]any{"id": 2, "result": map[string]any{"name": f.name, "descr": f.descr}})
		})
	httpmock.RegisterResponder("PUT", "http://superset-host/api/v1/annotation_layer/2",
		func(req *http.Request) (*http.Response, error) {
			if store(req) {
				return httpmock.NewStringResponse(400, `{"message": {"descr": ["Field may not be null."]}}`), nil
			}
			return httpmock.NewStringResponse(200, `{"id": 2}`), nil
		})
	httpmock.RegisterResponder("DELETE", "http://superset-host/api/v1/annotation_layer/2",
		func(req *http.Request) (*http.Response, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.deleted = true
			return httpmock.NewStringResponse(200, `{"message": "OK"}`), nil
		})
}

func TestAccAnnotationLayerResource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeAnnotationLayer{}
	fake.serve()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
resource "superset_annotation_layer" "test" {
  name = "Releases"
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_annotation_layer.test", "id", "2"),
					resource.TestCheckResourceAttr("superset_annotation_layer.test", "name", "Releases"),
					resource.TestCheckNoResourceAttr("superset_annotation_layer.test", "description"),
				),
			},
			{
				ResourceName:      "superset_annotation_layer.test",
				ImportState:       true,
				ImportStateVerify: true,
			},
			{
				Config: providerConfig + `
resource "superset_annotation_layer" "test" {
  name        = "Product releases"
  description = "One annotation per release"
}
`,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_annotation_layer.test", "name", "Product releases"),
					resource.TestCheckResourceAttr("superset_annotation_layer.test", "description", "One annotation per release"),
				),
			},
			{
				Config: providerConfig + `
resource "superset_annotation_layer" "test" {
  name = "Product releases"
}
`,
				Check: resource.TestCheckNoResourceAttr("superset_annotation_layer.test", "description"),
			},
		},
	})
}
