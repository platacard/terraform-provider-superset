package provider

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"sync"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
	"github.com/jarcoal/httpmock"
)

// fakeDatabase serves the database endpoints of the fake Superset host for one connection with
// id 208. Like Superset it masks the password of the URI and the secrets of the tunnel, breaks
// the URI into parameters and reports the backend without its driver. With legacyTunnel set it
// behaves like Superset 3.1, which only removes a tunnel through its own endpoint; otherwise a
// null tunnel in an update removes the tunnel and that endpoint does not exist.
type fakeDatabase struct {
	mu           sync.Mutex
	conn         map[string]any
	tunnel       map[string]any
	deleted      bool
	legacyTunnel bool
}

const mask = "XXXXXXXXXX"

func (f *fakeDatabase) serve() {
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/security/login",
		httpmock.NewStringResponder(200, `{"access_token": "fake-token"}`))
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/security/csrf_token/",
		httpmock.NewStringResponder(200, `{"result": "fake-csrf-token"}`))

	store := func(req *http.Request, create bool) *http.Response {
		var payload map[string]any
		if err := json.NewDecoder(req.Body).Decode(&payload); err != nil {
			return httpmock.NewStringResponse(400, `{"message": "bad json"}`)
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		f.conn, f.deleted = payload, false
		if tunnel, ok := payload["ssh_tunnel"].(map[string]any); ok {
			f.tunnel = tunnel
		} else if create || !f.legacyTunnel {
			f.tunnel = nil
		}
		return httpmock.NewStringResponse(map[bool]int{true: 201, false: 200}[create], `{"id": 208, "result": {}}`)
	}
	httpmock.RegisterResponder("POST", "http://superset-host/api/v1/database/",
		func(req *http.Request) (*http.Response, error) { return store(req, true), nil })
	httpmock.RegisterResponder("PUT", "http://superset-host/api/v1/database/208",
		func(req *http.Request) (*http.Response, error) { return store(req, false), nil })
	httpmock.RegisterResponder("GET", "http://superset-host/api/v1/database/208/connection",
		func(req *http.Request) (*http.Response, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			// Like Superset, the connection endpoint answers with an empty result for a
			// database that does not exist.
			if f.deleted || f.conn == nil {
				return httpmock.NewStringResponse(200, `{"result": {}}`), nil
			}
			result := map[string]any{"id": 208}
			for _, key := range []string{"database_name", "expose_in_sqllab", "allow_ctas", "allow_cvas", "allow_dml", "allow_run_async",
				"allow_file_upload", "impersonate_user", "cache_timeout", "force_ctas_schema", "extra", "server_cert"} {
				result[key] = f.conn[key]
			}
			if f.conn["masked_encrypted_extra"] != nil {
				result["masked_encrypted_extra"] = `{"token": "` + mask + `"}`
			}
			rawURI, _ := f.conn["sqlalchemy_uri"].(string)
			uri, _ := url.Parse(rawURI)
			result["backend"] = regexp.MustCompile(`\+.*`).ReplaceAllString(uri.Scheme, "")
			result["parameters"] = map[string]any{}
			if uri.Host != "" {
				port, _ := strconv.Atoi(uri.Port())
				result["parameters"] = map[string]any{"host": uri.Hostname(), "port": port, "username": uri.User.Username(),
					"password": mask, "database": uri.Path[1:]}
				uri.User = url.UserPassword(uri.User.Username(), mask)
			}
			result["sqlalchemy_uri"] = uri.String()
			if f.tunnel != nil {
				tunnel := map[string]any{"id": 1, "server_address": f.tunnel["server_address"], "server_port": f.tunnel["server_port"], "username": f.tunnel["username"]}
				for _, secret := range []string{"password", "private_key", "private_key_password"} {
					if f.tunnel[secret] != nil {
						tunnel[secret] = mask
					}
				}
				result["ssh_tunnel"] = tunnel
			} else {
				result["ssh_tunnel"] = nil
			}
			return httpmock.NewJsonResponse(200, map[string]any{"result": result})
		})
	httpmock.RegisterResponder("DELETE", "http://superset-host/api/v1/database/208/ssh_tunnel/",
		func(req *http.Request) (*http.Response, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if !f.legacyTunnel || f.tunnel == nil {
				return httpmock.NewStringResponse(404, `{"message": "Not found"}`), nil
			}
			f.tunnel = nil
			return httpmock.NewStringResponse(200, `{"message": "OK"}`), nil
		})
	httpmock.RegisterResponder("DELETE", "http://superset-host/api/v1/database/208",
		func(req *http.Request) (*http.Response, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.deleted = true
			return httpmock.NewStringResponse(200, `{"message": "OK"}`), nil
		})
}

// sent returns the value of a key of the last create or update request.
func (f *fakeDatabase) sent(key string) any {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.conn[key]
}

// tunnelValue returns one field of the stored tunnel, or nil without a tunnel.
func (f *fakeDatabase) tunnelValue(key string) any {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.tunnel == nil {
		return nil
	}
	return f.tunnel[key]
}

const databaseFlags = `
  allow_ctas       = false
  allow_cvas       = false
  allow_dml        = false
  allow_run_async  = true
  expose_in_sqllab = true
`

const partsConfig = providerConfig + `
resource "superset_database" "test" {
  connection_name = "DWH"
  db_engine       = "postgresql+psycopg2"
  db_user         = "superset_user"
  db_pass         = "dbpassword"
  db_host         = "pg.db.ro.domain.com"
  db_port         = 5432
  db_name         = "superset_db"
` + databaseFlags + `
}
`

const tunnelConfig = providerConfig + `
resource "superset_database" "test" {
  connection_name   = "DWH"
  sqlalchemy_uri    = "postgresql://superset_user:dbpassword@pg.internal:5432/superset_db"
  allow_file_upload = true
  cache_timeout     = 600
  force_ctas_schema = "scratch"
  extra             = jsonencode({ client_encoding = "utf8", schemas_allowed_for_file_upload = ["scratch"] })
  masked_encrypted_extra = jsonencode({ token = "secret" })
  server_cert       = "-----BEGIN CERTIFICATE-----"
` + databaseFlags + `
  ssh_tunnel = {
    server_address = "bastion.internal"
    server_port    = 22
    username       = "tunnel"
    password       = "tunnelpass"
  }
}
`

const keyTunnelConfig = providerConfig + `
resource "superset_database" "test" {
  connection_name = "DWH"
  sqlalchemy_uri  = "postgresql://superset_user:dbpassword@pg.internal:5432/superset_db"
` + databaseFlags + `
  ssh_tunnel = {
    server_address = "bastion.internal"
    server_port    = 2222
    username       = "tunnel"
    private_key    = "-----BEGIN RSA PRIVATE KEY-----"
  }
}
`

const uriConfig = providerConfig + `
resource "superset_database" "test" {
  connection_name = "DWH"
  sqlalchemy_uri  = "postgresql://superset_user:dbpassword@pg.internal:5432/superset_db"
` + databaseFlags + `
}
`

func TestAccDatabaseResource(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeDatabase{legacyTunnel: true}
	fake.serve()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: partsConfig,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_database.test", "id", "208"),
					resource.TestCheckResourceAttr("superset_database.test", "connection_name", "DWH"),
					resource.TestCheckResourceAttr("superset_database.test", "db_engine", "postgresql+psycopg2"),
					resource.TestCheckResourceAttr("superset_database.test", "db_user", "superset_user"),
					resource.TestCheckResourceAttr("superset_database.test", "db_pass", "dbpassword"),
					resource.TestCheckResourceAttr("superset_database.test", "db_host", "pg.db.ro.domain.com"),
					resource.TestCheckResourceAttr("superset_database.test", "db_port", "5432"),
					resource.TestCheckResourceAttr("superset_database.test", "db_name", "superset_db"),
					resource.TestCheckResourceAttr("superset_database.test", "allow_file_upload", "false"),
					resource.TestCheckResourceAttr("superset_database.test", "impersonate_user", "false"),
					resource.TestCheckResourceAttr("superset_database.test", "extra", `{"client_encoding": "utf8"}`),
					resource.TestCheckNoResourceAttr("superset_database.test", "sqlalchemy_uri"),
					resource.TestCheckNoResourceAttr("superset_database.test", "ssh_tunnel"),
					func(*terraform.State) error {
						if fake.sent("sqlalchemy_uri") != "postgresql+psycopg2://superset_user:dbpassword@pg.db.ro.domain.com:5432/superset_db" {
							return errors.New("the parts must be assembled into the URI")
						}
						return nil
					},
				),
			},
			{
				ResourceName:            "superset_database.test",
				ImportState:             true,
				ImportStateVerify:       true,
				ImportStateVerifyIgnore: []string{"db_pass", "db_engine"},
			},
			{
				// A host changed in the UI is drift; the password stays the configured one.
				PreConfig: func() {
					fake.mu.Lock()
					fake.conn["sqlalchemy_uri"] = "postgresql+psycopg2://superset_user:other@pg.other.domain.com:5432/superset_db"
					fake.mu.Unlock()
				},
				Config:             partsConfig,
				PlanOnly:           true,
				ExpectNonEmptyPlan: true,
			},
			{
				Config: tunnelConfig,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_database.test", "sqlalchemy_uri", "postgresql://superset_user:dbpassword@pg.internal:5432/superset_db"),
					resource.TestCheckNoResourceAttr("superset_database.test", "db_host"),
					resource.TestCheckResourceAttr("superset_database.test", "allow_file_upload", "true"),
					resource.TestCheckResourceAttr("superset_database.test", "cache_timeout", "600"),
					resource.TestCheckResourceAttr("superset_database.test", "force_ctas_schema", "scratch"),
					resource.TestCheckResourceAttr("superset_database.test", "masked_encrypted_extra", `{"token":"secret"}`),
					resource.TestCheckResourceAttr("superset_database.test", "server_cert", "-----BEGIN CERTIFICATE-----"),
					resource.TestCheckResourceAttr("superset_database.test", "ssh_tunnel.server_address", "bastion.internal"),
					resource.TestCheckResourceAttr("superset_database.test", "ssh_tunnel.server_port", "22"),
					resource.TestCheckResourceAttr("superset_database.test", "ssh_tunnel.password", "tunnelpass"),
					resource.TestCheckNoResourceAttr("superset_database.test", "ssh_tunnel.private_key"),
					func(*terraform.State) error {
						if fake.tunnelValue("password") != "tunnelpass" {
							return errors.New("the tunnel password must be sent")
						}
						return nil
					},
				),
			},
			{
				// A tunnel server changed in the UI is drift.
				PreConfig: func() {
					fake.mu.Lock()
					fake.tunnel["server_address"] = "other.internal"
					fake.mu.Unlock()
				},
				Config:             tunnelConfig,
				PlanOnly:           true,
				ExpectNonEmptyPlan: true,
			},
			{
				Config: keyTunnelConfig,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("superset_database.test", "ssh_tunnel.server_port", "2222"),
					resource.TestCheckResourceAttr("superset_database.test", "ssh_tunnel.private_key", "-----BEGIN RSA PRIVATE KEY-----"),
					resource.TestCheckNoResourceAttr("superset_database.test", "ssh_tunnel.password"),
					// A cache timeout removed from the configuration keeps its value.
					resource.TestCheckResourceAttr("superset_database.test", "cache_timeout", "600"),
					resource.TestCheckNoResourceAttr("superset_database.test", "force_ctas_schema"),
					resource.TestCheckNoResourceAttr("superset_database.test", "masked_encrypted_extra"),
					resource.TestCheckNoResourceAttr("superset_database.test", "server_cert"),
					func(*terraform.State) error {
						if fake.tunnelValue("private_key") == nil || fake.tunnelValue("password") != nil {
							return errors.New("switching to a private key must send the key and no password")
						}
						return nil
					},
				),
			},
			{
				// Removing the tunnel from the configuration removes it in Superset, through the
				// dedicated endpoint of Superset 3.1 here.
				Config: uriConfig,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckNoResourceAttr("superset_database.test", "ssh_tunnel"),
					func(*terraform.State) error {
						if fake.tunnelValue("username") != nil {
							return errors.New("the tunnel must be removed")
						}
						return nil
					},
				),
			},
			{
				PreConfig: func() {
					fake.mu.Lock()
					fake.deleted = true
					fake.mu.Unlock()
				},
				Config: uriConfig,
				Check:  resource.TestCheckResourceAttr("superset_database.test", "id", "208"),
			},
		},
	})
}

func TestAccDatabaseResourceTunnelRemovalOnRecentSuperset(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeDatabase{}
	fake.serve()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: tunnelConfig,
				Check:  resource.TestCheckResourceAttr("superset_database.test", "ssh_tunnel.username", "tunnel"),
			},
			{
				Config: uriConfig,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckNoResourceAttr("superset_database.test", "ssh_tunnel"),
					func(*terraform.State) error {
						if fake.tunnelValue("username") != nil {
							return errors.New("the tunnel must be removed by the update")
						}
						return nil
					},
				),
			},
		},
	})
}

func TestAccDatabaseResourceValidation(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	fake := &fakeDatabase{}
	fake.serve()

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerConfig + `
resource "superset_database" "test" {
  connection_name = "DWH"
  sqlalchemy_uri  = "postgresql://u:p@h:5432/d"
  db_host         = "h"
` + databaseFlags + `
}
`,
				ExpectError: regexp.MustCompile("sqlalchemy_uri cannot be combined with"),
			},
			{
				Config: providerConfig + `
resource "superset_database" "test" {
  connection_name = "DWH"
  db_engine       = "postgresql"
  db_host         = "h"
` + databaseFlags + `
}
`,
				ExpectError: regexp.MustCompile("Set either sqlalchemy_uri or all of"),
			},
			{
				Config: providerConfig + `
resource "superset_database" "test" {
  connection_name = "DWH"
  sqlalchemy_uri  = "postgresql://u:p@h:5432/d"
` + databaseFlags + `
  ssh_tunnel = {
    server_address = "bastion"
    server_port    = 22
    username       = "tunnel"
    password       = "p"
    private_key    = "k"
  }
}
`,
				ExpectError: regexp.MustCompile("Set exactly one of password and private_key"),
			},
			{
				Config: providerConfig + `
resource "superset_database" "test" {
  connection_name = "DWH"
  sqlalchemy_uri  = "postgresql://u:p@h:5432/d"
` + databaseFlags + `
  ssh_tunnel = {
    server_address = "bastion"
    server_port    = 22
    username       = "tunnel"
  }
}
`,
				ExpectError: regexp.MustCompile("Set exactly one of password and private_key"),
			},
		},
	})
}
