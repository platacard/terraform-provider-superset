package client

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/jarcoal/httpmock"
)

// fabList serves a Flask-AppBuilder list endpoint from rows the way Superset does: it applies
// the eq and rel_o_m filters, lowers page_size to maxPageSize and pages the remaining rows.
type fabList struct {
	mu          sync.Mutex
	rows        []map[string]any
	maxPageSize int
	requests    []string // the q argument of every request, in order
}

var (
	pageArg   = regexp.MustCompile(`page:(\d+)`)
	sizeArg   = regexp.MustCompile(`page_size:(\d+)`)
	filterArg = regexp.MustCompile(`\(col:(\w+),opr:(\w+),value:('(?:[^'!]|!.)*'|[^,)]+)\)`)
)

func (l *fabList) serve(path string) {
	httpmock.RegisterResponder("GET", "=~^"+regexp.QuoteMeta(testHost+path)+`\?`,
		func(req *http.Request) (*http.Response, error) {
			q := req.URL.Query().Get("q")
			l.mu.Lock()
			l.requests = append(l.requests, q)
			l.mu.Unlock()

			rows := l.rows
			for _, m := range filterArg.FindAllStringSubmatch(q, -1) {
				rows = filterRows(rows, m[1], m[2], m[3])
			}
			page, _ := strconv.Atoi(pageArg.FindStringSubmatch(q)[1])
			size, _ := strconv.Atoi(sizeArg.FindStringSubmatch(q)[1])
			size = min(size, l.maxPageSize)
			start := min(page*size, len(rows))
			end := min(start+size, len(rows))
			return httpmock.NewJsonResponse(200, map[string]any{"count": len(rows), "result": rows[start:end]})
		})
}

func filterRows(rows []map[string]any, column, operator, value string) []map[string]any {
	var kept []map[string]any
	for _, row := range rows {
		switch operator {
		case "eq":
			if s, ok := row[column].(string); ok && s == unrison(value) {
				kept = append(kept, row)
			}
		case "rel_o_m":
			if rel, ok := row[column].(map[string]any); ok && fmt.Sprint(rel["id"]) == value {
				kept = append(kept, row)
			}
		}
	}
	return kept
}

// unrison decodes a quoted rison string.
func unrison(s string) string {
	s = strings.TrimSuffix(strings.TrimPrefix(s, "'"), "'")
	return strings.NewReplacer("!'", "'", "!!", "!").Replace(s)
}

func roleRows(n int) []map[string]any {
	rows := make([]map[string]any, 0, n)
	for i := 1; i <= n; i++ {
		rows = append(rows, map[string]any{"id": i, "name": fmt.Sprintf("role-%03d", i)})
	}
	return rows
}

func TestRisonString(t *testing.T) {
	for in, want := range map[string]string{
		"Gamma":                "'Gamma'",
		"[Trino].[sales data]": "'[Trino].[sales data]'",
		"it's":                 "'it!'s'",
		"wow!":                 "'wow!!'",
		"":                     "''",
	} {
		if got := risonString(in); got != want {
			t.Errorf("risonString(%q) = %s, want %s", in, got, want)
		}
	}
}

func TestListQuery(t *testing.T) {
	if got, want := listQuery(nil, 2), "(page:2,page_size:1000)"; got != want {
		t.Errorf("without filters: got %s, want %s", got, want)
	}
	got := listQuery([]listFilter{equals("name", "Gamma"), relatedTo("view_menu", 8)}, 0)
	want := "(filters:!((col:name,opr:eq,value:'Gamma'),(col:view_menu,opr:rel_o_m,value:8)),page:0,page_size:1000)"
	if got != want {
		t.Errorf("with filters: got %s, want %s", got, want)
	}
}

func TestFetchRolesFollowsServerPages(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	registerLogin("token-1")
	list := &fabList{rows: roleRows(250), maxPageSize: 100}
	list.serve("/api/v1/security/roles/")

	roles, err := NewClient(testHost, "user", "pass").FetchRoles()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(roles) != 250 {
		t.Fatalf("expected all 250 roles, got %d", len(roles))
	}
	if roles[0].Name != "role-001" || roles[249].Name != "role-250" {
		t.Fatalf("rows out of order: first %q, last %q", roles[0].Name, roles[249].Name)
	}
	if len(list.requests) != 3 {
		t.Fatalf("expected 3 page requests for 250 rows capped at 100 per page, got %d: %v", len(list.requests), list.requests)
	}
	for i, q := range list.requests {
		if want := fmt.Sprintf("(page:%d,page_size:1000)", i); q != want {
			t.Errorf("request %d: got %s, want %s", i, q, want)
		}
	}
}

func TestFetchRolesEmptyList(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	registerLogin("token-1")
	list := &fabList{maxPageSize: 100}
	list.serve("/api/v1/security/roles/")

	roles, err := NewClient(testHost, "user", "pass").FetchRoles()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(roles) != 0 || len(list.requests) != 1 {
		t.Fatalf("expected no roles after a single request, got %d roles and %d requests", len(roles), len(list.requests))
	}
}

func TestGetRoleIDByNameFiltersOnTheServer(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	registerLogin("token-1")
	list := &fabList{rows: roleRows(250), maxPageSize: 100}
	list.serve("/api/v1/security/roles/")
	c := NewClient(testHost, "user", "pass")

	id, err := c.GetRoleIDByName("role-237")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id != 237 {
		t.Fatalf("expected role id 237, got %d", id)
	}
	if len(list.requests) != 1 || !strings.Contains(list.requests[0], "filters:!((col:name,opr:eq,value:'role-237'))") {
		t.Fatalf("expected a single filtered request, got %v", list.requests)
	}

	_, err = c.GetRoleIDByName("role-999")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for a missing role, got %v", err)
	}
}

func TestPermissionLookupFetchesEachViewMenuOnce(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	registerLogin("token-1")
	viewMenu := func(id int, name string) map[string]any { return map[string]any{"id": id, "name": name} }
	resources := &fabList{maxPageSize: 100, rows: []map[string]any{viewMenu(8, "Dashboard"), viewMenu(9, "[Trino].[sales]")}}
	resources.serve("/api/v1/security/resources/")
	permissions := &fabList{maxPageSize: 100, rows: []map[string]any{
		{"id": 15, "permission": viewMenu(1, "can_read"), "view_menu": viewMenu(8, "Dashboard")},
		{"id": 16, "permission": viewMenu(2, "can_write"), "view_menu": viewMenu(8, "Dashboard")},
		{"id": 91, "permission": viewMenu(3, "schema_access"), "view_menu": viewMenu(9, "[Trino].[sales]")},
	}}
	permissions.serve("/api/v1/security/permissions-resources/")
	c := NewClient(testHost, "user", "pass")

	for _, tc := range []struct {
		permission, viewMenu string
		want                 int64
	}{
		{"can_read", "Dashboard", 15},
		{"can_write", "Dashboard", 16},
		{"schema_access", "[Trino].[sales]", 91},
		{"can_read", "Dashboard", 15},
	} {
		id, err := c.GetPermissionIDByNameAndView(tc.permission, tc.viewMenu)
		if err != nil {
			t.Fatalf("%s on %s: unexpected error: %v", tc.permission, tc.viewMenu, err)
		}
		if id != tc.want {
			t.Fatalf("%s on %s: expected id %d, got %d", tc.permission, tc.viewMenu, tc.want, id)
		}
	}
	if len(resources.requests) != 2 || len(permissions.requests) != 2 {
		t.Fatalf("expected one view menu lookup and one permission listing per view menu, got %d and %d requests",
			len(resources.requests), len(permissions.requests))
	}
	if !strings.Contains(resources.requests[1], "value:'[Trino].[sales]'") || !strings.Contains(permissions.requests[1], "(col:view_menu,opr:rel_o_m,value:9)") {
		t.Fatalf("expected filtered requests, got %v and %v", resources.requests, permissions.requests)
	}

	_, err := c.GetPermissionIDByNameAndView("can_delete", "Dashboard")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for a permission the view menu does not have, got %v", err)
	}
	_, err = c.GetPermissionIDByNameAndView("can_read", "Nowhere")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for an unknown view menu, got %v", err)
	}
	if len(resources.requests) != 3 || len(permissions.requests) != 2 {
		t.Fatalf("expected the cache to serve known view menus and no permission listing for an unknown one, got %d and %d requests",
			len(resources.requests), len(permissions.requests))
	}
}

func TestCreateRoleReportsLookupFailures(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	registerLogin("token-1")
	httpmock.RegisterResponder("GET", "=~^"+regexp.QuoteMeta(testHost+"/api/v1/security/roles/")+`\?`,
		httpmock.NewStringResponder(500, `{"message": "boom"}`))
	httpmock.RegisterResponder("POST", testHost+"/api/v1/security/roles/",
		httpmock.NewStringResponder(201, `{"id": 7}`))

	_, err := NewClient(testHost, "user", "pass").CreateRole("Analysts")
	if err == nil {
		t.Fatal("expected the lookup failure to be reported")
	}
	if n := httpmock.GetCallCountInfo()["POST "+testHost+"/api/v1/security/roles/"]; n != 0 {
		t.Fatalf("expected no creation attempt after a failed lookup, got %d", n)
	}
}

func TestResolvePermissionIDsReportsEveryMissingReference(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	registerLogin("token-1")
	viewMenu := func(id int, name string) map[string]any { return map[string]any{"id": id, "name": name} }
	resources := &fabList{maxPageSize: 100, rows: []map[string]any{viewMenu(8, "Dashboard"), viewMenu(9, "Chart")}}
	resources.serve("/api/v1/security/resources/")
	permissions := &fabList{maxPageSize: 100, rows: []map[string]any{
		{"id": 15, "permission": viewMenu(1, "can_read"), "view_menu": viewMenu(8, "Dashboard")},
		{"id": 17, "permission": viewMenu(1, "can_read"), "view_menu": viewMenu(9, "Chart")},
	}}
	permissions.serve("/api/v1/security/permissions-resources/")
	c := NewClient(testHost, "user", "pass")

	ids, err := c.ResolvePermissionIDs([]PermissionRef{
		{"can_read", "Dashboard"}, {"can_read", "Chart"}, {"can_read", "Dashboard"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ids[PermissionRef{"can_read", "Dashboard"}] != 15 || ids[PermissionRef{"can_read", "Chart"}] != 17 {
		t.Fatalf("unexpected ids: %v", ids)
	}
	if len(resources.requests) != 2 || len(permissions.requests) != 2 {
		t.Fatalf("expected one lookup per distinct view menu, got %d and %d requests", len(resources.requests), len(permissions.requests))
	}

	_, err = c.ResolvePermissionIDs([]PermissionRef{
		{"can_read", "Dashboard"}, {"can_write", "Dashboard"}, {"can_read", "Nowhere"},
	})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	for _, want := range []string{"can_write on Dashboard", "can_read on Nowhere"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("expected the error to list %q, got: %v", want, err)
		}
	}
	if strings.Contains(err.Error(), "can_read on Dashboard") {
		t.Errorf("a resolvable reference must not be reported as missing: %v", err)
	}
}
