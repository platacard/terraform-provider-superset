package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
)

// ErrNotFound reports that a lookup by name matched nothing.
var ErrNotFound = errors.New("not found")

// listPageSize is the page size requested from list endpoints. Superset lowers it to its own
// maximum (100 unless the instance is configured otherwise), so pages are followed by the
// number of rows the server actually returns rather than by the number requested.
const listPageSize = 1000

// listFilter is one entry of the filters argument of a Flask-AppBuilder list endpoint.
type listFilter struct {
	column   string
	operator string
	value    string
}

// equals matches rows whose column equals value.
func equals(column, value string) listFilter {
	return listFilter{column: column, operator: "eq", value: risonString(value)}
}

// hasID matches the row with the given id.
func hasID(id int64) listFilter {
	return equalsInt("id", id)
}

// equalsInt matches rows whose integer column equals value.
func equalsInt(column string, value int64) listFilter {
	return listFilter{column: column, operator: "eq", value: strconv.FormatInt(value, 10)}
}

// relatedTo matches rows whose many-to-one relation column points at the row with the given id.
func relatedTo(column string, id int64) listFilter {
	return listFilter{column: column, operator: "rel_o_m", value: strconv.FormatInt(id, 10)}
}

// risonString encodes s as a quoted rison string.
func risonString(s string) string {
	return "'" + strings.NewReplacer("!", "!!", "'", "!'").Replace(s) + "'"
}

// listQuery renders the rison q argument that selects one page of a list endpoint.
func listQuery(filters []listFilter, page int) string {
	var b strings.Builder
	b.WriteString("(")
	if len(filters) > 0 {
		b.WriteString("filters:!(")
		for i, f := range filters {
			if i > 0 {
				b.WriteString(",")
			}
			fmt.Fprintf(&b, "(col:%s,opr:%s,value:%s)", f.column, f.operator, f.value)
		}
		b.WriteString("),")
	}
	fmt.Fprintf(&b, "page:%d,page_size:%d)", page, listPageSize)
	return b.String()
}

// listPage is the envelope of a Flask-AppBuilder list response.
type listPage struct {
	Count  int               `json:"count"`
	Result []json.RawMessage `json:"result"`
}

// listAll returns every row of a list endpoint, following the pages the server returns.
func (c *Client) listAll(path string, filters ...listFilter) ([]json.RawMessage, error) {
	var rows []json.RawMessage
	for page := 0; ; page++ {
		query := url.Values{"q": {listQuery(filters, page)}}
		resp, err := c.DoRequest(http.MethodGet, path+"?"+query.Encode(), nil)
		if err != nil {
			return nil, err
		}
		current, err := decodeListPage(resp, path)
		if err != nil {
			return nil, err
		}
		rows = append(rows, current.Result...)
		if len(current.Result) == 0 || len(rows) >= current.Count {
			return rows, nil
		}
	}
}

// decodeListPage reads one list response and closes its body.
func decodeListPage(resp *http.Response, path string) (listPage, error) {
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return listPage{}, responseError("list "+path, resp, body)
	}

	var page listPage
	if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
		return listPage{}, fmt.Errorf("failed to decode %s response: %w", path, err)
	}
	return page, nil
}

// listInto returns every row of a list endpoint decoded as T.
func listInto[T any](c *Client, path string, filters ...listFilter) ([]T, error) {
	rows, err := c.listAll(path, filters...)
	if err != nil {
		return nil, err
	}

	items := make([]T, 0, len(rows))
	for _, row := range rows {
		var item T
		if err := json.Unmarshal(row, &item); err != nil {
			return nil, fmt.Errorf("failed to decode %s row: %w", path, err)
		}
		items = append(items, item)
	}
	return items, nil
}

// namedItem is the id and name of a security object: a role, a permission or a view menu.
type namedItem struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

// findIDByName returns the id of the row whose name is exactly name.
// The server already filters on equality; the name is compared again here because the
// comparison can be case-insensitive depending on the collation of the metadata database.
func (c *Client) findIDByName(path, name string) (int64, error) {
	items, err := listInto[namedItem](c, path, equals("name", name))
	if err != nil {
		return 0, err
	}
	for _, item := range items {
		if item.Name == name {
			return item.ID, nil
		}
	}
	return 0, ErrNotFound
}

// permissionView is a row of the permissions-resources endpoint: a permission granted on a view menu.
type permissionView struct {
	ID         int64     `json:"id"`
	Permission namedItem `json:"permission"`
	ViewMenu   namedItem `json:"view_menu"`
}

// permissionIndex caches the permissions of the view menus a run has already resolved, keyed
// by view menu name and then by permission name. The permission catalogue of a large instance
// has tens of thousands of rows and does not change while Terraform runs, so every view menu
// is fetched at most once per provider instance.
type permissionIndex struct {
	mu        sync.Mutex
	viewMenus map[string]map[string]int64
}

func (p *permissionIndex) lookup(viewMenu string) (map[string]int64, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	permissions, ok := p.viewMenus[viewMenu]
	return permissions, ok
}

// forget drops the cached permissions of a view menu, after a permission was granted on it
// or removed from it, so the next lookup reads them again.
func (p *permissionIndex) forget(viewMenu string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.viewMenus, viewMenu)
}

func (p *permissionIndex) store(viewMenu string, permissions map[string]int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.viewMenus == nil {
		p.viewMenus = make(map[string]map[string]int64)
	}
	p.viewMenus[viewMenu] = permissions
}

// viewMenuPermissions returns the permission ids of one view menu keyed by permission name.
// It costs two small requests the first time a view menu is seen: one to resolve the view menu
// id by name and one to list the permissions granted on it. Misses are not cached, so a view
// menu created later in the same run is found once it exists.
func (c *Client) viewMenuPermissions(viewMenu string) (map[string]int64, error) {
	if permissions, ok := c.permissions.lookup(viewMenu); ok {
		return permissions, nil
	}

	viewMenuID, err := c.findIDByName("/api/v1/security/resources/", viewMenu)
	if err != nil {
		return nil, fmt.Errorf("view menu %s: %w", viewMenu, err)
	}

	rows, err := listInto[permissionView](c, "/api/v1/security/permissions-resources/", relatedTo("view_menu", viewMenuID))
	if err != nil {
		return nil, err
	}

	permissions := make(map[string]int64, len(rows))
	for _, row := range rows {
		if row.ViewMenu.Name == viewMenu {
			permissions[row.Permission.Name] = row.ID
		}
	}
	c.permissions.store(viewMenu, permissions)
	return permissions, nil
}

// GetPermissionIDByNameAndView returns the id of the permission granted on the view menu.
func (c *Client) GetPermissionIDByNameAndView(permissionName, viewMenuName string) (int64, error) {
	permissions, err := c.viewMenuPermissions(viewMenuName)
	if err != nil {
		return 0, err
	}

	id, ok := permissions[permissionName]
	if !ok {
		return 0, fmt.Errorf("permission %s with view menu %s: %w", permissionName, viewMenuName, ErrNotFound)
	}
	return id, nil
}

// PermissionRef names a permission granted on a view menu.
type PermissionRef struct {
	Permission string
	ViewMenu   string
}

func (r PermissionRef) String() string {
	return r.Permission + " on " + r.ViewMenu
}

// lookupConcurrency bounds the number of view menus resolved at the same time.
const lookupConcurrency = 8

// ResolvePermissionIDs returns the id of every reference. Each distinct view menu is resolved
// with its own filtered requests, concurrently, because the `in` operator of the resources
// endpoint is not reliable across Superset versions (3.1 ignores it and returns every row).
// References that do not exist are reported together in one error.
func (c *Client) ResolvePermissionIDs(refs []PermissionRef) (map[PermissionRef]int64, error) {
	viewMenus := make([]string, 0, len(refs))
	seen := make(map[string]bool, len(refs))
	for _, ref := range refs {
		if !seen[ref.ViewMenu] {
			seen[ref.ViewMenu] = true
			viewMenus = append(viewMenus, ref.ViewMenu)
		}
	}

	var (
		wg       sync.WaitGroup
		slots    = make(chan struct{}, lookupConcurrency)
		mu       sync.Mutex
		firstErr error
		resolved = make(map[string]map[string]int64, len(viewMenus))
	)
	for _, viewMenu := range viewMenus {
		wg.Add(1)
		go func(viewMenu string) {
			defer wg.Done()
			slots <- struct{}{}
			defer func() { <-slots }()

			permissions, err := c.viewMenuPermissions(viewMenu)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				resolved[viewMenu] = permissions
			case errors.Is(err, ErrNotFound):
				resolved[viewMenu] = nil
			case firstErr == nil:
				firstErr = err
			}
		}(viewMenu)
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}

	ids := make(map[PermissionRef]int64, len(refs))
	var missing []string
	for _, ref := range refs {
		if id, ok := resolved[ref.ViewMenu][ref.Permission]; ok {
			ids[ref] = id
		} else {
			missing = append(missing, ref.String())
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("permissions %w: %s", ErrNotFound, strings.Join(missing, ", "))
	}
	return ids, nil
}

// relatedToMany matches rows whose many-to-many relation column contains the row with the
// given id. A value that matches no row is ignored by Flask-AppBuilder, which then returns
// every row, so callers compare the relation again.
func relatedToMany(column string, id int64) listFilter {
	return listFilter{column: column, operator: "rel_m_m", value: strconv.FormatInt(id, 10)}
}

// equalsBool matches rows whose boolean column equals value.
func equalsBool(column string, value bool) listFilter {
	if value {
		return listFilter{column: column, operator: "eq", value: "!t"}
	}
	return listFilter{column: column, operator: "eq", value: "!f"}
}

// taggedWith matches rows that carry a tag whose name contains the given text, through the
// tag filter of the endpoint (chart_tags or dashboard_tags). The match is a case-insensitive
// substring match on the server, so callers compare the tag names again.
func taggedWith(operator, name string) listFilter {
	return listFilter{column: "tags", operator: operator, value: risonString(name)}
}

// idItem is a related row of which only the id matters.
type idItem struct {
	ID int64 `json:"id"`
}

// containsID reports whether one of the items has the given id.
func containsID(items []idItem, id int64) bool {
	for _, item := range items {
		if item.ID == id {
			return true
		}
	}
	return false
}

// hasTag reports whether one of the tags has exactly the given name.
func hasTag(tags []namedItem, name string) bool {
	for _, tag := range tags {
		if tag.Name == name {
			return true
		}
	}
	return false
}
