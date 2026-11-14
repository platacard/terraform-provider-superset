package client

import (
	"fmt"
	"net/http"
	"sort"
)

// The role members endpoints of Superset 6 replace the users and the groups of a role in one
// request; there is no endpoint that reads them back, so the members are found through the
// user and group lists.

// SetRoleUsers makes exactly the given users members of the role. A role or a user that does
// not exist is reported as ErrNotFound.
func (c *Client) SetRoleUsers(roleID int64, userIDs []int64) error {
	return c.setRoleMembers(roleID, "users", map[string][]int64{"user_ids": nonNil(userIDs)}, "set the users of the role")
}

// SetRoleGroups makes exactly the given groups carry the role. A role or a group that does
// not exist is reported as ErrNotFound.
func (c *Client) SetRoleGroups(roleID int64, groupIDs []int64) error {
	return c.setRoleMembers(roleID, "groups", map[string][]int64{"group_ids": nonNil(groupIDs)}, "set the groups of the role")
}

func (c *Client) setRoleMembers(roleID int64, kind string, payload map[string][]int64, action string) error {
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodPut, fmt.Sprintf("/api/v1/security/roles/%d/%s", roleID, kind), payload, headers, cookies)
	if err != nil {
		return err
	}
	return decodeResponse(resp, nil, action)
}

// RoleUserIDs returns the ids of the users that have the role, in ascending order. The
// server filters the user list on the relation; the roles of each user are compared again
// because the filter is ignored when the role does not exist.
func (c *Client) RoleUserIDs(roleID int64) ([]int64, error) {
	rows, err := listInto[rawUserModel](c, "/api/v1/security/users/", relatedToMany("roles", roleID))
	if err != nil {
		return nil, fmt.Errorf("users of role %d: %w", roleID, err)
	}
	ids := make([]int64, 0, len(rows))
	for _, row := range rows {
		for _, role := range row.Roles {
			if role.ID == roleID {
				ids = append(ids, row.ID)
				break
			}
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

// RoleGroupIDs returns the ids of the groups that carry the role, in ascending order. The
// group list cannot be filtered on the relation, so every group is read.
func (c *Client) RoleGroupIDs(roleID int64) ([]int64, error) {
	type row struct {
		ID int64 `json:"id"`
		groupResult
	}
	rows, err := listInto[row](c, groupsPath)
	if err != nil {
		return nil, fmt.Errorf("groups of role %d: %w", roleID, err)
	}
	ids := make([]int64, 0)
	for _, r := range rows {
		for _, role := range r.Roles {
			if role.ID == roleID {
				ids = append(ids, r.ID)
				break
			}
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}
