package provider

import (
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/types"
)

// Superset reports 0 for a dataset whose cache timeout was never set. The state must keep
// null then, or Terraform fails the create with "Provider produced inconsistent result after
// apply" and plans an update forever.
func TestDatasetCacheTimeout(t *testing.T) {
	zero, ten := int64(0), int64(10)
	cases := []struct {
		name  string
		value *int64
		prior types.Int64
		want  types.Int64
	}{
		{"unset and reported as 0", &zero, types.Int64Null(), types.Int64Null()},
		{"unset and reported as null", nil, types.Int64Null(), types.Int64Null()},
		{"unset and changed in Superset", &ten, types.Int64Null(), types.Int64Value(10)},
		{"set to 0", &zero, types.Int64Value(0), types.Int64Value(0)},
		{"set and reported", &ten, types.Int64Value(10), types.Int64Value(10)},
		{"set and removed in Superset", nil, types.Int64Value(10), types.Int64Null()},
	}
	for _, tc := range cases {
		if got := datasetCacheTimeout(tc.value, tc.prior); !got.Equal(tc.want) {
			t.Errorf("%s: got %s, want %s", tc.name, got, tc.want)
		}
	}
}
