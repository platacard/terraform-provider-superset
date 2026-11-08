package provider

import (
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/attr/xattr"
	"github.com/hashicorp/terraform-plugin-framework/path"
)

func TestJSONValueSemanticEquals(t *testing.T) {
	tests := []struct {
		name  string
		typ   jsonType
		prior string
		new   string
		equal bool
	}{
		{"same formatting", jsonDocument, `{"a":1}`, `{"a":1}`, true},
		{"different formatting and key order", jsonDocument, `{"a": 1, "b": [1, 2]}`, `{"b":[1,2],"a":1}`, true},
		{"different value", jsonDocument, `{"a":1}`, `{"a":2}`, false},
		{"extra key is drift for an exact document", jsonDocument, `{"a":1}`, `{"a":1,"b":2}`, false},
		{"extra key is tolerated for a partial document", jsonPartialDocument, `{"a":1}`, `{"a":1,"b":2}`, true},
		{"extra nested key is tolerated for a partial document", jsonPartialDocument,
			`{"CHART-1":{"meta":{"chartId":7,"width":4}}}`, `{"CHART-1":{"meta":{"chartId":7,"uuid":"x","width":4}},"ROOT_ID":{}}`, true},
		{"changed nested value is drift for a partial document", jsonPartialDocument,
			`{"CHART-1":{"meta":{"chartId":7,"width":4}}}`, `{"CHART-1":{"meta":{"chartId":7,"width":8}}}`, false},
		{"missing key is drift for a partial document", jsonPartialDocument, `{"a":1,"b":2}`, `{"a":1}`, false},
		{"arrays must match exactly for a partial document", jsonPartialDocument, `{"a":[1,2]}`, `{"a":[1,2,3]}`, false},
		{"arrays of objects are compared element by element", jsonPartialDocument, `{"a":[{"x":1}]}`, `{"a":[{"x":1,"y":2}]}`, true},
		{"scalar type matters", jsonDocument, `{"a":1}`, `{"a":"1"}`, false},
		{"invalid prior document is never equal", jsonDocument, `{`, `{}`, false},
		{"invalid new document is never equal", jsonDocument, `{}`, `{`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// The framework calls the method on the new value with the prior value as the argument.
			equal, diags := tt.typ.value(tt.new).StringSemanticEquals(t.Context(), tt.typ.value(tt.prior))
			if diags.HasError() {
				t.Fatalf("unexpected diagnostics: %v", diags)
			}
			if equal != tt.equal {
				t.Fatalf("semantic equality of %q and %q: got %v, want %v", tt.prior, tt.new, equal, tt.equal)
			}
		})
	}
}

func TestJSONValueNullAndUnknown(t *testing.T) {
	ctx := t.Context()
	if equal, _ := jsonDocument.value("").StringSemanticEquals(ctx, jsonDocument.value("")); !equal {
		t.Fatal("two null values must be equal")
	}
	if equal, _ := jsonDocument.value(`{}`).StringSemanticEquals(ctx, jsonDocument.value("")); equal {
		t.Fatal("a document must differ from a null prior value")
	}
	if equal, _ := jsonPartialDocument.value("").StringSemanticEquals(ctx, jsonPartialDocument.value(`{}`)); equal {
		t.Fatal("a null value must differ from a prior document")
	}
}

func TestJSONValueValidate(t *testing.T) {
	ctx := t.Context()
	req := xattr.ValidateAttributeRequest{Path: path.Root("params")}

	var resp xattr.ValidateAttributeResponse
	jsonDocument.value(`{"a": [1, 2]}`).ValidateAttribute(ctx, req, &resp)
	if resp.Diagnostics.HasError() {
		t.Fatalf("a valid document must pass validation: %v", resp.Diagnostics)
	}

	resp = xattr.ValidateAttributeResponse{}
	jsonDocument.value(`{"a": }`).ValidateAttribute(ctx, req, &resp)
	if !resp.Diagnostics.HasError() {
		t.Fatal("an invalid document must fail validation")
	}

	resp = xattr.ValidateAttributeResponse{}
	jsonDocument.value("").ValidateAttribute(ctx, req, &resp)
	if resp.Diagnostics.HasError() {
		t.Fatalf("a null value must pass validation: %v", resp.Diagnostics)
	}
}

func TestJSONTypeEqual(t *testing.T) {
	if jsonDocument.Equal(jsonPartialDocument) {
		t.Fatal("exact and partial types must differ")
	}
	if !jsonPartialDocument.Equal(jsonType{partial: true}) {
		t.Fatal("partial types must be equal")
	}
	if jsonDocument.value(`{}`).Equal(jsonPartialDocument.value(`{}`)) {
		t.Fatal("values of different types must differ")
	}
}
