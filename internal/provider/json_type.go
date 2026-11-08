package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/hashicorp/terraform-plugin-framework/attr"
	"github.com/hashicorp/terraform-plugin-framework/attr/xattr"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/types/basetypes"
	"github.com/hashicorp/terraform-plugin-go/tftypes"
)

// jsonType is the type of string attributes that hold a JSON document. Values are compared as
// documents, so formatting differences never show up as drift. A partial type goes further and
// treats a value as equal to any document that contains it: Superset completes dashboard
// metadata and layouts with values of its own, which must not be reported as drift either.
type jsonType struct {
	basetypes.StringType
	partial bool
}

var (
	jsonDocument        = jsonType{}
	jsonPartialDocument = jsonType{partial: true}
)

func (t jsonType) Equal(other attr.Type) bool {
	o, ok := other.(jsonType)
	return ok && o.partial == t.partial
}

func (t jsonType) String() string {
	if t.partial {
		return "jsonType(partial)"
	}
	return "jsonType"
}

func (t jsonType) ValueFromString(_ context.Context, in basetypes.StringValue) (basetypes.StringValuable, diag.Diagnostics) {
	return jsonValue{StringValue: in, partial: t.partial}, nil
}

func (t jsonType) ValueFromTerraform(ctx context.Context, in tftypes.Value) (attr.Value, error) {
	value, err := t.StringType.ValueFromTerraform(ctx, in)
	if err != nil {
		return nil, err
	}
	str, ok := value.(basetypes.StringValue)
	if !ok {
		return nil, fmt.Errorf("unexpected value type %T", value)
	}
	return jsonValue{StringValue: str, partial: t.partial}, nil
}

func (t jsonType) ValueType(context.Context) attr.Value {
	return jsonValue{partial: t.partial}
}

// value returns the document as a value of the type; an empty document is null.
func (t jsonType) value(document string) jsonValue {
	if document == "" {
		return jsonValue{StringValue: basetypes.NewStringNull(), partial: t.partial}
	}
	return jsonValue{StringValue: basetypes.NewStringValue(document), partial: t.partial}
}

// jsonValue is a value of jsonType.
type jsonValue struct {
	basetypes.StringValue
	partial bool
}

var (
	_ basetypes.StringValuableWithSemanticEquals = jsonValue{}
	_ xattr.ValidateableAttribute                = jsonValue{}
)

func (v jsonValue) Type(context.Context) attr.Type {
	return jsonType{partial: v.partial}
}

func (v jsonValue) Equal(other attr.Value) bool {
	o, ok := other.(jsonValue)
	return ok && o.partial == v.partial && v.StringValue.Equal(o.StringValue)
}

func (v jsonValue) ValidateAttribute(_ context.Context, req xattr.ValidateAttributeRequest, resp *xattr.ValidateAttributeResponse) {
	if v.IsNull() || v.IsUnknown() {
		return
	}
	if !json.Valid([]byte(v.ValueString())) {
		resp.Diagnostics.AddAttributeError(req.Path, "Invalid JSON Document", "The value must be a valid JSON document.")
	}
}

// StringSemanticEquals reports whether v, the value Superset returned, is the same document as
// the prior value or, for a partial type, contains the prior value. The framework calls it on
// the new value with the prior value as the argument.
func (v jsonValue) StringSemanticEquals(_ context.Context, priorValuable basetypes.StringValuable) (bool, diag.Diagnostics) {
	prior, ok := priorValuable.(jsonValue)
	if !ok {
		return false, nil
	}
	if v.IsNull() || v.IsUnknown() || prior.IsNull() || prior.IsUnknown() {
		return v.StringValue.Equal(prior.StringValue), nil
	}

	var priorDocument, newDocument any
	if err := json.Unmarshal([]byte(prior.ValueString()), &priorDocument); err != nil {
		return false, nil
	}
	if err := json.Unmarshal([]byte(v.ValueString()), &newDocument); err != nil {
		return false, nil
	}
	if v.partial {
		return contains(newDocument, priorDocument), nil
	}
	return reflect.DeepEqual(priorDocument, newDocument), nil
}

// contains reports whether the document has every value of part: objects of the document may
// carry extra keys, arrays must match element by element and scalars must be equal.
func contains(document, part any) bool {
	switch part := part.(type) {
	case map[string]any:
		object, ok := document.(map[string]any)
		if !ok {
			return false
		}
		for key, value := range part {
			if !contains(object[key], value) {
				return false
			}
		}
		return true
	case []any:
		array, ok := document.([]any)
		if !ok || len(array) != len(part) {
			return false
		}
		for i := range part {
			if !contains(array[i], part[i]) {
				return false
			}
		}
		return true
	default:
		return reflect.DeepEqual(document, part)
	}
}
