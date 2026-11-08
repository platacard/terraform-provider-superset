package provider

import (
	"context"
	"fmt"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
)

// oneOf validates that a string attribute has one of the listed values.
type oneOf []string

func (v oneOf) Description(_ context.Context) string {
	return "value must be one of: " + strings.Join(v, ", ")
}

func (v oneOf) MarkdownDescription(ctx context.Context) string {
	return v.Description(ctx)
}

func (v oneOf) ValidateString(ctx context.Context, req validator.StringRequest, resp *validator.StringResponse) {
	if req.ConfigValue.IsNull() || req.ConfigValue.IsUnknown() {
		return
	}
	for _, allowed := range v {
		if req.ConfigValue.ValueString() == allowed {
			return
		}
	}
	resp.Diagnostics.AddAttributeError(req.Path, "Invalid Attribute Value",
		fmt.Sprintf("%s, got %q.", v.Description(ctx), req.ConfigValue.ValueString()))
}
