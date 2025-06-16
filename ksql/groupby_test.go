package ksql

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGroupByExpression(t *testing.T) {
	tests := []struct {
		name     string
		fields   []Field
		wantExpr string
		expectOK bool
	}{
		{
			name:     "Single field",
			fields:   []Field{F("schema.col")},
			wantExpr: "GROUP BY schema.col",
			expectOK: true,
		},
		{
			name:     "Multiple fields",
			fields:   []Field{F("schema.col1"), F("schema.col2")},
			wantExpr: "GROUP BY schema.col1, schema.col2",
			expectOK: true,
		},
		{
			name:     "Empty fields",
			fields:   []Field{},
			wantExpr: "",
			expectOK: false,
		},
		{
			name:     "Field with only column",
			fields:   []Field{F("col")},
			wantExpr: "GROUP BY col",
			expectOK: true,
		},
		{
			name:     "Invalid field",
			fields:   []Field{F("")},
			wantExpr: "",
			expectOK: false,
		},
		{
			name:     "Single field",
			fields:   []Field{F("schema.col")},
			wantExpr: "GROUP BY schema.col",
			expectOK: true,
		},
		{
			name:     "Multiple fields",
			fields:   []Field{F("schema.col1"), F("schema.col2")},
			wantExpr: "GROUP BY schema.col1, schema.col2",
			expectOK: true,
		},
		{
			name:     "Empty fields",
			fields:   []Field{},
			wantExpr: "",
			expectOK: false,
		},
		{
			name:     "Field with only column",
			fields:   []Field{F("col")},
			wantExpr: "GROUP BY col",
			expectOK: true,
		},
		{
			name:     "Invalid field",
			fields:   []Field{F("")},
			wantExpr: "",
			expectOK: false,
		},
		{
			name:     "Field with special characters",
			fields:   []Field{F("schema.col@!")},
			wantExpr: "GROUP BY schema.col@!",
			expectOK: true,
		},
		{
			name:     "Field with spaces",
			fields:   []Field{F("schema.col name")},
			wantExpr: "GROUP BY schema.col name",
			expectOK: true,
		},
		{
			name:     "Multiple fields with mixed validity",
			fields:   []Field{F("schema.col1"), F(""), F("schema.col2")},
			wantExpr: "",
			expectOK: false,
		},
		{
			name:     "Duplicate fields",
			fields:   []Field{F("schema.col"), F("schema.col")},
			wantExpr: "GROUP BY schema.col, schema.col",
			expectOK: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			group := NewGroupByExpression().GroupBy(tt.fields...)
			got, ok := group.Expression()
			assert.Equal(t, tt.expectOK, ok)
			if ok {
				assert.Equal(t, tt.wantExpr, got)
			}
		})
	}
}
