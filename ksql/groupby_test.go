package ksql

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGroupByExpression(t *testing.T) {
	tests := []struct {
		name      string
		fields    []Field
		wantExpr  string
		expectErr bool
	}{
		{
			name:      "Single field",
			fields:    []Field{F("schema.col")},
			wantExpr:  "GROUP BY schema.col",
			expectErr: false,
		},
		{
			name:      "Multiple fields",
			fields:    []Field{F("schema.col1"), F("schema.col2")},
			wantExpr:  "GROUP BY schema.col1, schema.col2",
			expectErr: false,
		},
		{
			name:      "Empty fields",
			fields:    []Field{},
			wantExpr:  "",
			expectErr: true,
		},
		{
			name:      "Field with only column",
			fields:    []Field{F("col")},
			wantExpr:  "GROUP BY col",
			expectErr: false,
		},
		{
			name:      "Invalid field",
			fields:    []Field{F("")},
			wantExpr:  "",
			expectErr: true,
		},
		{
			name:      "Single field",
			fields:    []Field{F("schema.col")},
			wantExpr:  "GROUP BY schema.col",
			expectErr: false,
		},
		{
			name:      "Multiple fields",
			fields:    []Field{F("schema.col1"), F("schema.col2")},
			wantExpr:  "GROUP BY schema.col1, schema.col2",
			expectErr: false,
		},
		{
			name:      "Empty fields",
			fields:    []Field{},
			wantExpr:  "",
			expectErr: true,
		},
		{
			name:      "Field with only column",
			fields:    []Field{F("col")},
			wantExpr:  "GROUP BY col",
			expectErr: false,
		},
		{
			name:      "Invalid field",
			fields:    []Field{F("")},
			wantExpr:  "",
			expectErr: true,
		},
		{
			name:      "Field with special characters",
			fields:    []Field{F("schema.col@!")},
			wantExpr:  "GROUP BY schema.col@!",
			expectErr: false,
		},
		{
			name:      "Field with spaces",
			fields:    []Field{F("schema.col name")},
			wantExpr:  "GROUP BY schema.col name",
			expectErr: false,
		},
		{
			name:      "Multiple fields with mixed validity",
			fields:    []Field{F("schema.col1"), F(""), F("schema.col2")},
			wantExpr:  "",
			expectErr: true,
		},
		{
			name:      "Duplicate fields",
			fields:    []Field{F("schema.col"), F("schema.col")},
			wantExpr:  "GROUP BY schema.col, schema.col",
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			group := NewGroupByExpression().GroupBy(tt.fields...)
			got, err := group.Expression()
			assert.Equal(t, tt.expectErr, err != nil)
			if !tt.expectErr {
				assert.Equal(t, tt.wantExpr, got)
			}
		})
	}
}
