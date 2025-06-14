package ksql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestField(t *testing.T) {
	tests := []struct {
		name     string
		fieldStr string
		wantExpr string
		expectOK bool
	}{
		{
			name:     "Field with schema and column",
			fieldStr: "schema.col",
			wantExpr: "schema.col",
			expectOK: true,
		},
		{
			name:     "Field with only column",
			fieldStr: "col",
			wantExpr: "col",
			expectOK: true,
		},
		{
			name:     "Empty field string",
			fieldStr: "",
			wantExpr: "",
			expectOK: false,
		},
		{
			name:     "Field with column only invalid",
			fieldStr: ".col",
			wantExpr: "",
			expectOK: false,
		},
		{
			name:     "Field with schema only invalid",
			fieldStr: "schema.",
			wantExpr: "",
			expectOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := F(tt.fieldStr)
			got, ok := f.Expression()
			assert.Equal(t, tt.expectOK, ok)
			if ok {
				assert.Equal(t, tt.wantExpr, got)
			}
		})
	}
}
