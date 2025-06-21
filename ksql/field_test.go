package ksql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestField(t *testing.T) {
	tests := []struct {
		name      string
		fieldStr  string
		wantExpr  string
		expectErr bool
	}{
		{
			name:      "Field with schema and column",
			fieldStr:  "schema.col",
			wantExpr:  "schema.col",
			expectErr: false,
		},
		{
			name:      "Field with only column",
			fieldStr:  "col",
			wantExpr:  "col",
			expectErr: false,
		},
		{
			name:      "Empty field string",
			fieldStr:  "",
			wantExpr:  "",
			expectErr: true,
		},
		{
			name:      "Field with column only invalid",
			fieldStr:  ".col",
			wantExpr:  "",
			expectErr: true,
		},
		{
			name:      "Field with schema only invalid",
			fieldStr:  "schema.",
			wantExpr:  "",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := F(tt.fieldStr)
			got, err := f.Expression()
			assert.Equal(t, tt.expectErr, err != nil)
			if !tt.expectErr {
				assert.Equal(t, tt.wantExpr, got)
			}
		})
	}
}
