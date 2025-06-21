package ksql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFromExpression(t *testing.T) {
	tests := []struct {
		name      string
		schema    string
		wantExpr  string
		expectErr bool
	}{
		{
			name:      "Valid schema",
			schema:    "schema_name",
			wantExpr:  "FROM schema_name",
			expectErr: false,
		},
		{
			name:      "Empty schema",
			schema:    "",
			wantExpr:  "",
			expectErr: true,
		},
		{
			name:      "Schema with special characters",
			schema:    "schema@123",
			wantExpr:  "FROM schema@123",
			expectErr: false,
		},
		{
			name:      "Schema with spaces",
			schema:    "schema name",
			wantExpr:  "FROM schema name",
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			from := NewFromExpression().From(tt.schema)
			got, err := from.Expression()
			assert.Equal(t, tt.expectErr, err != nil)
			if !tt.expectErr {
				assert.Equal(t, tt.wantExpr, got)
			}
		})
	}
}
