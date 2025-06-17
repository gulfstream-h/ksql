package ksql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFromExpression(t *testing.T) {
	tests := []struct {
		name     string
		schema   string
		wantExpr string
		expectOK bool
	}{
		{
			name:     "Valid schema",
			schema:   "schema_name",
			wantExpr: "FROM schema_name",
			expectOK: true,
		},
		{
			name:     "Empty schema",
			schema:   "",
			wantExpr: "",
			expectOK: false,
		},
		{
			name:     "Schema with special characters",
			schema:   "schema@123",
			wantExpr: "FROM schema@123",
			expectOK: true,
		},
		{
			name:     "Schema with spaces",
			schema:   "schema name",
			wantExpr: "FROM schema name",
			expectOK: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			from := NewFromExpression().From(tt.schema)
			got, ok := from.Expression()
			assert.Equal(t, tt.expectOK, ok)
			if ok {
				assert.Equal(t, tt.wantExpr, got)
			}
		})
	}
}
