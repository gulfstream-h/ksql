package ksql

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_DropExpression(t *testing.T) {
	testcases := []struct {
		name      string
		reference Reference
		schema    string
		wantExpr  string
		expectErr bool
	}{
		{
			name:      "Drop Stream",
			reference: STREAM,
			schema:    "my_stream",
			wantExpr:  "DROP STREAM my_stream;",
			expectErr: false,
		},
		{
			name:      "Drop Table",
			reference: TABLE,
			schema:    "my_table",
			wantExpr:  "DROP TABLE my_table;",
			expectErr: false,
		},
		{
			name:      "Drop Topic",
			reference: TOPIC,
			schema:    "my_topic",
			wantExpr:  "DROP TOPIC my_topic;",
			expectErr: false,
		},
		{
			name:      "Invalid Reference",
			reference: Reference(999), // Assuming 999 is an invalid reference
			schema:    "invalid_schema",
			wantExpr:  "",
			expectErr: true,
		},
	}

	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			dropBuilder := Drop(tt.reference, tt.schema)
			expr, err := dropBuilder.Expression()

			assert.Equal(t, tt.expectErr, err != nil)
			if !tt.expectErr {
				assert.Equal(t, tt.wantExpr, expr)
			}
		})
	}
}
