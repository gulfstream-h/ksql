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
		expectOK  bool
	}{
		{
			name:      "Drop Stream",
			reference: STREAM,
			schema:    "my_stream",
			wantExpr:  "DROP STREAM my_stream;",
			expectOK:  true,
		},
		{
			name:      "Drop Table",
			reference: TABLE,
			schema:    "my_table",
			wantExpr:  "DROP TABLE my_table;",
			expectOK:  true,
		},
		{
			name:      "Drop Topic",
			reference: TOPIC,
			schema:    "my_topic",
			wantExpr:  "DROP TOPIC my_topic;",
			expectOK:  true,
		},
		{
			name:      "Invalid Reference",
			reference: Reference(999), // Assuming 999 is an invalid reference
			schema:    "invalid_schema",
			wantExpr:  "",
			expectOK:  false,
		},
	}

	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			dropBuilder := Drop(tt.reference, tt.schema)
			expr, ok := dropBuilder.Expression()

			assert.Equal(t, tt.expectOK, ok)
			if ok {
				assert.Equal(t, tt.wantExpr, expr)
			}
		})
	}
}
