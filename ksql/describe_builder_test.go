package ksql

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDescribeExpression(t *testing.T) {
	testcases := []struct {
		name      string
		reference Reference
		schema    string
		wantExpr  string
		expectOK  bool
	}{
		{
			name:      "Describe Stream",
			reference: STREAM,
			schema:    "my_stream",
			wantExpr:  "DESCRIBE STREAM my_stream;",
			expectOK:  true,
		},
		{
			name:      "Describe Table",
			reference: TABLE,
			schema:    "my_table",
			wantExpr:  "DESCRIBE TABLE my_table;",
			expectOK:  true,
		},
		{
			name:      "Describe Topic",
			reference: TOPIC,
			schema:    "my_topic",
			wantExpr:  "DESCRIBE TOPIC my_topic;",
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
			describeBuilder := Describe(tt.reference, tt.schema)
			expr, ok := describeBuilder.Expression()

			assert.Equal(t, tt.expectOK, ok)
			if ok {
				assert.Equal(t, tt.wantExpr, expr)
			}
		})
	}

}
