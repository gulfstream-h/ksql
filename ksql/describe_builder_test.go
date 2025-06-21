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
		expectErr bool
	}{
		{
			name:      "Describe Stream",
			reference: STREAM,
			schema:    "my_stream",
			wantExpr:  "DESCRIBE my_stream;",
			expectErr: false,
		},
		{
			name:      "Describe Table",
			reference: TABLE,
			schema:    "my_table",
			wantExpr:  "DESCRIBE my_table;",
			expectErr: false,
		},
		{
			name:      "Describe Topic",
			reference: TOPIC,
			schema:    "my_topic",
			wantExpr:  "DESCRIBE my_topic;",
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
			describeBuilder := Describe(tt.reference, tt.schema)
			expr, err := describeBuilder.Expression()

			assert.Equal(t, tt.expectErr, err != nil)
			if !tt.expectErr {
				assert.Equal(t, tt.wantExpr, expr)
			}
		})
	}

}
