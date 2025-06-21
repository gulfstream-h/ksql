package ksql

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_ListExpression(t *testing.T) {
	testcases := []struct {
		name      string
		reference Reference
		wantExpr  string
		expectErr bool
	}{
		{
			name:      "List Streams",
			reference: STREAM,
			wantExpr:  "LIST STREAMS;",
			expectErr: false,
		},
		{
			name:      "List Tables",
			reference: TABLE,
			wantExpr:  "LIST TABLES;",
			expectErr: false,
		},
		{
			name:      "List Topics",
			reference: TOPIC,
			wantExpr:  "LIST TOPICS;",
			expectErr: false,
		},
		{
			name:      "Invalid Reference",
			reference: Reference(999), // Assuming 999 is an invalid reference
			wantExpr:  "",
			expectErr: true,
		},
	}

	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			listBuilder := List(tt.reference)
			expr, err := listBuilder.Expression()

			assert.Equal(t, tt.expectErr, err != nil)
			if !tt.expectErr {
				assert.Equal(t, tt.wantExpr, expr)
			}
		})
	}
}
