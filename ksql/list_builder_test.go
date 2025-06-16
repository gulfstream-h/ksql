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
		expectOK  bool
	}{
		{
			name:      "List Streams",
			reference: STREAM,
			wantExpr:  "LIST STREAMS;",
			expectOK:  true,
		},
		{
			name:      "List Tables",
			reference: TABLE,
			wantExpr:  "LIST TABLES;",
			expectOK:  true,
		},
		{
			name:      "List Topics",
			reference: TOPIC,
			wantExpr:  "LIST TOPICS;",
			expectOK:  true,
		},
		{
			name:      "Invalid Reference",
			reference: Reference(999), // Assuming 999 is an invalid reference
			wantExpr:  "",
			expectOK:  false,
		},
	}

	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			listBuilder := List(tt.reference)
			expr, ok := listBuilder.Expression()

			assert.Equal(t, tt.expectOK, ok)
			if ok {
				assert.Equal(t, tt.wantExpr, expr)
			}
		})
	}
}
