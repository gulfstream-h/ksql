package ksql

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_OrderByExpression(t *testing.T) {
	testcases := []struct {
		name        string
		expressions []OrderedExpression
		wantExpr    string
		expectErr   bool
	}{
		{
			name: "Single ascending order",
			expressions: []OrderedExpression{
				F("column1").Asc(),
			},
			wantExpr:  "ORDER BY column1 ASC",
			expectErr: false,
		},
		{
			name: "Single descending order",
			expressions: []OrderedExpression{
				newOrderedExpression(F("column1"), Descending),
			},
			wantExpr:  "ORDER BY column1 DESC",
			expectErr: false,
		},
		{
			name: "Multiple orders",
			expressions: []OrderedExpression{
				newOrderedExpression(F("column1"), Ascending),
				newOrderedExpression(F("column2"), Descending),
			},
			wantExpr:  "ORDER BY column1 ASC, column2 DESC",
			expectErr: false,
		},
	}

	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			orderBy := &orderby{expressions: tt.expressions}
			expr, err := orderBy.Expression()

			assert.Equal(t, tt.expectErr, err != nil)

			if !tt.expectErr {
				assert.Equal(t, tt.wantExpr, expr)
			}
		})
	}
}
