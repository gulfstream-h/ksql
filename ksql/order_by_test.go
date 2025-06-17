package ksql

import "testing"

func Test_OrderByExpression(t *testing.T) {
	testcases := []struct {
		name        string
		expressions []OrderedExpression
		wantExpr    string
		expectOK    bool
	}{
		{
			name: "Single ascending order",
			expressions: []OrderedExpression{
				F("column1").Asc(),
			},
			wantExpr: "ORDER BY column1 ASC",
			expectOK: true,
		},
		{
			name: "Single descending order",
			expressions: []OrderedExpression{
				newOrderedExpression(F("column1"), Descending),
			},
			wantExpr: "ORDER BY column1 DESC",
			expectOK: true,
		},
		{
			name: "Multiple orders",
			expressions: []OrderedExpression{
				newOrderedExpression(F("column1"), Ascending),
				newOrderedExpression(F("column2"), Descending),
			},
			wantExpr: "ORDER BY column1 ASC, column2 DESC",
			expectOK: true,
		},
	}

	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			orderBy := &orderby{expressions: tt.expressions}
			expr, ok := orderBy.Expression()

			if ok != tt.expectOK {
				t.Errorf("expected ok=%v, got %v", tt.expectOK, ok)
				return
			}

			if expr != tt.wantExpr {
				t.Errorf("expected expression %q, got %q", tt.wantExpr, expr)
			}
		})
	}
}
