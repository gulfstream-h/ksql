package ksql

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_ExpressionList(t *testing.T) {
	tests := []struct {
		name     string
		exprs    []Expression
		typ      BooleanOperationType
		wantExpr string
		expectOK bool
	}{
		{
			name:     "Single Expression",
			exprs:    []Expression{F("col1")},
			typ:      AndType,
			wantExpr: "( col1 )",
			expectOK: true,
		},
		{
			name:     "Multiple Expressions with AND",
			exprs:    []Expression{F("col1"), F("col2")},
			typ:      AndType,
			wantExpr: "( col1 AND col2 )",
			expectOK: true,
		},
		{
			name:     "Multiple Expressions with OR",
			exprs:    []Expression{F("col1"), F("col2")},
			typ:      OrType,
			wantExpr: "( col1 OR col2 )",
			expectOK: true,
		},
		{
			name:     "Empty Expression List",
			exprs:    []Expression{},
			typ:      AndType,
			wantExpr: "",
			expectOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var exprList ExpressionList
			if tt.typ == AndType {
				exprList = And(tt.exprs...)
			} else if tt.typ == OrType {
				exprList = Or(tt.exprs...)
			}

			expr, ok := exprList.Expression()
			assert.Equal(t, tt.expectOK, ok)
			if ok {
				assert.Equal(t, tt.wantExpr, expr)
			}
		})
	}
}
