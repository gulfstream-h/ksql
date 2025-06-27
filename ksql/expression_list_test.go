package ksql

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_ExpressionList(t *testing.T) {
	tests := []struct {
		name      string
		exprs     []Conditional
		typ       BooleanOperationType
		wantExpr  string
		expectErr bool
	}{
		{
			name:      "Single Expression",
			exprs:     []Conditional{F("col1").Equal(1)},
			typ:       AndType,
			wantExpr:  "( col1 = 1 )",
			expectErr: false,
		},
		{
			name:      "Multiple Expressions with AND",
			exprs:     []Conditional{F("col1").Equal(1), F("col2").NotEqual(2)},
			typ:       AndType,
			wantExpr:  "( col1 = 1 AND col2 != 2 )",
			expectErr: false,
		},
		{
			name:      "Multiple Expressions with OR",
			exprs:     []Conditional{F("col1").IsNull(), F("col2").IsNotNull()},
			typ:       OrType,
			wantExpr:  "( col1 IS NULL OR col2 IS NOT NULL )",
			expectErr: false,
		},
		{
			name:      "Empty Expression List",
			exprs:     []Conditional{},
			typ:       AndType,
			wantExpr:  "",
			expectErr: true,
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

			expr, err := exprList.Expression()
			assert.Equal(t, tt.expectErr, err != nil)
			if !tt.expectErr {
				assert.Equal(t, tt.wantExpr, expr)
			}
		})
	}
}
