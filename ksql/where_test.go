package ksql

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_Where(t *testing.T) {
	tests := []struct {
		name         string
		conditionals []Expression
		wantExpr     string
		expectOK     bool
	}{
		{
			name:         "Single Expression",
			conditionals: []Expression{NewBooleanExp(F("col1"), 5, more)},
			wantExpr:     "WHERE col1 > 5",
			expectOK:     true,
		},
		{
			name: "Two Expressions",
			conditionals: []Expression{
				NewBooleanExp(F("col1"), 6, less),
				NewBooleanExp(F("col2"), 7, equal),
			},
			expectOK: true,
			wantExpr: "WHERE col1 < 6 AND col2 = 7",
		},
		{
			name:         "No Expressions",
			conditionals: []Expression{},
			wantExpr:     "",
			expectOK:     false,
		},
		{
			name: "Invalid Expression",
			conditionals: []Expression{
				NewBooleanExp(F("col1"), struct{}{}, less),
			},
			wantExpr: "",
			expectOK: false,
		},
		{
			name: "OR Condition",
			conditionals: []Expression{
				Or(
					NewBooleanExp(F("col1"), 10, more),
					NewBooleanExp(F("col2"), 20, less),
				),
			},
			wantExpr: "WHERE ( col1 > 10 OR col2 < 20 )",
			expectOK: true,
		},
		{
			name: "AND Condition",
			conditionals: []Expression{
				And(
					NewBooleanExp(F("col1"), 15, equal),
					NewBooleanExp(F("col2"), 25, notEqual),
				),
			},
			wantExpr: "WHERE ( col1 = 15 AND col2 != 25 )",
			expectOK: true,
		},
		{
			name: "Complex AND/OR Condition",
			conditionals: []Expression{
				And(
					NewBooleanExp(F("col1"), 5, more),
					Or(
						NewBooleanExp(F("col2"), 10, less),
						NewBooleanExp(F("col3"), 20, equal),
					),
				),
			},
			wantExpr: "WHERE ( col1 > 5 AND ( col2 < 10 OR col3 = 20 ) )",
			expectOK: true,
		},
		{
			name: "Nested OR",
			conditionals: []Expression{
				Or(
					NewBooleanExp(F("col1"), 30, less),
					Or(
						NewBooleanExp(F("col2"), 40, more),
						NewBooleanExp(F("col3"), 50, equal),
					),
				),
			},
			wantExpr: "WHERE ( col1 < 30 OR ( col2 > 40 OR col3 = 50 ) )",
			expectOK: true,
		},
		{
			name: "Multiple AND Conditions",
			conditionals: []Expression{
				And(
					NewBooleanExp(F("col1"), 1, more),
					NewBooleanExp(F("col2"), 2, more),
					NewBooleanExp(F("col3"), 3, more),
				),
			},
			wantExpr: "WHERE ( col1 > 1 AND col2 > 2 AND col3 > 3 )",
			expectOK: true,
		},
		{
			name: "Empty OR Condition",
			conditionals: []Expression{
				Or(),
			},
			wantExpr: "",
			expectOK: false,
		},
		{
			name: "Empty AND Condition",
			conditionals: []Expression{
				And(),
			},
			wantExpr: "",
			expectOK: false,
		},
		{
			name: "Single OR Condition",
			conditionals: []Expression{
				Or(NewBooleanExp(F("col1"), 100, less)),
			},
			wantExpr: "WHERE ( col1 < 100 )",
			expectOK: true,
		},
		{
			name: "Single AND Condition",
			conditionals: []Expression{
				And(NewBooleanExp(F("col1"), 200, equal)),
			},
			wantExpr: "WHERE ( col1 = 200 )",
			expectOK: true,
		},
		{
			name: "Invalid Nested Condition",
			conditionals: []Expression{
				And(
					NewBooleanExp(F("col1"), struct{}{}, less),
					Or(NewBooleanExp(F("col2"), 300, more)),
				),
			},
			wantExpr: "",
			expectOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewWhereExpression()
			w.Where(tt.conditionals...)

			expr, ok := w.Expression()
			assert.Equal(t, tt.expectOK, ok)
			if ok {
				assert.Equal(t, tt.wantExpr, expr)
			}
		})
	}
}
