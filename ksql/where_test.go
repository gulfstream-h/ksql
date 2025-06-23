package ksql

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_Where(t *testing.T) {
	tests := []struct {
		name         string
		conditionals []Conditional
		wantExpr     string
		expectErr    bool
	}{
		{
			name:         "Single Expression",
			conditionals: []Conditional{NewBooleanExp(F("col1"), 5, more)},
			wantExpr:     "WHERE col1 > 5",
			expectErr:    false,
		},
		{
			name: "Two Expressions",
			conditionals: []Conditional{
				NewBooleanExp(F("col1"), 6, less),
				NewBooleanExp(F("col2"), 7, equal),
			},
			expectErr: false,
			wantExpr:  "WHERE col1 < 6 AND col2 = 7",
		},
		{
			name:         "No Expressions",
			conditionals: []Conditional{},
			wantExpr:     "",
			expectErr:    true,
		},
		{
			name: "Invalid Expression",
			conditionals: []Conditional{
				NewBooleanExp(F("col1"), struct{}{}, less),
			},
			wantExpr:  "",
			expectErr: true,
		},
		{
			name: "OR Condition",
			conditionals: []Conditional{
				Or(
					NewBooleanExp(F("col1"), 10, more),
					NewBooleanExp(F("col2"), 20, less),
				),
			},
			wantExpr:  "WHERE ( col1 > 10 OR col2 < 20 )",
			expectErr: false,
		},
		{
			name: "AND Condition",
			conditionals: []Conditional{
				And(
					NewBooleanExp(F("col1"), 15, equal),
					NewBooleanExp(F("col2"), 25, notEqual),
				),
			},
			wantExpr:  "WHERE ( col1 = 15 AND col2 != 25 )",
			expectErr: false,
		},
		{
			name: "Complex AND/OR Condition",
			conditionals: []Conditional{
				And(
					NewBooleanExp(F("col1"), 5, more),
					Or(
						NewBooleanExp(F("col2"), 10, less),
						NewBooleanExp(F("col3"), 20, equal),
					),
				),
			},
			wantExpr:  "WHERE ( col1 > 5 AND ( col2 < 10 OR col3 = 20 ) )",
			expectErr: false,
		},
		{
			name: "Nested OR",
			conditionals: []Conditional{
				Or(
					NewBooleanExp(F("col1"), 30, less),
					Or(
						NewBooleanExp(F("col2"), 40, more),
						NewBooleanExp(F("col3"), 50, equal),
					),
				),
			},
			wantExpr:  "WHERE ( col1 < 30 OR ( col2 > 40 OR col3 = 50 ) )",
			expectErr: false,
		},
		{
			name: "Multiple AND Conditions",
			conditionals: []Conditional{
				And(
					NewBooleanExp(F("col1"), 1, more),
					NewBooleanExp(F("col2"), 2, more),
					NewBooleanExp(F("col3"), 3, more),
				),
			},
			wantExpr:  "WHERE ( col1 > 1 AND col2 > 2 AND col3 > 3 )",
			expectErr: false,
		},
		{
			name: "Empty OR Condition",
			conditionals: []Conditional{
				Or(),
			},
			wantExpr:  "",
			expectErr: true,
		},
		{
			name: "Empty AND Condition",
			conditionals: []Conditional{
				And(),
			},
			wantExpr:  "",
			expectErr: true,
		},
		{
			name: "Single OR Condition",
			conditionals: []Conditional{
				Or(NewBooleanExp(F("col1"), 100, less)),
			},
			wantExpr:  "WHERE ( col1 < 100 )",
			expectErr: false,
		},
		{
			name: "Single AND Condition",
			conditionals: []Conditional{
				And(NewBooleanExp(F("col1"), 200, equal)),
			},
			wantExpr:  "WHERE ( col1 = 200 )",
			expectErr: false,
		},
		{
			name: "Invalid Nested Condition",
			conditionals: []Conditional{
				And(
					NewBooleanExp(F("col1"), struct{}{}, less),
					Or(NewBooleanExp(F("col2"), 300, more)),
				),
			},
			wantExpr:  "",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewWhereExpression()
			w.Where(tt.conditionals...)

			expr, err := w.Expression()
			assert.Equal(t, tt.expectErr, err != nil)
			if !tt.expectErr {
				assert.Equal(t, tt.wantExpr, expr)
			}
		})
	}
}
