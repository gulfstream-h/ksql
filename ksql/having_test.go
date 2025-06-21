package ksql

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_HavingExpression(t *testing.T) {
	tests := []struct {
		name         string
		conditionals []Expression
		wantExpr     string
		expectErr    bool
	}{
		{
			name:         "Single Expression",
			conditionals: []Expression{NewBooleanExp(F("aggregated"), 5, more)},
			wantExpr:     "HAVING aggregated > 5",
			expectErr:    false,
		},
		{
			name: "Two Expressions",
			conditionals: []Expression{
				NewBooleanExp(F("aggregated1"), 6, less),
				NewBooleanExp(F("aggregated2"), 7, equal),
			},
			expectErr: false,
			wantExpr:  "HAVING aggregated1 < 6 AND aggregated2 = 7",
		},
		{
			name:         "No Expressions",
			conditionals: []Expression{},
			wantExpr:     "",
			expectErr:    true,
		},
		{
			name: "Invalid Expression",
			conditionals: []Expression{
				NewBooleanExp(F("aggregated2"), struct{}{}, less),
			},
			wantExpr:  "",
			expectErr: true,
		},
		{
			name: "Mixed Valid and Invalid Expressions",
			conditionals: []Expression{
				NewBooleanExp(F("aggregated1"), 10, more),
				NewBooleanExp(F("aggregated2"), struct{}{}, less),
			},
			wantExpr:  "",
			expectErr: true,
		},
		{
			name: "Three Valid Expressions",
			conditionals: []Expression{
				NewBooleanExp(F("aggregated1"), 1, more),
				NewBooleanExp(F("aggregated2"), 2, less),
				NewBooleanExp(F("aggregated3"), 3, equal),
			},
			wantExpr:  "HAVING aggregated1 > 1 AND aggregated2 < 2 AND aggregated3 = 3",
			expectErr: false,
		},
		{
			name: "Empty Field Name",
			conditionals: []Expression{
				NewBooleanExp(F(""), 5, more),
			},
			wantExpr:  "",
			expectErr: true,
		},
		{
			name: "Duplicate Expressions",
			conditionals: []Expression{
				NewBooleanExp(F("aggregated"), 5, more),
				NewBooleanExp(F("aggregated"), 5, more),
			},
			wantExpr:  "HAVING aggregated > 5 AND aggregated > 5",
			expectErr: false,
		},
		{
			name: "Expression With Negative Value",
			conditionals: []Expression{
				NewBooleanExp(F("aggregated"), -10, less),
			},
			wantExpr:  "HAVING aggregated < -10",
			expectErr: false,
		},
		{
			name: "Expression With Zero Value",
			conditionals: []Expression{
				NewBooleanExp(F("aggregated"), 0, equal),
			},
			wantExpr:  "HAVING aggregated = 0",
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expression, err := NewHavingExpression().
				Having(tt.conditionals...).
				Expression()

			assert.Equal(t, tt.expectErr, err != nil)
			if !tt.expectErr {
				assert.Equal(t, tt.wantExpr, expression)
			}
		})
	}
}
