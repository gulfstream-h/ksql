package ksql

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_Join(t *testing.T) {
	tests := []struct {
		name      string
		schema    FromExpression
		on        Conditional
		joinType  JoinType
		wantExpr  string
		expectErr bool
	}{
		{
			name:      "Left Join",
			schema:    Schema("schema1", STREAM),
			on:        NewBooleanExp(F("table1.col1"), F("table2.col2"), equal),
			joinType:  Left,
			wantExpr:  "LEFT JOIN schema1 ON table1.col1 = table2.col2",
			expectErr: false,
		},
		{
			name:      "Right Join",
			schema:    Schema("schema2", STREAM),
			on:        NewBooleanExp(F("table1.col1"), F("table2.col2"), equal),
			joinType:  Right,
			wantExpr:  "RIGHT JOIN schema2 ON table1.col1 = table2.col2",
			expectErr: false,
		},
		{
			name:      "Inner Join",
			schema:    Schema("schema3", STREAM),
			on:        NewBooleanExp(F("table1.col1"), F("table2.col2"), equal),
			joinType:  Inner,
			wantExpr:  "JOIN schema3 ON table1.col1 = table2.col2",
			expectErr: false,
		},
		{
			name:      "Outer Join",
			schema:    Schema("schema4", STREAM),
			on:        NewBooleanExp(F("table1.col1"), F("table2.col2"), equal),
			joinType:  Outer,
			wantExpr:  "OUTER JOIN schema4 ON table1.col1 = table2.col2",
			expectErr: false,
		},
		{
			name:      "Empty Schema",
			schema:    nil,
			on:        NewBooleanExp(F("table1.col1"), F("table2.col2"), equal),
			joinType:  Left,
			wantExpr:  "",
			expectErr: true,
		},
		{
			name:      "Nil Expression",
			schema:    nil,
			on:        nil,
			joinType:  Inner,
			wantExpr:  "",
			expectErr: true,
		},
		{
			name:      "Invalid Join Type",
			schema:    Schema("schema7", STREAM),
			on:        NewBooleanExp(F("table1.col1"), F("table2.col2"), equal),
			joinType:  JoinType(999),
			wantExpr:  "",
			expectErr: true,
		},
		{
			name:   "Complex Expression",
			schema: Schema("schema8", TABLE),
			on: And(
				NewBooleanExp(F("table1.col1"), F("table2.col2"), equal),
				NewBooleanExp(F("table3.col3"), F("table3.col3"), equal),
			),
			joinType:  Inner,
			wantExpr:  "JOIN schema8 ON ( table1.col1 = table2.col2 AND table3.col3 = table3.col3 )",
			expectErr: false,
		},
		{
			name:      "No Operation",
			schema:    Schema("schema9", TABLE),
			on:        NewBooleanExp(F("table1.col1"), F("table2.col2"), equal),
			joinType:  JoinType(-1),
			wantExpr:  "",
			expectErr: true,
		},
		{
			name:   "OR Expression",
			schema: Schema("schema10", TABLE),
			on: Or(
				NewBooleanExp(F("table1.col1"), F("table2.col2"), equal),
				NewBooleanExp(F("table3.col3"), F("table4.col4"), equal),
			),
			joinType:  Inner,
			wantExpr:  "JOIN schema10 ON ( table1.col1 = table2.col2 OR table3.col3 = table4.col4 )",
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			joinExpr := Join(tt.schema, tt.on, tt.joinType)
			expr, err := joinExpr.Expression()
			assert.Equal(t, tt.expectErr, err != nil)
			if !tt.expectErr {
				assert.Equal(t, tt.wantExpr, expr)
			}
		})
	}
}
