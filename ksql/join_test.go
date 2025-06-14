package ksql

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_Join(t *testing.T) {
	tests := []struct {
		name     string
		schema   string
		on       Expression
		joinType JoinType
		wantExpr string
		expectOK bool
	}{
		{
			name:     "Left Join",
			schema:   "schema1",
			on:       NewBooleanExp(F("table1.col1"), F("table2.col2"), equal),
			joinType: Left,
			wantExpr: "LEFT JOIN schema1 ON table1.col1 = table2.col2",
			expectOK: true,
		},
		{
			name:     "Right Join",
			schema:   "schema2",
			on:       NewBooleanExp(F("table1.col1"), F("table2.col2"), equal),
			joinType: Right,
			wantExpr: "RIGHT JOIN schema2 ON table1.col1 = table2.col2",
			expectOK: true,
		},
		{
			name:     "Inner Join",
			schema:   "schema3",
			on:       NewBooleanExp(F("table1.col1"), F("table2.col2"), equal),
			joinType: Inner,
			wantExpr: "JOIN schema3 ON table1.col1 = table2.col2",
			expectOK: true,
		},
		{
			name:     "Outer Join",
			schema:   "schema4",
			on:       NewBooleanExp(F("table1.col1"), F("table2.col2"), equal),
			joinType: Outer,
			wantExpr: "OUTER JOIN schema4 ON table1.col1 = table2.col2",
			expectOK: true,
		},
		{
			name:     "Empty Schema",
			schema:   "",
			on:       NewBooleanExp(F("table1.col1"), F("table2.col2"), equal),
			joinType: Left,
			wantExpr: "",
			expectOK: false,
		},
		{
			name:     "Nil Expression",
			schema:   "schema6",
			on:       nil,
			joinType: Inner,
			wantExpr: "",
			expectOK: false,
		},
		{
			name:     "Invalid Join Type",
			schema:   "schema7",
			on:       NewBooleanExp(F("table1.col1"), F("table2.col2"), equal),
			joinType: JoinType(999),
			wantExpr: "",
			expectOK: false,
		},
		{
			name:   "Complex Expression",
			schema: "schema8",
			on: And(
				NewBooleanExp(F("table1.col1"), F("table2.col2"), equal),
				NewBooleanExp(F("table3.col3"), F("table3.col3"), equal),
			),
			joinType: Inner,
			wantExpr: "JOIN schema8 ON ( table1.col1 = table2.col2 AND table3.col3 = table3.col3 )",
			expectOK: true,
		},
		{
			name:     "No Operation",
			schema:   "schema9",
			on:       NewBooleanExp(F("table1.col1"), F("table2.col2"), equal),
			joinType: JoinType(-1),
			wantExpr: "",
			expectOK: false,
		},
		{
			name:   "OR Expression",
			schema: "schema10",
			on: Or(
				NewBooleanExp(F("table1.col1"), F("table2.col2"), equal),
				NewBooleanExp(F("table3.col3"), F("table4.col4"), equal),
			),
			joinType: Inner,
			wantExpr: "JOIN schema10 ON ( table1.col1 = table2.col2 OR table3.col3 = table4.col4 )",
			expectOK: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			joinExpr := Join(tt.schema, tt.on, tt.joinType)
			expr, ok := joinExpr.Expression()
			assert.Equal(t, tt.expectOK, ok)
			if ok {
				assert.Equal(t, tt.wantExpr, expr)
			}
		})
	}
}
