package ksql

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_SelectExpression(t *testing.T) {
	testcases := []struct {
		name              string
		fields            []Field
		schemaFrom        string
		whereExpressions  []Expression
		join              []JoinExpression
		havingExpressions []Expression
		groupByFields     []Field
		structScan        any
		wantExpr          string
		expectOK          bool
	}{
		{
			name:       "Simple SELECT with one field",
			fields:     []Field{F("table.column1")},
			schemaFrom: "table",
			wantExpr:   "SELECT table.column1\nFROM table;",
			expectOK:   true,
		},
		{
			name:       "SELECT with alias",
			fields:     []Field{F("table.column1").As("alias1")},
			schemaFrom: "table",
			wantExpr:   "SELECT table.column1 AS alias1\nFROM table;",
			expectOK:   true,
		},
		{
			name:             "SELECT with WHERE clause",
			fields:           []Field{F("table.column1")},
			schemaFrom:       "table",
			whereExpressions: []Expression{F("table.column1").Equal(1)},
			wantExpr:         "SELECT table.column1\nFROM table\nWHERE table.column1 = 1;",
			expectOK:         true,
		},
		{
			name:       "SELECT with JOIN",
			fields:     []Field{F("table1.column1"), F("table2.column2")},
			schemaFrom: "table1",
			join:       []JoinExpression{Join("table2", F("table1.id").Equal(F("table2.id")), Inner)},
			wantExpr:   "SELECT table1.column1, table2.column2\nFROM table1\nJOIN table2 ON table1.id = table2.id;",
			expectOK:   true,
		},
		{
			name:       "SELECT with LEFT JOIN",
			fields:     []Field{F("table1.column1"), F("table2.column2")},
			schemaFrom: "table1",
			join:       []JoinExpression{Join("table2", F("table1.id").Equal(F("table2.id")), Left)},
			wantExpr:   "SELECT table1.column1, table2.column2\nFROM table1\nLEFT JOIN table2 ON table1.id = table2.id;",
			expectOK:   true,
		},
		{
			name:          "SELECT with GROUP BY",
			fields:        []Field{F("table.column1"), F("table.column2")},
			schemaFrom:    "table",
			groupByFields: []Field{F("table.column1")},
			wantExpr:      "SELECT table.column1, table.column2\nFROM table\nGROUP BY table.column1;",
			expectOK:      true,
		},
		// todo : implement Aggregated functions
		//{
		//	name:          "SELECT with HAVING",
		//	fields:        []Field{F("table.column1"), F("table.column2")},
		//	schemaFrom:    "table",
		//	groupByFields: []Field{F("table.column1")},
		//	havingExpressions: F("aggregated")"COUNT(table.column2) > 1")),
		//	wantExpr: "SELECT table.column1, table.column2\nFROM table\nGROUP BY table.column1\nHAVING COUNT(table.column2) > 1",
		//	expectOK: true,
		//},
		{
			name:       "SELECT with multiple JOINs",
			fields:     []Field{F("table1.column1"), F("table2.column2"), F("table3.column3")},
			schemaFrom: "table1",
			join: []JoinExpression{
				Join("table2", F("table1.id").Equal(F("table2.id")), Inner),
				Join("table3", F("table2.id").Equal(F("table3.id")), Left),
			},
			wantExpr: "SELECT table1.column1, table2.column2, table3.column3\nFROM table1\nJOIN table2 ON table1.id = table2.id\nLEFT JOIN table3 ON table2.id = table3.id;",
			expectOK: true,
		},
		// todo: fix struct scan
		{
			name: "SELECT with struct scan",
			structScan: struct {
				ID   int    `ksql:"id"`
				Name string `ksql:"name"`
			}{},
			schemaFrom: "users",
			wantExpr:   "SELECT users.id, users.name\nFROM users",
			expectOK:   true,
		},
		//{
		//	name:     "SELECT with CTE",
		//	fields:   []Field{NewField("main", "column1")},
		//	from:     NewFromExpression().From("main"),
		//	join:     []JoinExpression{},
		//	wantExpr: "WITH cte AS (\nSELECT column1\nFROM table\n)\nSELECT main.column1\nFROM main",
		//	expectOK: true,
		//},
		// Add 10 more test cases with combinations of fields, joins, where, group by, having, and struct scans.
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			sb := newSelectBuilder()
			sb = sb.Select(tc.fields...).From(tc.schemaFrom)

			for _, j := range tc.join {
				switch j.Type() {
				case Inner:
					sb = sb.Join(j.Schema(), j.On())
				case Left:
					sb = sb.LeftJoin(j.Schema(), j.On())
				case Right:
					sb = sb.RightJoin(j.Schema(), j.On())
				case Outer:
					sb = sb.OuterJoin(j.Schema(), j.On())
				default:
					t.Fatalf("unsupported join type: %d", j.Type())
				}
			}
			if tc.structScan != nil {
				sb = sb.SelectStruct(tc.structScan, "users")
			}

			gotExpr, gotOK := sb.Where(tc.whereExpressions...).
				GroupBy(tc.groupByFields...).
				Having(tc.havingExpressions...).
				Expression()

			assert.Equal(t, tc.expectOK, gotOK)

			assert.Equal(t, tc.wantExpr, gotExpr)
		})
	}
}
