package ksql

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_SelectExpression(t *testing.T) {
	testcases := []struct {
		name               string
		fields             []Field
		schemaFrom         string
		whereExpressions   []Expression
		join               []JoinExpression
		havingExpressions  []Expression
		groupByFields      []Field
		orderbyExpressions []OrderedExpression
		structScan         any
		wantExpr           string
		expectOK           bool
	}{
		{
			name:       "Simple SELECT with one field",
			fields:     []Field{F("table.column1")},
			schemaFrom: "table",
			wantExpr:   "SELECT table.column1 FROM table;",
			expectOK:   true,
		},
		{
			name:       "SELECT with alias",
			fields:     []Field{F("table.column1").As("alias1")},
			schemaFrom: "table",
			wantExpr:   "SELECT table.column1 AS alias1 FROM table;",
			expectOK:   true,
		},
		{
			name:             "SELECT with WHERE clause",
			fields:           []Field{F("table.column1")},
			schemaFrom:       "table",
			whereExpressions: []Expression{F("table.column1").Equal(1)},
			wantExpr:         "SELECT table.column1 FROM table WHERE table.column1 = 1;",
			expectOK:         true,
		},
		{
			name:       "SELECT with JOIN",
			fields:     []Field{F("table1.column1"), F("table2.column2")},
			schemaFrom: "table1",
			join:       []JoinExpression{Join("table2", F("table1.id").Equal(F("table2.id")), Inner)},
			wantExpr:   "SELECT table1.column1, table2.column2 FROM table1 JOIN table2 ON table1.id = table2.id;",
			expectOK:   true,
		},
		{
			name:       "SELECT with LEFT JOIN",
			fields:     []Field{F("table1.column1"), F("table2.column2")},
			schemaFrom: "table1",
			join:       []JoinExpression{Join("table2", F("table1.id").Equal(F("table2.id")), Left)},
			wantExpr:   "SELECT table1.column1, table2.column2 FROM table1 LEFT JOIN table2 ON table1.id = table2.id;",
			expectOK:   true,
		},
		{
			name:          "SELECT with GROUP BY",
			fields:        []Field{F("table.column1"), F("table.column2")},
			schemaFrom:    "table",
			groupByFields: []Field{F("table.column1")},
			wantExpr:      "SELECT table.column1, table.column2 FROM table GROUP BY table.column1;",
			expectOK:      true,
		},
		{
			name:              "SELECT with HAVING",
			fields:            []Field{F("table.column1"), F("table.column2")},
			schemaFrom:        "table",
			groupByFields:     []Field{F("table.column1")},
			havingExpressions: []Expression{Count(F("table.column2")).Greater(1)},
			wantExpr:          "SELECT table.column1, table.column2 FROM table GROUP BY table.column1 HAVING COUNT(table.column2) > 1;",
			expectOK:          true,
		},
		{
			name:       "SELECT with multiple JOINs",
			fields:     []Field{F("table1.column1"), F("table2.column2"), F("table3.column3")},
			schemaFrom: "table1",
			join: []JoinExpression{
				Join("table2", F("table1.id").Equal(F("table2.id")), Inner),
				Join("table3", F("table2.id").Equal(F("table3.id")), Left),
			},
			wantExpr: "SELECT table1.column1, table2.column2, table3.column3 FROM table1 JOIN table2 ON table1.id = table2.id LEFT JOIN table3 ON table2.id = table3.id;",
			expectOK: true,
		},
		{
			name: "SELECT with struct scan",
			structScan: struct {
				ID   int    `ksql:"id"`
				Name string `ksql:"name"`
			}{},
			schemaFrom: "users",
			wantExpr:   "SELECT users.id, users.name FROM users;",
			expectOK:   true,
		},
		{
			name:             "SELECT with multiple WHERE clauses",
			fields:           []Field{F("table.column1")},
			schemaFrom:       "table",
			whereExpressions: []Expression{F("table.column1").Equal(1), F("table.column2").Greater(5)},
			wantExpr:         "SELECT table.column1 FROM table WHERE table.column1 = 1 AND table.column2 > 5;",
			expectOK:         true,
		},
		{
			name:             "SELECT with multiple WHERE and JOIN clauses",
			fields:           []Field{F("table1.column1"), F("table2.column2")},
			schemaFrom:       "table1",
			join:             []JoinExpression{Join("table2", F("table1.id").Equal(F("table2.id")), Inner)},
			whereExpressions: []Expression{F("table1.column1").Equal(1), F("table2.column2").Less(10)},
			wantExpr:         "SELECT table1.column1, table2.column2 FROM table1 JOIN table2 ON table1.id = table2.id WHERE table1.column1 = 1 AND table2.column2 < 10;",
			expectOK:         true,
		},
		{
			name:             "SELECT with multiple WHERE, JOIN, and GROUP BY",
			fields:           []Field{F("table1.column1"), F("table2.column2")},
			schemaFrom:       "table1",
			join:             []JoinExpression{Join("table2", F("table1.id").Equal(F("table2.id")), Left)},
			whereExpressions: []Expression{F("table1.column1").Greater(5), F("table2.column2").NotEqual(3)},
			groupByFields:    []Field{F("table1.column1")},
			wantExpr:         "SELECT table1.column1, table2.column2 FROM table1 LEFT JOIN table2 ON table1.id = table2.id WHERE table1.column1 > 5 AND table2.column2 != 3 GROUP BY table1.column1;",
			expectOK:         true,
		},
		{
			name:             "SELECT with WHERE, JOIN, GROUP BY, and HAVING",
			fields:           []Field{F("table1.column1"), F("table2.column2")},
			schemaFrom:       "table1",
			join:             []JoinExpression{Join("table2", F("table1.id").Equal(F("table2.id")), Inner)},
			whereExpressions: []Expression{F("table1.column1").Equal(2)},
			groupByFields:    []Field{F("table1.column1")},
			havingExpressions: []Expression{
				F("COUNT(table2.column2)").Greater(1),
			},
			wantExpr: "SELECT table1.column1, table2.column2 FROM table1 JOIN table2 ON table1.id = table2.id WHERE table1.column1 = 2 GROUP BY table1.column1 HAVING COUNT(table2.column2) > 1;",
			expectOK: true,
		},
		{
			name:       "SELECT with multiple JOINs and ORDER BY",
			fields:     []Field{F("table1.column1"), F("table2.column2"), F("table3.column3")},
			schemaFrom: "table1",
			join: []JoinExpression{
				Join("table2", F("table1.id").Equal(F("table2.id")), Inner),
				Join("table3", F("table2.id").Equal(F("table3.id")), Left),
			},
			wantExpr: "SELECT table1.column1, table2.column2, table3.column3 FROM table1 JOIN table2 ON table1.id = table2.id LEFT JOIN table3 ON table2.id = table3.id;",
			expectOK: true,
		},
		{
			name:          "SELECT with HAVING clause",
			fields:        []Field{F("table.column1"), F("table.column2")},
			schemaFrom:    "table",
			groupByFields: []Field{F("table.column1")},
			havingExpressions: []Expression{
				Count(F("table.column2")).Greater(1),
			},
			wantExpr: "SELECT table.column1, table.column2 FROM table GROUP BY table.column1 HAVING COUNT(table.column2) > 1;",
			expectOK: true,
		},
		{
			name:               "SELECT with ORDER BY",
			fields:             []Field{F("table.column1"), F("table.column2")},
			orderbyExpressions: []OrderedExpression{F("table.column1").Asc(), F("table.column2").Desc()},

			schemaFrom: "table",
			wantExpr:   "SELECT table.column1, table.column2 FROM table ORDER BY table.column1 ASC, table.column2 DESC;",
			expectOK:   true,
		},
		{
			name: "Complex SELECT with multiple JOINs, WHERE, GROUP BY, HAVING, ORDER BY, and aggregate functions",
			fields: []Field{
				Count(F("table1.column1")).As("count_column1"),
				Sum(F("table2.column2")).As("sum_column2"),
				Avg(F("table3.column3")).As("avg_column3"),
			},
			schemaFrom: "table1",
			join: []JoinExpression{
				Join("table2", F("table1.id").Equal(F("table2.id")), Inner),
				Join("table3", F("table2.id").Equal(F("table3.id")), Left),
			},
			whereExpressions: []Expression{
				F("table1.column1").Greater(10),
				F("table2.column2").Less(50),
				F("table3.column3").NotEqual(0),
			},
			groupByFields: []Field{F("table1.column1")},
			havingExpressions: []Expression{
				Count(F("table2.column2")).Greater(5),
				Sum(F("table3.column3")).Less(100),
			},
			orderbyExpressions: []OrderedExpression{
				F("count_column1").Desc(),
				F("sum_column2").Asc(),
			},
			wantExpr: "SELECT COUNT(table1.column1) AS count_column1, SUM(table2.column2) AS sum_column2, AVG(table3.column3) AS avg_column3 FROM table1 JOIN table2 ON table1.id = table2.id LEFT JOIN table3 ON table2.id = table3.id WHERE table1.column1 > 10 AND table2.column2 < 50 AND table3.column3 != 0 GROUP BY table1.column1 HAVING COUNT(table2.column2) > 5 AND SUM(table3.column3) < 100 ORDER BY count_column1 DESC, sum_column2 ASC;",
			expectOK: true,
		},
		{
			name: "Simple SELECT with multiple fields",
			fields: []Field{
				F("table.column1"),
				F("table.column2"),
			},
			schemaFrom: "table",
			wantExpr:   "SELECT table.column1, table.column2 FROM table;",
			expectOK:   true,
		},
		{
			name: "SELECT with alias and aggregate function",
			fields: []Field{
				Count(F("table.column1")).As("count_column1"),
			},
			schemaFrom: "table",
			wantExpr:   "SELECT COUNT(table.column1) AS count_column1 FROM table;",
			expectOK:   true,
		},
		{
			name: "SELECT with INNER JOIN and WHERE clause",
			fields: []Field{
				F("table1.column1"),
				F("table2.column2"),
			},
			schemaFrom: "table1",
			join: []JoinExpression{
				Join("table2", F("table1.id").Equal(F("table2.id")), Inner),
			},
			whereExpressions: []Expression{
				F("table1.column1").Greater(5),
			},
			wantExpr: "SELECT table1.column1, table2.column2 FROM table1 JOIN table2 ON table1.id = table2.id WHERE table1.column1 > 5;",
			expectOK: true,
		},
		{
			name: "SELECT with RIGHT JOIN and multiple WHERE clauses",
			fields: []Field{
				F("table1.column1"),
				F("table2.column2"),
			},
			schemaFrom: "table1",
			join: []JoinExpression{
				Join("table2", F("table1.id").Equal(F("table2.id")), Right),
			},
			whereExpressions: []Expression{
				F("table1.column1").NotEqual(0),
				F("table2.column2").Less(100),
			},
			wantExpr: "SELECT table1.column1, table2.column2 FROM table1 RIGHT JOIN table2 ON table1.id = table2.id WHERE table1.column1 != 0 AND table2.column2 < 100;",
			expectOK: true,
		},
		{
			name: "SELECT with GROUP BY and aggregate function",
			fields: []Field{
				F("table.column1"),
				Count(F("table.column2")).As("count_column2"),
			},
			schemaFrom: "table",
			groupByFields: []Field{
				F("table.column1"),
			},
			wantExpr: "SELECT table.column1, COUNT(table.column2) AS count_column2 FROM table GROUP BY table.column1;",
			expectOK: true,
		},
		{
			name: "SELECT with HAVING clause and aggregate function",
			fields: []Field{
				F("table.column1"),
				Sum(F("table.column2")).As("sum_column2"),
			},
			schemaFrom: "table",
			groupByFields: []Field{
				F("table.column1"),
			},
			havingExpressions: []Expression{
				Sum(F("table.column2")).Greater(50),
			},
			wantExpr: "SELECT table.column1, SUM(table.column2) AS sum_column2 FROM table GROUP BY table.column1 HAVING SUM(table.column2) > 50;",
			expectOK: true,
		},
		{
			name: "SELECT with ORDER BY clause",
			fields: []Field{
				F("table.column1"),
				F("table.column2"),
			},
			schemaFrom: "table",
			orderbyExpressions: []OrderedExpression{
				F("table.column1").Asc(),
				F("table.column2").Desc(),
			},
			wantExpr: "SELECT table.column1, table.column2 FROM table ORDER BY table.column1 ASC, table.column2 DESC;",
			expectOK: true,
		},
		{
			name: "SELECT with multiple JOINs and WHERE clause",
			fields: []Field{
				F("table1.column1"),
				F("table2.column2"),
				F("table3.column3"),
			},
			schemaFrom: "table1",
			join: []JoinExpression{
				Join("table2", F("table1.id").Equal(F("table2.id")), Inner),
				Join("table3", F("table2.id").Equal(F("table3.id")), Left),
			},
			whereExpressions: []Expression{
				F("table1.column1").Greater(10),
				F("table3.column3").NotEqual(0),
			},
			wantExpr: "SELECT table1.column1, table2.column2, table3.column3 FROM table1 JOIN table2 ON table1.id = table2.id LEFT JOIN table3 ON table2.id = table3.id WHERE table1.column1 > 10 AND table3.column3 != 0;",
			expectOK: true,
		},
		{
			name: "SELECT with multiple GROUP BY fields",
			fields: []Field{
				F("table.column1"),
				F("table.column2"),
				Count(F("table.column3")).As("count_column3"),
			},
			schemaFrom: "table",
			groupByFields: []Field{
				F("table.column1"),
				F("table.column2"),
			},
			wantExpr: "SELECT table.column1, table.column2, COUNT(table.column3) AS count_column3 FROM table GROUP BY table.column1, table.column2;",
			expectOK: true,
		},
		{
			name: "SELECT with HAVING and ORDER BY",
			fields: []Field{
				F("table.column1"),
				Sum(F("table.column2")).As("sum_column2"),
			},
			schemaFrom: "table",
			groupByFields: []Field{
				F("table.column1"),
			},
			havingExpressions: []Expression{
				Sum(F("table.column2")).Greater(100),
			},
			orderbyExpressions: []OrderedExpression{
				F("sum_column2").Desc(),
			},
			wantExpr: "SELECT table.column1, SUM(table.column2) AS sum_column2 FROM table GROUP BY table.column1 HAVING SUM(table.column2) > 100 ORDER BY sum_column2 DESC;",
			expectOK: true,
		},
		{
			name: "SELECT",
			fields: []Field{
				F("table.column1"),
			},
			schemaFrom: "table",
			wantExpr:   "SELECT table.column1 FROM table;",
			expectOK:   true,
		},
		{
			name: "SELECT with aggregate function and alias",
			fields: []Field{
				Avg(F("table.column1")).As("avg_column1"),
			},
			schemaFrom: "table",
			wantExpr:   "SELECT AVG(table.column1) AS avg_column1 FROM table;",
			expectOK:   true,
		},
		{
			name: "SELECT with multiple WHERE and GROUP BY",
			fields: []Field{
				F("table.column1"),
				Count(F("table.column2")).As("count_column2"),
			},
			schemaFrom: "table",
			whereExpressions: []Expression{
				F("table.column1").Greater(5),
				F("table.column2").Less(50),
			},
			groupByFields: []Field{
				F("table.column1"),
			},
			wantExpr: "SELECT table.column1, COUNT(table.column2) AS count_column2 FROM table WHERE table.column1 > 5 AND table.column2 < 50 GROUP BY table.column1;",
			expectOK: true,
		},
		{
			name: "SELECT with multiple HAVING conditions",
			fields: []Field{
				F("table.column1"),
				Sum(F("table.column2")).As("sum_column2"),
			},
			schemaFrom: "table",
			groupByFields: []Field{
				F("table.column1"),
			},
			havingExpressions: []Expression{
				Sum(F("table.column2")).Greater(50),
				Count(F("table.column1")).Less(10),
			},
			wantExpr: "SELECT table.column1, SUM(table.column2) AS sum_column2 FROM table GROUP BY table.column1 HAVING SUM(table.column2) > 50 AND COUNT(table.column1) < 10;",
			expectOK: true,
		},
		{
			name: "SELECT with multiple ORDER BY fields",
			fields: []Field{
				F("table.column1"),
				F("table.column2"),
			},
			schemaFrom: "table",
			orderbyExpressions: []OrderedExpression{
				F("table.column1").Asc(),
				F("table.column2").Desc(),
			},
			wantExpr: "SELECT table.column1, table.column2 FROM table ORDER BY table.column1 ASC, table.column2 DESC;",
			expectOK: true,
		},
		{
			name: "SELECT with aggregate function in HAVING",
			fields: []Field{
				F("table.column1"),
			},
			schemaFrom: "table",
			groupByFields: []Field{
				F("table.column1"),
			},
			havingExpressions: []Expression{
				Count(F("table.column1")).Greater(5),
			},
			wantExpr: "SELECT table.column1 FROM table GROUP BY table.column1 HAVING COUNT(table.column1) > 5;",
			expectOK: true,
		},
		{
			name: "SELECT with LEFT JOIN and ORDER BY",
			fields: []Field{
				F("table1.column1"),
				F("table2.column2"),
			},
			schemaFrom: "table1",
			join: []JoinExpression{
				Join("table2", F("table1.id").Equal(F("table2.id")), Left),
			},
			orderbyExpressions: []OrderedExpression{
				F("table1.column1").Asc(),
			},
			wantExpr: "SELECT table1.column1, table2.column2 FROM table1 LEFT JOIN table2 ON table1.id = table2.id ORDER BY table1.column1 ASC;",
			expectOK: true,
		},
		{
			name: "SELECT with RIGHT JOIN and GROUP BY",
			fields: []Field{
				F("table1.column1"),
				Count(F("table2.column2")).As("count_column2"),
			},
			schemaFrom: "table1",
			join: []JoinExpression{
				Join("table2", F("table1.id").Equal(F("table2.id")), Right),
			},
			groupByFields: []Field{
				F("table1.column1"),
			},
			wantExpr: "SELECT table1.column1, COUNT(table2.column2) AS count_column2 FROM table1 RIGHT JOIN table2 ON table1.id = table2.id GROUP BY table1.column1;",
			expectOK: true,
		},
		{
			name: "SELECT with FULL OUTER JOIN",
			fields: []Field{
				F("table1.column1"),
				F("table2.column2"),
			},
			schemaFrom: "table1",
			join: []JoinExpression{
				Join("table2", F("table1.id").Equal(F("table2.id")), Outer),
			},
			wantExpr: "SELECT table1.column1, table2.column2 FROM table1 OUTER JOIN table2 ON table1.id = table2.id;",
			expectOK: true,
		},
		{
			name: "SELECT with multiple JOINs, WHERE, and ORDER BY",
			fields: []Field{
				F("table1.column1"),
				F("table2.column2"),
				F("table3.column3"),
			},
			schemaFrom: "table1",
			join: []JoinExpression{
				Join("table2", F("table1.id").Equal(F("table2.id")), Inner),
				Join("table3", F("table2.id").Equal(F("table3.id")), Left),
			},
			whereExpressions: []Expression{
				F("table1.column1").Greater(10),
				F("table3.column3").NotEqual(0),
			},
			orderbyExpressions: []OrderedExpression{
				F("table1.column1").Asc(),
			},
			wantExpr: "SELECT table1.column1, table2.column2, table3.column3 FROM table1 JOIN table2 ON table1.id = table2.id LEFT JOIN table3 ON table2.id = table3.id WHERE table1.column1 > 10 AND table3.column3 != 0 ORDER BY table1.column1 ASC;",
			expectOK: true,
		},
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
				sb = sb.SelectStruct("users", tc.structScan)
			}

			gotExpr, gotOK := sb.Where(tc.whereExpressions...).
				GroupBy(tc.groupByFields...).
				Having(tc.havingExpressions...).
				OrderBy(tc.orderbyExpressions...).
				Expression()

			assert.Equal(t, tc.expectOK, gotOK)

			assert.Equal(t, tc.wantExpr, gotExpr)
		})
	}
}
