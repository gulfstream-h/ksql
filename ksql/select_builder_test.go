package ksql

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"ksql/schema"
	"ksql/static"
	"regexp"
	"sort"
	"strings"
	"testing"
)

func normalizeSelectSQL(sql string) string {

	re := regexp.MustCompile(`(?i)^SELECT\s+(.+?)\s+FROM\s+(\w+)\s*(.*);?$`)
	matches := re.FindStringSubmatch(sql)
	if len(matches) != 4 {
		return sql
	}

	fields := strings.Split(matches[1], ", ")
	sort.Strings(fields)
	relation := matches[2]
	tail := matches[3]
	return "SELECT " + strings.Join(fields, ", ") + " FROM " + relation + tail + ";"
}

func Test_SelectExpression(t *testing.T) {
	testcases := []struct {
		name                string
		fields              []Field
		schemaFrom          string
		whereExpressions    []Conditional
		join                []JoinExpression
		havingExpressions   []Conditional
		groupByFields       []Field
		windowEx            WindowExpression
		orderbyExpressions  []OrderedExpression
		structScan          any
		wantExpr            string
		expectErr           bool
		normalizationNeeded bool
	}{
		{
			name:       "Simple SELECT with one field",
			fields:     []Field{F("table.column1")},
			schemaFrom: "table",
			wantExpr:   "SELECT table.column1 FROM table;",
			expectErr:  false,
		},
		{
			name:       "SELECT with alias",
			fields:     []Field{F("table.column1").As("alias1")},
			schemaFrom: "table",
			wantExpr:   "SELECT table.column1 AS alias1 FROM table;",
			expectErr:  false,
		},
		{
			name:             "SELECT with WHERE clause",
			fields:           []Field{F("table.column1")},
			schemaFrom:       "table",
			whereExpressions: []Conditional{F("table.column1").Equal(1)},
			wantExpr:         "SELECT table.column1 FROM table WHERE table.column1 = 1;",
			expectErr:        false,
		},
		{
			name:       "SELECT with JOIN",
			fields:     []Field{F("table1.column1"), F("table2.column2")},
			schemaFrom: "table1",
			join:       []JoinExpression{Join(Schema("table2", TABLE), F("table1.id").Equal(F("table2.id")), Inner)},
			wantExpr:   "SELECT table1.column1, table2.column2 FROM table1 JOIN table2 ON table1.id = table2.id;",
			expectErr:  false,
		},
		{
			name:       "SELECT with LEFT JOIN",
			fields:     []Field{F("table1.column1"), F("table2.column2")},
			schemaFrom: "table1",
			join:       []JoinExpression{Join(Schema("table2", TABLE), F("table1.id").Equal(F("table2.id")), Left)},
			wantExpr:   "SELECT table1.column1, table2.column2 FROM table1 LEFT JOIN table2 ON table1.id = table2.id;",
			expectErr:  false,
		},
		{
			name:          "SELECT with GROUP BY",
			fields:        []Field{F("table.column1"), F("table.column2")},
			schemaFrom:    "table",
			groupByFields: []Field{F("table.column1")},
			wantExpr:      "SELECT table.column1, table.column2 FROM table GROUP BY table.column1;",
			expectErr:     false,
		},
		{
			name:              "SELECT with HAVING",
			fields:            []Field{F("table.column1"), F("table.column2")},
			schemaFrom:        "table",
			groupByFields:     []Field{F("table.column1")},
			havingExpressions: []Conditional{Count(F("table.column2")).Greater(1)},
			wantExpr:          "SELECT table.column1, table.column2 FROM table GROUP BY table.column1 HAVING COUNT(table.column2) > 1;",
			expectErr:         false,
		},
		{
			name:       "SELECT with multiple JOINs",
			fields:     []Field{F("table1.column1"), F("table2.column2"), F("table3.column3")},
			schemaFrom: "table1",
			join: []JoinExpression{
				Join(Schema("table2", TABLE), F("table1.id").Equal(F("table2.id")), Inner),
				Join(Schema("table3", TABLE), F("table2.id").Equal(F("table3.id")), Left),
			},
			wantExpr:  "SELECT table1.column1, table2.column2, table3.column3 FROM table1 JOIN table2 ON table1.id = table2.id LEFT JOIN table3 ON table2.id = table3.id;",
			expectErr: false,
		},
		{
			name: "SELECT with struct scan",
			structScan: struct {
				ID   int    `ksql:"id"`
				Name string `ksql:"name"`
			}{
				ID:   0,
				Name: "",
			},
			schemaFrom:          "users",
			wantExpr:            "SELECT users.id, users.name FROM users;",
			expectErr:           false,
			normalizationNeeded: true,
		},
		{
			name:             "SELECT with multiple WHERE clauses",
			fields:           []Field{F("table.column1")},
			schemaFrom:       "table",
			whereExpressions: []Conditional{F("table.column1").Equal(1), F("table.column2").Greater(5)},
			wantExpr:         "SELECT table.column1 FROM table WHERE table.column1 = 1 AND table.column2 > 5;",
			expectErr:        false,
		},
		{
			name:             "SELECT with multiple WHERE and JOIN clauses",
			fields:           []Field{F("table1.column1"), F("table2.column2")},
			schemaFrom:       "table1",
			join:             []JoinExpression{Join(Schema("table2", TABLE), F("table1.id").Equal(F("table2.id")), Inner)},
			whereExpressions: []Conditional{F("table1.column1").Equal(1), F("table2.column2").Less(10)},
			wantExpr:         "SELECT table1.column1, table2.column2 FROM table1 JOIN table2 ON table1.id = table2.id WHERE table1.column1 = 1 AND table2.column2 < 10;",
			expectErr:        false,
		},
		{
			name:             "SELECT with multiple WHERE, JOIN, and GROUP BY",
			fields:           []Field{F("table1.column1"), F("table2.column2")},
			schemaFrom:       "table1",
			join:             []JoinExpression{Join(Schema("table2", TABLE), F("table1.id").Equal(F("table2.id")), Left)},
			whereExpressions: []Conditional{F("table1.column1").Greater(5), F("table2.column2").NotEqual(3)},
			groupByFields:    []Field{F("table1.column1")},
			wantExpr:         "SELECT table1.column1, table2.column2 FROM table1 LEFT JOIN table2 ON table1.id = table2.id WHERE table1.column1 > 5 AND table2.column2 != 3 GROUP BY table1.column1;",
			expectErr:        false,
		},
		{
			name:             "SELECT with WHERE, JOIN, GROUP BY, and HAVING",
			fields:           []Field{F("table1.column1"), F("table2.column2")},
			schemaFrom:       "table1",
			join:             []JoinExpression{Join(Schema("table2", TABLE), F("table1.id").Equal(F("table2.id")), Inner)},
			whereExpressions: []Conditional{F("table1.column1").Equal(2)},
			groupByFields:    []Field{F("table1.column1")},
			havingExpressions: []Conditional{
				F("COUNT(table2.column2)").Greater(1),
			},
			wantExpr:  "SELECT table1.column1, table2.column2 FROM table1 JOIN table2 ON table1.id = table2.id WHERE table1.column1 = 2 GROUP BY table1.column1 HAVING COUNT(table2.column2) > 1;",
			expectErr: false,
		},
		{
			name:       "SELECT with multiple JOINs and ORDER BY",
			fields:     []Field{F("table1.column1"), F("table2.column2"), F("table3.column3")},
			schemaFrom: "table1",
			join: []JoinExpression{
				Join(Schema("table2", TABLE), F("table1.id").Equal(F("table2.id")), Inner),
				Join(Schema("table3", TABLE), F("table2.id").Equal(F("table3.id")), Left),
			},
			wantExpr:  "SELECT table1.column1, table2.column2, table3.column3 FROM table1 JOIN table2 ON table1.id = table2.id LEFT JOIN table3 ON table2.id = table3.id;",
			expectErr: false,
		},
		{
			name:          "SELECT with HAVING clause",
			fields:        []Field{F("table.column1"), F("table.column2")},
			schemaFrom:    "table",
			groupByFields: []Field{F("table.column1")},
			havingExpressions: []Conditional{
				Count(F("table.column2")).Greater(1),
			},
			wantExpr:  "SELECT table.column1, table.column2 FROM table GROUP BY table.column1 HAVING COUNT(table.column2) > 1;",
			expectErr: false,
		},
		{
			name:               "SELECT with ORDER BY",
			fields:             []Field{F("table.column1"), F("table.column2")},
			orderbyExpressions: []OrderedExpression{F("table.column1").Asc(), F("table.column2").Desc()},

			schemaFrom: "table",
			wantExpr:   "SELECT table.column1, table.column2 FROM table ORDER BY table.column1 ASC, table.column2 DESC;",
			expectErr:  false,
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
				Join(Schema("table2", TABLE), F("table1.id").Equal(F("table2.id")), Inner),
				Join(Schema("table3", TABLE), F("table2.id").Equal(F("table3.id")), Left),
			},
			whereExpressions: []Conditional{
				F("table1.column1").Greater(10),
				F("table2.column2").Less(50),
				F("table3.column3").NotEqual(0),
			},
			groupByFields: []Field{F("table1.column1")},
			havingExpressions: []Conditional{
				Count(F("table2.column2")).Greater(5),
				Sum(F("table3.column3")).Less(100),
			},
			orderbyExpressions: []OrderedExpression{
				F("count_column1").Desc(),
				F("sum_column2").Asc(),
			},
			wantExpr:  "SELECT COUNT(table1.column1) AS count_column1, SUM(table2.column2) AS sum_column2, AVG(table3.column3) AS avg_column3 FROM table1 JOIN table2 ON table1.id = table2.id LEFT JOIN table3 ON table2.id = table3.id WHERE table1.column1 > 10 AND table2.column2 < 50 AND table3.column3 != 0 GROUP BY table1.column1 HAVING COUNT(table2.column2) > 5 AND SUM(table3.column3) < 100 ORDER BY count_column1 DESC, sum_column2 ASC;",
			expectErr: false,
		},
		{
			name: "Simple SELECT with multiple fields",
			fields: []Field{
				F("table.column1"),
				F("table.column2"),
			},
			schemaFrom: "table",
			wantExpr:   "SELECT table.column1, table.column2 FROM table;",
			expectErr:  false,
		},
		{
			name: "SELECT with alias and aggregate function",
			fields: []Field{
				Count(F("table.column1")).As("count_column1"),
			},
			groupByFields: []Field{F("count_column1")},
			schemaFrom:    "table",
			wantExpr:      "SELECT COUNT(table.column1) AS count_column1 FROM table GROUP BY count_column1;",
			expectErr:     false,
		},
		{
			name: "SELECT with INNER JOIN and WHERE clause",
			fields: []Field{
				F("table1.column1"),
				F("table2.column2"),
			},
			schemaFrom: "table1",
			join: []JoinExpression{
				Join(Schema("table2", TABLE), F("table1.id").Equal(F("table2.id")), Inner),
			},
			whereExpressions: []Conditional{
				F("table1.column1").Greater(5),
			},
			wantExpr:  "SELECT table1.column1, table2.column2 FROM table1 JOIN table2 ON table1.id = table2.id WHERE table1.column1 > 5;",
			expectErr: false,
		},
		{
			name: "SELECT with RIGHT JOIN and multiple WHERE clauses",
			fields: []Field{
				F("table1.column1"),
				F("table2.column2"),
			},
			schemaFrom: "table1",
			join: []JoinExpression{
				Join(Schema("table2", TABLE), F("table1.id").Equal(F("table2.id")), Right),
			},
			whereExpressions: []Conditional{
				F("table1.column1").NotEqual(0),
				F("table2.column2").Less(100),
			},
			wantExpr:  "SELECT table1.column1, table2.column2 FROM table1 RIGHT JOIN table2 ON table1.id = table2.id WHERE table1.column1 != 0 AND table2.column2 < 100;",
			expectErr: false,
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
			wantExpr:  "SELECT table.column1, COUNT(table.column2) AS count_column2 FROM table GROUP BY table.column1;",
			expectErr: false,
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
			havingExpressions: []Conditional{
				Sum(F("table.column2")).Greater(50),
			},
			wantExpr:  "SELECT table.column1, SUM(table.column2) AS sum_column2 FROM table GROUP BY table.column1 HAVING SUM(table.column2) > 50;",
			expectErr: false,
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
			wantExpr:  "SELECT table.column1, table.column2 FROM table ORDER BY table.column1 ASC, table.column2 DESC;",
			expectErr: false,
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
				Join(Schema("table2", TABLE), F("table1.id").Equal(F("table2.id")), Inner),
				Join(Schema("table3", TABLE), F("table2.id").Equal(F("table3.id")), Left),
			},
			whereExpressions: []Conditional{
				F("table1.column1").Greater(10),
				F("table3.column3").NotEqual(0),
			},
			wantExpr:  "SELECT table1.column1, table2.column2, table3.column3 FROM table1 JOIN table2 ON table1.id = table2.id LEFT JOIN table3 ON table2.id = table3.id WHERE table1.column1 > 10 AND table3.column3 != 0;",
			expectErr: false,
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
			wantExpr:  "SELECT table.column1, table.column2, COUNT(table.column3) AS count_column3 FROM table GROUP BY table.column1, table.column2;",
			expectErr: false,
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
			havingExpressions: []Conditional{
				Sum(F("table.column2")).Greater(100),
			},
			orderbyExpressions: []OrderedExpression{
				F("sum_column2").Desc(),
			},
			wantExpr:  "SELECT table.column1, SUM(table.column2) AS sum_column2 FROM table GROUP BY table.column1 HAVING SUM(table.column2) > 100 ORDER BY sum_column2 DESC;",
			expectErr: false,
		},
		{
			name: "SELECT",
			fields: []Field{
				F("table.column1"),
			},
			schemaFrom: "table",
			wantExpr:   "SELECT table.column1 FROM table;",
			expectErr:  false,
		},
		{
			name: "SELECT with aggregate function and alias",
			fields: []Field{
				Avg(F("table.column1")).As("avg_column1"),
			},
			groupByFields: []Field{F("avg_column1")},
			schemaFrom:    "table",
			wantExpr:      "SELECT AVG(table.column1) AS avg_column1 FROM table GROUP BY avg_column1;",
			expectErr:     false,
		},
		{
			name: "SELECT with multiple WHERE and GROUP BY",
			fields: []Field{
				F("table.column1"),
				Count(F("table.column2")).As("count_column2"),
			},
			schemaFrom: "table",
			whereExpressions: []Conditional{
				F("table.column1").Greater(5),
				F("table.column2").Less(50),
			},
			groupByFields: []Field{
				F("table.column1"),
			},
			wantExpr:  "SELECT table.column1, COUNT(table.column2) AS count_column2 FROM table WHERE table.column1 > 5 AND table.column2 < 50 GROUP BY table.column1;",
			expectErr: false,
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
			havingExpressions: []Conditional{
				Sum(F("table.column2")).Greater(50),
				Count(F("table.column1")).Less(10),
			},
			wantExpr:  "SELECT table.column1, SUM(table.column2) AS sum_column2 FROM table GROUP BY table.column1 HAVING SUM(table.column2) > 50 AND COUNT(table.column1) < 10;",
			expectErr: false,
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
			wantExpr:  "SELECT table.column1, table.column2 FROM table ORDER BY table.column1 ASC, table.column2 DESC;",
			expectErr: false,
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
			havingExpressions: []Conditional{
				Count(F("table.column1")).Greater(5),
			},
			wantExpr:  "SELECT table.column1 FROM table GROUP BY table.column1 HAVING COUNT(table.column1) > 5;",
			expectErr: false,
		},
		{
			name: "SELECT with LEFT JOIN and ORDER BY",
			fields: []Field{
				F("table1.column1"),
				F("table2.column2"),
			},
			schemaFrom: "table1",
			join: []JoinExpression{
				Join(Schema("table2", TABLE), F("table1.id").Equal(F("table2.id")), Left),
			},
			orderbyExpressions: []OrderedExpression{
				F("table1.column1").Asc(),
			},
			wantExpr:  "SELECT table1.column1, table2.column2 FROM table1 LEFT JOIN table2 ON table1.id = table2.id ORDER BY table1.column1 ASC;",
			expectErr: false,
		},
		{
			name: "SELECT with RIGHT JOIN and GROUP BY",
			fields: []Field{
				F("table1.column1"),
				Count(F("table2.column2")).As("count_column2"),
			},
			schemaFrom: "table1",
			join: []JoinExpression{
				Join(Schema("table2", TABLE), F("table1.id").Equal(F("table2.id")), Right),
			},
			groupByFields: []Field{
				F("table1.column1"),
			},
			wantExpr:  "SELECT table1.column1, COUNT(table2.column2) AS count_column2 FROM table1 RIGHT JOIN table2 ON table1.id = table2.id GROUP BY table1.column1;",
			expectErr: false,
		},
		{
			name: "SELECT with FULL OUTER JOIN",
			fields: []Field{
				F("table1.column1"),
				F("table2.column2"),
			},
			schemaFrom: "table1",
			join: []JoinExpression{
				Join(Schema("table2", TABLE), F("table1.id").Equal(F("table2.id")), Outer),
			},
			wantExpr:  "SELECT table1.column1, table2.column2 FROM table1 OUTER JOIN table2 ON table1.id = table2.id;",
			expectErr: false,
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
				Join(Schema("table2", TABLE), F("table1.id").Equal(F("table2.id")), Inner),
				Join(Schema("table3", TABLE), F("table2.id").Equal(F("table3.id")), Left),
			},
			whereExpressions: []Conditional{
				F("table1.column1").Greater(10),
				F("table3.column3").NotEqual(0),
			},
			orderbyExpressions: []OrderedExpression{
				F("table1.column1").Asc(),
			},
			wantExpr:  "SELECT table1.column1, table2.column2, table3.column3 FROM table1 JOIN table2 ON table1.id = table2.id LEFT JOIN table3 ON table2.id = table3.id WHERE table1.column1 > 10 AND table3.column3 != 0 ORDER BY table1.column1 ASC;",
			expectErr: false,
		},
		{
			name:       "SELECT with invalid Window (negative size)",
			fields:     []Field{F("table.column1")},
			schemaFrom: "table",
			windowEx:   NewTumblingWindow(TimeUnit{Val: -10, Unit: Seconds}),
			wantExpr:   "",
			expectErr:  true,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			sb := newSelectBuilder()
			sb = sb.Select(tc.fields...).From(Schema(tc.schemaFrom, TABLE))

			for _, j := range tc.join {
				switch j.Type() {
				case Inner:
					sb = sb.Join(Schema(j.Schema(), TABLE), j.On())
				case Left:
					sb = sb.LeftJoin(Schema(j.Schema(), TABLE), j.On())
				case Right:
					sb = sb.RightJoin(Schema(j.Schema(), TABLE), j.On())
				case Outer:
					sb = sb.OuterJoin(Schema(j.Schema(), TABLE), j.On())
				default:
					t.Fatalf("unsupported join type: %d", j.Type())
				}
			}
			if tc.structScan != nil {
				sb = sb.SelectStruct("users", tc.structScan)
			}

			gotExpr, err := sb.Where(tc.whereExpressions...).
				Windowed(tc.windowEx).
				GroupBy(tc.groupByFields...).
				Having(tc.havingExpressions...).
				OrderBy(tc.orderbyExpressions...).
				Expression()

			if tc.normalizationNeeded {
				fmt.Println("normalized")
				gotExpr = normalizeSelectSQL(gotExpr)
				tc.wantExpr = normalizeSelectSQL(tc.wantExpr)
			}

			if tc.expectErr {
				assert.NotNil(t, err, "expected an error but got none")
				return
			} else {
				assert.Nil(t, err, "expected no error but got one")
			}
			if !tc.expectErr {
				assert.Equal(t, tc.wantExpr, gotExpr)
			}
		})
	}
}

func Test_SelectBuilder(t *testing.T) {
	testcases := []struct {
		name      string
		selectSQL SelectBuilder
		expected  string
		expectErr bool
	}{
		{
			name: "Simple SELECT",
			selectSQL: Select(F("table.column1")).
				From(Schema("table", TABLE)),
			expected:  "SELECT table.column1 FROM table;",
			expectErr: false,
		},
		{
			name: "SELECT with alias",
			selectSQL: Select(F("table.column1").As("alias1")).
				From(Schema("table", TABLE)),
			expected:  "SELECT table.column1 AS alias1 FROM table;",
			expectErr: false,
		},
		{
			name: "SELECT with WHERE clause",
			selectSQL: Select(F("table.column1")).
				From(Schema("table", TABLE)).
				Where(F("table.column1").Equal(1)),
			expected:  "SELECT table.column1 FROM table WHERE table.column1 = 1;",
			expectErr: false,
		},
		{
			name: "SELECT with EMIT CHANGES on stream",
			selectSQL: Select(F("stream.column1")).
				From(Schema("stream", STREAM)).
				EmitChanges(),
			expected:  "SELECT stream.column1 FROM stream EMIT CHANGES;",
			expectErr: false,
		},
		{
			name: "SELECT with EMIT CHANGES on table (invalid)",
			selectSQL: Select(F("table.column1")).
				From(Schema("table", TABLE)).
				EmitChanges(),
			expected:  "",
			expectErr: true,
		},
		{
			name: "SELECT with GROUP BY on stream without WINDOW (invalid)",
			selectSQL: Select(F("stream.column1")).
				From(Schema("stream", STREAM)).
				GroupBy(F("stream.column1")),
			expected:  "",
			expectErr: true,
		},
		{
			name: "SELECT with GROUP BY and WINDOW on stream",
			selectSQL: Select(F("stream.column1")).
				From(Schema("stream", STREAM)).
				GroupBy(F("stream.column1")).
				Windowed(NewTumblingWindow(TimeUnit{Val: 10, Unit: Seconds})),
			expected:  "SELECT stream.column1 FROM stream GROUP BY stream.column1 WINDOW TUMBLING (SIZE 10 SECONDS);",
			expectErr: false,
		},
		{
			name: "SELECT with HAVING without GROUP BY (invalid)",
			selectSQL: Select(F("table.column1")).
				From(Schema("table", TABLE)).
				Having(F("table.column1").Greater(1)),
			expected:  "",
			expectErr: true,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			expr, err := tc.selectSQL.Expression()
			assert.Equal(t, tc.expectErr, err != nil)
			if !tc.expectErr {
				assert.Equal(t, tc.expected, expr)
			}
		})
	}
}

func Test_SelectBuilderRelationStorage(t *testing.T) {
	previous := static.ReflectionFlag
	static.ReflectionFlag = true
	defer func() {
		static.ReflectionFlag = previous
	}()

	testcases := []struct {
		name                    string
		builder                 SelectBuilder
		expectedRelationStorage map[string]map[string]schema.SearchField
		expectedReturn          map[string]schema.SearchField
	}{
		{
			name: "Simple SELECT with table storage",
			builder: Select(F("table.column1")).
				From(Schema("table", TABLE)),
			expectedRelationStorage: map[string]map[string]schema.SearchField{
				"table": {
					"column1": {
						Name:     "column1",
						Relation: "table",
					},
				},
			},
			expectedReturn: map[string]schema.SearchField{
				"column1": {
					Name:     "column1",
					Relation: "table",
				},
			},
		},
		{
			name: "SELECT with multiple fields",
			builder: Select(F("table.column1"), F("table.column2")).
				From(Schema("table", TABLE)),
			expectedRelationStorage: map[string]map[string]schema.SearchField{
				"table": {
					"column1": {
						Name:     "column1",
						Relation: "table",
					},
					"column2": {
						Name:     "column2",
						Relation: "table",
					},
				},
			},
			expectedReturn: map[string]schema.SearchField{
				"column1": {
					Name:     "column1",
					Relation: "table",
				},
				"column2": {
					Name:     "column2",
					Relation: "table",
				},
			},
		},
		{
			name: "SELECT with JOIN",
			builder: Select(F("table1.column1"), F("table2.column2")).
				From(Schema("table1", TABLE)).
				Join(Schema("table2", TABLE), F("table1.id").Equal(F("table2.id"))),
			expectedRelationStorage: map[string]map[string]schema.SearchField{
				"table1": {
					"column1": schema.SearchField{
						Name:     "column1",
						Relation: "table1",
					},
					"id": schema.SearchField{
						Name:     "id",
						Relation: "table1",
					},
				},
				"table2": {
					"column2": schema.SearchField{
						Name:     "column2",
						Relation: "table2",
					},
					"id": schema.SearchField{
						Name:     "id",
						Relation: "table2",
					},
				},
			},
			expectedReturn: map[string]schema.SearchField{
				"column1": {
					Name:     "column1",
					Relation: "table1",
				},
				"column2": {
					Name:     "column2",
					Relation: "table2",
				},
			},
		},
		{
			name: "SELECT with GROUP BY",
			builder: Select(F("table.column1"), F("table.column2")).
				From(Schema("table", TABLE)).
				GroupBy(F("table.column1")),
			expectedRelationStorage: map[string]map[string]schema.SearchField{
				"table": {
					"column1": schema.SearchField{
						Name:     "column1",
						Relation: "table",
					},
					"column2": schema.SearchField{
						Name:     "column2",
						Relation: "table",
					},
				},
			},
			expectedReturn: map[string]schema.SearchField{
				"column1": {
					Name:     "column1",
					Relation: "table",
				},
				"column2": {
					Name:     "column2",
					Relation: "table",
				},
			},
		},
		{
			name: "SELECT with WHERE clause",
			builder: Select(F("table.column1")).
				From(Schema("table", TABLE)).
				Where(F("table.column1").Equal(1)),
			expectedRelationStorage: map[string]map[string]schema.SearchField{
				"table": {
					"column1": {
						Name:     "column1",
						Relation: "table",
					},
				},
			},
			expectedReturn: map[string]schema.SearchField{
				"column1": {
					Name:     "column1",
					Relation: "table",
				},
			},
		},
		{
			name: "SELECT with multiple JOINs, WHERE, GROUP BY, and HAVING",
			builder: Select(F("table1.column1"), F("table2.column2"), Count(F("table3.column3")).As("count_column3")).
				From(Schema("table1", TABLE)).
				Join(Schema("table2", TABLE), F("table1.id").Equal(F("table2.id"))).
				Join(Schema("table3", TABLE), F("table2.id").Equal(F("table3.id"))).
				Where(F("table1.column1").Greater(10), F("table2.column2").Less(50)).
				GroupBy(F("table1.column1"), F("table2.column2")).
				Having(F("count_column3").Greater(5)),
			expectedRelationStorage: map[string]map[string]schema.SearchField{
				"table1": {
					"column1": schema.SearchField{
						Name:     "column1",
						Relation: "table1",
					},
					"id": schema.SearchField{
						Name:     "id",
						Relation: "table1",
					},
				},
				"table2": {
					"column2": schema.SearchField{
						Name:     "column2",
						Relation: "table2",
					},
					"id": schema.SearchField{
						Name:     "id",
						Relation: "table2",
					},
				},
				"table3": {
					"id": schema.SearchField{
						Name:     "id",
						Relation: "table3",
					},
					"column3": schema.SearchField{
						Name:     "column3",
						Relation: "table3",
					},
				},
			},
			expectedReturn: map[string]schema.SearchField{
				"column1": {
					Name:     "column1",
					Relation: "table1",
				},
				"column2": {
					Name:     "column2",
					Relation: "table2",
				},
				"count_column3": {
					Name:     "count_column3",
					Relation: "",
				},
			},
		},
		{
			name: "SELECT with WINDOW, GROUP BY, and ORDER BY",
			builder: Select(F("stream.column1"), Count(F("stream.column2")).As("count_column2")).
				From(Schema("stream", STREAM)).
				GroupBy(F("stream.column1")).
				Windowed(NewTumblingWindow(TimeUnit{Val: 10, Unit: Seconds})).
				OrderBy(F("count_column2").Desc()),
			expectedRelationStorage: map[string]map[string]schema.SearchField{
				"stream": {
					"column1": schema.SearchField{
						Name:     "column1",
						Relation: "stream",
					},
					"column2": schema.SearchField{
						Name:     "column2",
						Relation: "stream",
					},
				},
			},

			expectedReturn: map[string]schema.SearchField{
				"column1": {
					Name:     "column1",
					Relation: "stream",
				},
				"count_column2": {
					Name:     "count_column2",
					Relation: "",
				},
			},
		},
		{
			name: "SELECT with many fields and multiple JOINs",
			builder: Select(
				F("t1.col1"), F("t1.col2"), F("t1.col3"), F("t1.col4"), F("t1.col5"),
				F("t2.col6"), F("t2.col7"), F("t2.col8"),
				F("t3.col9"), F("t3.col10")).
				From(Schema("t1", TABLE)).
				Join(Schema("t2", TABLE), F("t1.id").Equal(F("t2.t1_id"))).
				Join(Schema("t3", TABLE), F("t2.id").Equal(F("t3.t2_id"))),
			expectedRelationStorage: map[string]map[string]schema.SearchField{
				"t1": {
					"col1": {Name: "col1", Relation: "t1"},
					"col2": {Name: "col2", Relation: "t1"},
					"col3": {Name: "col3", Relation: "t1"},
					"col4": {Name: "col4", Relation: "t1"},
					"col5": {Name: "col5", Relation: "t1"},
					"id":   {Name: "id", Relation: "t1"},
				},
				"t2": {
					"col6":  {Name: "col6", Relation: "t2"},
					"col7":  {Name: "col7", Relation: "t2"},
					"col8":  {Name: "col8", Relation: "t2"},
					"t1_id": {Name: "t1_id", Relation: "t2"},
					"id":    {Name: "id", Relation: "t2"},
				},
				"t3": {
					"col9":  {Name: "col9", Relation: "t3"},
					"col10": {Name: "col10", Relation: "t3"},
					"t2_id": {Name: "t2_id", Relation: "t3"},
				},
			},
			expectedReturn: map[string]schema.SearchField{
				"col1":  {Name: "col1", Relation: "t1"},
				"col2":  {Name: "col2", Relation: "t1"},
				"col3":  {Name: "col3", Relation: "t1"},
				"col4":  {Name: "col4", Relation: "t1"},
				"col5":  {Name: "col5", Relation: "t1"},
				"col6":  {Name: "col6", Relation: "t2"},
				"col7":  {Name: "col7", Relation: "t2"},
				"col8":  {Name: "col8", Relation: "t2"},
				"col9":  {Name: "col9", Relation: "t3"},
				"col10": {Name: "col10", Relation: "t3"},
			},
		},
		{
			name: "Complex SELECT with aliases, aggregates, WHERE, GROUP BY, HAVING",
			builder: Select(
				F("sales.region"),
				Sum(F("sales.amount")).As("total_amount"),
				Count(F("sales.transaction_id")).As("transaction_count"),
				Avg(F("sales.discount")).As("avg_discount"),
				Max(F("sales.amount")).As("max_amount")).
				From(Schema("sales", TABLE)).
				Where(F("sales.amount").Greater(100)).
				GroupBy(F("sales.region")).
				Having(F("total_amount").Greater(10000)),
			expectedRelationStorage: map[string]map[string]schema.SearchField{
				"sales": {
					"region":         {Name: "region", Relation: "sales"},
					"amount":         {Name: "amount", Relation: "sales"},
					"transaction_id": {Name: "transaction_id", Relation: "sales"},
					"discount":       {Name: "discount", Relation: "sales"},
				},
			},
			expectedReturn: map[string]schema.SearchField{
				"region":            {Name: "region", Relation: "sales"},
				"total_amount":      {Name: "total_amount"},
				"transaction_count": {Name: "transaction_count"},
				"avg_discount":      {Name: "avg_discount"},
				"max_amount":        {Name: "max_amount"},
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			relationStorage := tc.builder.RelationReport()
			result := make(map[string]map[string]schema.SearchField)
			for relation, fields := range relationStorage {
				result[relation] = fields.Map()
			}
			assert.Equal(t, tc.expectedRelationStorage, result)

			returnRelation := tc.builder.Returns().Map()
			assert.Equal(t, tc.expectedReturn, returnRelation)

		})
	}

}
