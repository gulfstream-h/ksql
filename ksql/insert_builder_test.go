package ksql

import (
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func normalizeSQL(sql string) string {
	re := regexp.MustCompile(`(?i)INSERT INTO (\w+) \((.+?)\) VALUES \((.+?)\);`)
	matches := re.FindStringSubmatch(sql)
	if len(matches) != 4 {
		return sql
	}

	table := matches[1]
	columns := strings.Split(matches[2], ",")
	values := strings.Split(matches[3], ",")

	if len(columns) != len(values) {
		return sql
	}

	for i := range columns {
		columns[i] = strings.TrimSpace(columns[i])
		values[i] = strings.TrimSpace(values[i])
	}

	pairs := make([]string, len(columns))
	for i := range columns {
		pairs[i] = columns[i] + "=" + values[i]
	}
	sort.Strings(pairs)

	for i := range pairs {
		parts := strings.SplitN(pairs[i], "=", 2)
		columns[i] = parts[0]
		values[i] = parts[1]
	}

	return "INSERT INTO " + table + " (" + strings.Join(columns, ", ") + ") VALUES (" + strings.Join(values, ", ") + ");"
}

func Test_InsertExpression(t *testing.T) {
	type example struct {
		ID   int    `ksql:"id"`
		Name string `ksql:"name"`
		Age  string `ksql:"age"`
	}

	testcases := []struct {
		name      string
		fields    Row
		structRow []any
		expected  string
		expectErr bool
	}{
		{
			name: "Insert with fields",
			fields: Row{
				"id":   123,
				"name": "John",
				"age":  "30",
			},
			expected:  "INSERT INTO table_name (id, name, age) VALUES (123, 'John', '30');",
			expectErr: false,
		},
		{
			name:      "Insert with empty fields",
			fields:    Row{},
			expected:  "",
			expectErr: true,
		},
		{
			name: "Insert with struct",
			structRow: []any{
				example{
					ID:   35,
					Name: "JJJ",
					Age:  "32",
				},
			},
			expected:  "INSERT INTO table_name (id, name, age) VALUES (35, 'JJJ', '32');",
			expectErr: false,
		},
		{
			name: "Insert with nil value",
			fields: Row{
				"id":   1,
				"name": nil,
			},
			expected:  "INSERT INTO table_name (id, name) VALUES (1, NULL);",
			expectErr: false,
		},
		{
			name: "Insert with numeric and string mix",
			fields: Row{
				"id":   42,
				"name": "Alice",
			},
			expected:  "INSERT INTO table_name (id, name) VALUES (42, 'Alice');",
			expectErr: false,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			builder := Insert(TABLE, "table_name").Rows(tc.fields)
			for _, str := range tc.structRow {
				builder = builder.InsertStruct("", str)
			}

			expr, err := builder.Expression()
			assert.Equal(t, tc.expectErr, err != nil)
			if !tc.expectErr {
				assert.Equal(t, normalizeSQL(tc.expected), normalizeSQL(expr))
			}
		})
	}
}

func Test_InsertAsSelectExpression(t *testing.T) {
	testcases := []struct {
		name      string
		selectSQL SelectBuilder
		expected  string
		expectErr bool
	}{
		{
			name: "Insert with simple SELECT",
			selectSQL: Select(F("table1.column1"), F("table1.column2")).
				From("table1", TABLE).
				Where(F("table1.column1").Greater(10)),
			expected:  "INSERT INTO table_name SELECT table1.column1, table1.column2 FROM table1 WHERE table1.column1 > 10;",
			expectErr: false,
		},
		{
			name: "Insert with SELECT and JOIN",
			selectSQL: Select(F("table1.column1"), F("table2.column2")).
				From("table1", TABLE).
				Join("table2", F("table1.id").Equal(F("table2.id"))),
			expected:  "INSERT INTO table_name SELECT table1.column1, table2.column2 FROM table1 JOIN table2 ON table1.id = table2.id;",
			expectErr: false,
		},
		{
			name: "Insert with SELECT, WHERE, and ORDER BY",
			selectSQL: Select(F("table1.column1"), F("table1.column2")).
				From("table1", TABLE).
				Where(F("table1.column1").Greater(5)).
				OrderBy(F("table1.column1").Asc()),
			expected:  "INSERT INTO table_name SELECT table1.column1, table1.column2 FROM table1 WHERE table1.column1 > 5 ORDER BY table1.column1 ASC;",
			expectErr: false,
		},
		{
			name: "Insert with SELECT, GROUP BY, and HAVING",
			selectSQL: Select(F("table1.column1"), Count(F("table1.column2")).As("count_column2")).
				From("table1", TABLE).
				GroupBy(F("table1.column1")).
				Having(Count(F("table1.column2")).Greater(1)),
			expected:  "INSERT INTO table_name SELECT table1.column1, COUNT(table1.column2) AS count_column2 FROM table1 GROUP BY table1.column1 HAVING COUNT(table1.column2) > 1;",
			expectErr: false,
		},
		{
			name: "Insert with complex SELECT",
			selectSQL: Select(F("table1.column1"), F("table2.column2"), Avg(F("table3.column3")).As("avg_column3")).
				From("table1", TABLE).
				Join("table2", F("table1.id").Equal(F("table2.id"))).
				LeftJoin("table3", F("table2.id").Equal(F("table3.id"))).
				Where(F("table1.column1").Greater(10)).
				GroupBy(F("table1.column1")).
				Having(Avg(F("table3.column3")).Greater(50)).
				OrderBy(F("avg_column3").Desc()),
			expected:  "INSERT INTO table_name SELECT table1.column1, table2.column2, AVG(table3.column3) AS avg_column3 FROM table1 JOIN table2 ON table1.id = table2.id LEFT JOIN table3 ON table2.id = table3.id WHERE table1.column1 > 10 GROUP BY table1.column1 HAVING AVG(table3.column3) > 50 ORDER BY avg_column3 DESC;",
			expectErr: false,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			builder := Insert(TABLE, "table_name").AsSelect(tc.selectSQL)

			expr, err := builder.Expression()
			assert.Equal(t, tc.expectErr, err != nil)
			if !tc.expectErr {
				assert.Equal(t, tc.expected, expr)
			}
		})
	}
}
