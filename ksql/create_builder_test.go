package ksql

import (
	"github.com/gulfstream-h/ksql/internal/schema"
	"github.com/gulfstream-h/ksql/kinds"
	"github.com/stretchr/testify/assert"
	"regexp"
	"sort"
	"strings"
	"testing"
)

func normalizeCreateSQL(sql string) string {
	re := regexp.MustCompile(`(?i)CREATE (TABLE|STREAM) (\w+) \((.+?)\)(?: WITH \((.+?)\))?;`)
	matches := re.FindStringSubmatch(sql)
	if len(matches) < 3 {
		return sql
	}

	tableType := matches[1]
	tableName := matches[2]
	fields := strings.Split(strings.TrimSpace(matches[3]), ", ")

	sort.Strings(fields)
	options := ""
	if len(matches) > 4 && matches[4] != "" {
		options = " WITH (" + strings.TrimSpace(matches[4]) + ")"
	}

	return "CREATE " + tableType + " " + tableName + " (" + strings.Join(fields, ", ") + ")" + options + ";"
}

func Test_CreateSchemaMethods(t *testing.T) {
	testcases := []struct {
		name                string
		createSQL           CreateBuilder
		expected            string
		expectErr           bool
		normalizationNeeded bool
	}{
		{
			name: "Create Table with SchemaFields",
			createSQL: Create(TABLE, "table_name").
				SchemaFields(
					schema.SearchField{Name: "column1", Kind: kinds.String},
					schema.SearchField{Name: "column2", Kind: kinds.Int},
				),
			expected:  "CREATE TABLE table_name (column1 VARCHAR, column2 INT);",
			expectErr: false,
		},
		{
			name: "Create Table with SchemaFromStruct",
			createSQL: Create(TABLE, "table_name").
				SchemaFromStruct(struct {
					Column1 string `ksql:"column1"`
					Column2 int    `ksql:"column2"`
				}{}),
			expected:            "CREATE TABLE table_name (column1 VARCHAR, column2 INT);",
			expectErr:           false,
			normalizationNeeded: true,
		},
		{
			name: "Create Table with SchemaFields",
			createSQL: Create(TABLE, "table_name").
				SchemaFields(
					schema.SearchField{Name: "column1", Kind: kinds.String},
					schema.SearchField{Name: "column2", Kind: kinds.Int},
				),
			expected:  "CREATE TABLE table_name (column1 VARCHAR, column2 INT);",
			expectErr: false,
		},
		{
			name: "Create Table with SchemaFromStruct",
			createSQL: Create(TABLE, "table_name").
				SchemaFromStruct(struct {
					Column1 string `ksql:"column1"`
					Column2 int    `ksql:"column2"`
				}{}),
			expected:            "CREATE TABLE table_name (column1 VARCHAR, column2 INT);",
			expectErr:           false,
			normalizationNeeded: true,
		},
		{
			name:      "Create Table with empty SchemaFields",
			createSQL: Create(TABLE, "empty_table"),
			expected:  "",
			expectErr: true,
		},
		{
			name: "Create Stream with SchemaFields",
			createSQL: Create(STREAM, "stream_name").
				SchemaFields(
					schema.SearchField{Name: "column1", Kind: kinds.String},
					schema.SearchField{Name: "column2", Kind: kinds.Double},
				),
			expected:  "CREATE STREAM stream_name (column1 VARCHAR, column2 DOUBLE);",
			expectErr: false,
		},
		{
			name: "Create Table with metadata",
			createSQL: Create(TABLE, "table_name").
				With(Metadata{Topic: "value"}).
				SchemaFields(
					schema.SearchField{Name: "column1", Kind: kinds.String},
				),
			expected:  "CREATE TABLE table_name (column1 VARCHAR) WITH (KAFKA_TOPIC = 'value');",
			expectErr: false,
		},
		{
			name: "Create Stream with empty schema name",
			createSQL: Create(STREAM, "").
				SchemaFields(
					schema.SearchField{Name: "column1", Kind: kinds.String},
				),
			expected:  "",
			expectErr: true,
		},
		{
			name: "Create Table with multiple fields",
			createSQL: Create(TABLE, "table_name").
				SchemaFields(
					schema.SearchField{Name: "column1", Kind: kinds.String},
					schema.SearchField{Name: "column2", Kind: kinds.Int},
					schema.SearchField{Name: "column3", Kind: kinds.Double},
				),
			expected:  "CREATE TABLE table_name (column1 VARCHAR, column2 INT, column3 DOUBLE);",
			expectErr: false,
		},
		{
			name: "Create Stream with SchemaFromStruct",
			createSQL: Create(STREAM, "stream_name").
				SchemaFromStruct(struct {
					Column1 string  `ksql:"column1"`
					Column2 float64 `ksql:"column2"`
				}{}),
			expected:            "CREATE STREAM stream_name (column1 VARCHAR, column2 DOUBLE);",
			expectErr:           false,
			normalizationNeeded: true,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			expr, err := tc.createSQL.Expression()
			if tc.normalizationNeeded {
				expr = normalizeCreateSQL(expr)
				tc.expected = normalizeCreateSQL(tc.expected)
			}

			assert.Equal(t, tc.expectErr, err != nil)
			if !tc.expectErr {
				assert.Equal(t, tc.expected, expr)
			}
		})
	}
}

func Test_CreateAsSelectExpression(t *testing.T) {
	testcases := []struct {
		name      string
		createSQL CreateBuilder
		expected  string
		expectErr bool
	}{
		{
			name: "Create Stream with simple SELECT",
			createSQL: Create(TABLE, "stream_name").
				AsSelect(
					Select(F("table1.column1"), F("table1.column2")).
						From(Schema("table1", TABLE)).
						Where(F("table1.column1").Greater(10)),
				),
			expected:  "CREATE TABLE stream_name AS SELECT table1.column1, table1.column2 FROM table1 WHERE table1.column1 > 10;",
			expectErr: false,
		},
		{
			name: "Create Table with SELECT and JOIN",
			createSQL: Create(TABLE, "table_name").
				AsSelect(
					Select(F("table1.column1"), F("table2.column2")).
						From(Schema("table1", TABLE)).
						Join(Schema("table2", TABLE), F("table1.id").Equal(F("table2.id"))),
				),
			expected:  "CREATE TABLE table_name AS SELECT table1.column1, table2.column2 FROM table1 JOIN table2 ON table1.id = table2.id;",
			expectErr: false,
		},
		{
			name: "Create Stream with SELECT, WHERE, and ORDER BY",
			createSQL: Create(STREAM, "stream_name").
				AsSelect(
					Select(F("table1.column1"), F("table1.column2")).
						From(Schema("table1", STREAM)).
						Where(F("table1.column1").Greater(5)).
						Windowed(NewHoppingWindow(TimeUnit{Unit: Seconds, Val: 60}, TimeUnit{Unit: Seconds, Val: 30})).
						OrderBy(F("table1.column1").Asc()),
				),
			expected:  "CREATE STREAM stream_name AS SELECT table1.column1, table1.column2 FROM table1 WHERE table1.column1 > 5 WINDOW HOPPING (SIZE 60 SECONDS, ADVANCE BY 30 SECONDS) ORDER BY table1.column1 ASC;",
			expectErr: false,
		},
		{
			name: "Create Table with SELECT, GROUP BY, and HAVING",
			createSQL: Create(TABLE, "table_name").
				AsSelect(
					Select(F("table1.column1"), Count(F("table1.column2")).As("count_column2")).
						From(Schema("table1", TABLE)).
						GroupBy(F("table1.column1")).
						Having(Count(F("table1.column2")).Greater(1)),
				),
			expected:  "CREATE TABLE table_name AS SELECT table1.column1, COUNT(table1.column2) AS count_column2 FROM table1 GROUP BY table1.column1 HAVING COUNT(table1.column2) > 1;",
			expectErr: false,
		},
		{
			name: "Create Stream with complex SELECT",
			createSQL: Create(STREAM, "stream_name").
				AsSelect(
					Select(F("table1.column1"), F("table2.column2"), Avg(F("table3.column3")).As("avg_column3")).
						From(Schema("table1", STREAM)).
						Join(Schema("table2", TABLE), F("table1.id").Equal(F("table2.id"))).
						LeftJoin(Schema("table3", TABLE), F("table2.id").Equal(F("table3.id"))).
						Where(F("table1.column1").Greater(10)).
						Windowed(NewHoppingWindow(TimeUnit{Unit: Seconds, Val: 30}, TimeUnit{Unit: Seconds, Val: 15})).
						GroupBy(F("table1.column1")).
						Having(Avg(F("table3.column3")).Greater(50)).
						OrderBy(F("avg_column3").Desc()),
				),
			expected:  "CREATE STREAM stream_name AS SELECT table1.column1, table2.column2, AVG(table3.column3) AS avg_column3 FROM table1 JOIN table2 ON table1.id = table2.id LEFT JOIN table3 ON table2.id = table3.id WHERE table1.column1 > 10 GROUP BY table1.column1 WINDOW HOPPING (SIZE 30 SECONDS, ADVANCE BY 15 SECONDS) HAVING AVG(table3.column3) > 50 ORDER BY avg_column3 DESC;",
			expectErr: false,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			expr, err := tc.createSQL.Expression()
			assert.Equal(t, tc.expectErr, err != nil)
			if !tc.expectErr {
				assert.Equal(t, tc.expected, expr)
			}
		})
	}
}
