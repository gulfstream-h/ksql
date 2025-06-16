package ksql

import (
	"github.com/stretchr/testify/assert"
	"ksql/kinds"
	"ksql/schema"
	"testing"
)

func Test_CreateSchemaMethods(t *testing.T) {
	testcases := []struct {
		name      string
		createSQL CreateBuilder
		expected  string
		wantOk    bool
	}{
		{
			name: "Create Table with SchemaFields",
			createSQL: Create(TABLE, "table_name").
				SchemaFields(
					schema.SearchField{Name: "column1", Kind: kinds.String},
					schema.SearchField{Name: "column2", Kind: kinds.Int},
				),
			expected: "CREATE TABLE table_name (column1 VARCHAR, column2 INT);",
			wantOk:   true,
		},
		{
			name: "Create Table with SchemaFromStruct",
			createSQL: Create(TABLE, "table_name").
				SchemaFromStruct("table_name", struct {
					Column1 string `ksql:"column1"`
					Column2 int    `ksql:"column2"`
				}{}),
			expected: "CREATE TABLE table_name (column1 VARCHAR, column2 INT);",
			wantOk:   true,
		},
		{
			name: "Create Table with SchemaFields",
			createSQL: Create(TABLE, "table_name").
				SchemaFields(
					schema.SearchField{Name: "column1", Kind: kinds.String},
					schema.SearchField{Name: "column2", Kind: kinds.Int},
				),
			expected: "CREATE TABLE table_name (column1 VARCHAR, column2 INT);",
			wantOk:   true,
		},
		{
			name: "Create Table with SchemaFromStruct",
			createSQL: Create(TABLE, "table_name").
				SchemaFromStruct("table_name", struct {
					Column1 string `ksql:"column1"`
					Column2 int    `ksql:"column2"`
				}{}),
			expected: "CREATE TABLE table_name (column1 VARCHAR, column2 INT);",
			wantOk:   true,
		},
		{
			name:      "Create Table with empty SchemaFields",
			createSQL: Create(TABLE, "empty_table"),
			expected:  "",
			wantOk:    false,
		},
		{
			name: "Create Stream with SchemaFields",
			createSQL: Create(STREAM, "stream_name").
				SchemaFields(
					schema.SearchField{Name: "column1", Kind: kinds.String},
					schema.SearchField{Name: "column2", Kind: kinds.Float},
				),
			expected: "CREATE STREAM stream_name (column1 VARCHAR, column2 FLOAT);",
			wantOk:   true,
		},
		{
			name: "Create Table with metadata",
			createSQL: Create(TABLE, "table_name").
				With(Metadata{Topic: "value"}).
				SchemaFields(
					schema.SearchField{Name: "column1", Kind: kinds.String},
				),
			expected: "CREATE TABLE table_name (column1 VARCHAR) WITH (\n  KAFKA_TOPIC = 'value'\n);",
			wantOk:   true,
		},
		{
			name: "Create Stream with empty schema name",
			createSQL: Create(STREAM, "").
				SchemaFields(
					schema.SearchField{Name: "column1", Kind: kinds.String},
				),
			expected: "",
			wantOk:   false,
		},
		{
			name: "Create Table with multiple fields",
			createSQL: Create(TABLE, "table_name").
				SchemaFields(
					schema.SearchField{Name: "column1", Kind: kinds.String},
					schema.SearchField{Name: "column2", Kind: kinds.Int},
					schema.SearchField{Name: "column3", Kind: kinds.Float},
				),
			expected: "CREATE TABLE table_name (column1 VARCHAR, column2 INT, column3 FLOAT);",
			wantOk:   true,
		},
		{
			name: "Create Stream with SchemaFromStruct",
			createSQL: Create(STREAM, "stream_name").
				SchemaFromStruct("stream_name", struct {
					Column1 string  `ksql:"column1"`
					Column2 float64 `ksql:"column2"`
				}{}),
			expected: "CREATE STREAM stream_name (column1 VARCHAR, column2 FLOAT);",
			wantOk:   true,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			expr, ok := tc.createSQL.Expression()
			assert.Equal(t, tc.wantOk, ok)
			if ok {
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
		wantOk    bool
	}{
		{
			name: "Create Stream with simple SELECT",
			createSQL: Create(STREAM, "stream_name").
				AsSelect(
					Select(F("table1.column1"), F("table1.column2")).
						From("table1").
						Where(F("table1.column1").Greater(10)),
				),
			expected: "CREATE STREAM stream_name AS SELECT table1.column1, table1.column2 FROM table1 WHERE table1.column1 > 10;",
			wantOk:   true,
		},
		{
			name: "Create Table with SELECT and JOIN",
			createSQL: Create(TABLE, "table_name").
				AsSelect(
					Select(F("table1.column1"), F("table2.column2")).
						From("table1").
						Join("table2", F("table1.id").Equal(F("table2.id"))),
				),
			expected: "CREATE TABLE table_name AS SELECT table1.column1, table2.column2 FROM table1 JOIN table2 ON table1.id = table2.id;",
			wantOk:   true,
		},
		{
			name: "Create Stream with SELECT, WHERE, and ORDER BY",
			createSQL: Create(STREAM, "stream_name").
				AsSelect(
					Select(F("table1.column1"), F("table1.column2")).
						From("table1").
						Where(F("table1.column1").Greater(5)).
						OrderBy(F("table1.column1").Asc()),
				),
			expected: "CREATE STREAM stream_name AS SELECT table1.column1, table1.column2 FROM table1 WHERE table1.column1 > 5 ORDER BY table1.column1 ASC;",
			wantOk:   true,
		},
		{
			name: "Create Table with SELECT, GROUP BY, and HAVING",
			createSQL: Create(TABLE, "table_name").
				AsSelect(
					Select(F("table1.column1"), Count(F("table1.column2")).As("count_column2")).
						From("table1").
						GroupBy(F("table1.column1")).
						Having(Count(F("table1.column2")).Greater(1)),
				),
			expected: "CREATE TABLE table_name AS SELECT table1.column1, COUNT(table1.column2) AS count_column2 FROM table1 GROUP BY table1.column1 HAVING COUNT(table1.column2) > 1;",
			wantOk:   true,
		},
		{
			name: "Create Stream with complex SELECT",
			createSQL: Create(STREAM, "stream_name").
				AsSelect(
					Select(F("table1.column1"), F("table2.column2"), Avg(F("table3.column3")).As("avg_column3")).
						From("table1").
						Join("table2", F("table1.id").Equal(F("table2.id"))).
						LeftJoin("table3", F("table2.id").Equal(F("table3.id"))).
						Where(F("table1.column1").Greater(10)).
						GroupBy(F("table1.column1")).
						Having(Avg(F("table3.column3")).Greater(50)).
						OrderBy(F("avg_column3").Desc()),
				),
			expected: "CREATE STREAM stream_name AS SELECT table1.column1, table2.column2, AVG(table3.column3) AS avg_column3 FROM table1 JOIN table2 ON table1.id = table2.id LEFT JOIN table3 ON table2.id = table3.id WHERE table1.column1 > 10 GROUP BY table1.column1 HAVING AVG(table3.column3) > 50 ORDER BY avg_column3 DESC;",
			wantOk:   true,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			expr, ok := tc.createSQL.Expression()
			assert.Equal(t, tc.wantOk, ok)
			if ok {
				assert.Equal(t, tc.expected, expr)
			}
		})
	}
}
