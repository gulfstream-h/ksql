package ksql

import (
	"ksql/schema"
	"ksql/util"
	"reflect"
	"strings"
)

type (
	InsertBuilder interface {
		AsSelect(selectBuilder SelectBuilder) InsertBuilder
		InsertStruct(val any) InsertBuilder
		Rows(rows Rows) InsertBuilder
		Schema() string
		Reference() Reference
		Expression() (string, bool)
	}

	// Rows represents a map of column names to their values for an insert operation.
	Rows map[string]any

	insertBuilder struct {
		selectBuilder SelectBuilder
		schema        string
		ref           Reference
		rows          Rows
	}
)

func Insert(ref Reference, schema string) InsertBuilder {
	return &insertBuilder{
		schema: schema,
		rows:   make(Rows),
		ref:    ref,
	}
}

func (i *insertBuilder) Reference() Reference {
	return i.ref
}

func (i *insertBuilder) Rows(rows Rows) InsertBuilder {
	i.rows = rows
	for k, v := range i.rows {
		if v == nil {
			delete(i.rows, k)
			continue
		}
		i.rows[k] = v
	}
	return i
}

func (i *insertBuilder) AsSelect(selectBuilder SelectBuilder) InsertBuilder {
	i.selectBuilder = selectBuilder
	return i
}

func (i *insertBuilder) InsertStruct(val any) InsertBuilder {
	t := reflect.TypeOf(val)
	fields := schema.ParseStructToFields(t.Name(), t)
	for _, field := range fields {
		if field.Value == nil {
			continue
		}
		i.rows[field.Name] = field.Value
	}
	return i
}

func (i *insertBuilder) Schema() string {
	return i.schema
}

func (i *insertBuilder) Expression() (string, bool) {
	if len(i.rows) == 0 {
		return "", false
	}

	builder := new(strings.Builder)

	rels := make([]string, 0, len(i.rows))
	vals := make([]string, 0, len(i.rows))
	for k, v := range i.rows {
		rels = append(rels, k)
		vals = append(vals, util.Serialize(v))
	}

	builder.WriteString("INSERT INTO ")
	builder.WriteString(i.schema)
	builder.WriteString(" (")
	builder.WriteString(strings.Join(rels, ", "))
	builder.WriteString(") VALUES (")
	builder.WriteString(strings.Join(vals, ", "))
	builder.WriteString(");")

	return builder.String(), true
}
