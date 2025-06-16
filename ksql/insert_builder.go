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
		InsertStruct(relationName string, val any) InsertBuilder
		Rows(rows ...Row) InsertBuilder
		Schema() string
		Reference() Reference
		Expression() (string, bool)
	}

	// Row represents a map of column names to their values for an insert operation.
	Row map[string]any

	insertBuilder struct {
		selectBuilder SelectBuilder
		schema        string
		ref           Reference

		columns map[string]struct{}
		vals    []map[string]string

		columnsNameSequence []string // to preserve order of insertion
	}
)

func Insert(ref Reference, schema string) InsertBuilder {
	return &insertBuilder{
		schema:              schema,
		columns:             make(map[string]struct{}),
		columnsNameSequence: make([]string, 0),
		vals:                make([]map[string]string, 0),
		ref:                 ref,
	}
}

func (i *insertBuilder) Reference() Reference {
	return i.ref
}

func (i *insertBuilder) Rows(rows ...Row) InsertBuilder {
	for _, row := range rows {
		if row == nil {
			continue
		}

		values := make(map[string]string, len(row))
		for k, v := range row {
			if _, ok := i.columns[k]; !ok {
				i.columnsNameSequence = append(i.columnsNameSequence, k)
				i.columns[k] = struct{}{}
			}

			values[k] = util.Serialize(v)
		}
		i.vals = append(i.vals, values)
	}
	return i
}

func (i *insertBuilder) AsSelect(selectBuilder SelectBuilder) InsertBuilder {
	i.selectBuilder = selectBuilder
	return i
}

func (i *insertBuilder) InsertStruct(relationName string, val any) InsertBuilder {
	fields := schema.ParseStructToFields(relationName, reflect.TypeOf(val))
	values := make(map[string]string, len(fields))
	for _, field := range fields {
		if field.Value == nil {
			continue
		}
		if _, ok := i.columns[field.Name]; !ok {
			i.columnsNameSequence = append(i.columnsNameSequence, field.Name)
			i.columns[field.Name] = struct{}{}
		}

		values[field.Name] = *field.Value
	}
	i.vals = append(i.vals, values)
	return i
}

func (i *insertBuilder) Schema() string {
	return i.schema
}

func (i *insertBuilder) Expression() (string, bool) {
	if len(i.columns) == 0 && i.selectBuilder == nil {
		return "", false
	}

	if len(i.columns) != 0 && i.selectBuilder != nil {
		return "", false
	}

	builder := new(strings.Builder)
	builder.WriteString("INSERT INTO ")
	builder.WriteString(i.schema)

	if len(i.columns) != 0 {
		builder.WriteString(" (")
		builder.WriteString(strings.Join(i.columnsNameSequence, ", "))
		builder.WriteString(")")
		builder.WriteString(" VALUES ")

		vals := make([]string, 0, len(i.columns))

		for idx, v := range i.vals {
			for _, col := range i.columnsNameSequence {
				if _, ok := v[col]; !ok {
					return "", false
				}
				vals = append(vals, v[col])
			}
			builder.WriteString("(")
			builder.WriteString(strings.Join(vals, ", "))
			builder.WriteString(")")
			if idx != len(i.vals)-1 {
				builder.WriteString(", ")
			}
		}
	}

	if i.selectBuilder != nil {
		expr, ok := i.selectBuilder.Expression()
		if !ok {
			return "", false
		}
		builder.WriteString(" " + expr)
		return builder.String(), true
	}

	builder.WriteString(";")

	return builder.String(), true
}
