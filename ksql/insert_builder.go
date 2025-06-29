package ksql

import (
	"errors"
	"fmt"
	"ksql/schema"
	"ksql/util"
	"strings"
)

type (
	InsertBuilder interface {
		AsSelect(selectBuilder SelectBuilder) InsertBuilder
		InsertStruct(val any) InsertBuilder
		Rows(rows ...Row) InsertBuilder
		Schema() string
		Reference() Reference
		Expression() (string, error)
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

func (i *insertBuilder) InsertStruct(val any) InsertBuilder {
	fields, err := schema.NativeStructRepresentation(val)
	if err != nil {
		return nil
	}

	fieldsList := fields.Array()
	values := make(map[string]string, len(fieldsList))
	for _, field := range fieldsList {
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

func (i *insertBuilder) Expression() (string, error) {
	if len(i.columns) == 0 && i.selectBuilder == nil {
		return "", errors.New("cannot create INSERT expression with no columns or select statement")
	}

	if len(i.columns) != 0 && i.selectBuilder != nil {
		return "", errors.New("cannot create INSERT expression with both columns and select statement")
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
					return "", errors.New("missing value for column: " + col)
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
		expr, err := i.selectBuilder.Expression()
		if err != nil {
			return "", fmt.Errorf("select expression: %w", err)
		}
		builder.WriteString(" " + expr)
		return builder.String(), nil
	}

	builder.WriteString(";")

	return builder.String(), nil
}
