package ksql

import (
	"ksql/schema"
	"reflect"
	"strings"
)

type (
	CreateBuilder interface {
		Expression() (string, bool)
		AsSelect(builder SelectBuilder) CreateBuilder
		SchemaFields(fields ...schema.SearchField) CreateBuilder
		SchemaFromStruct(schemaName string, schemaStruct any) CreateBuilder
		SchemaFromRemoteStruct(
			schemaName string,
			schemaStruct reflect.Type,
		) CreateBuilder
		With(metadata Metadata) CreateBuilder
		Type() Reference
		Schema() string
	}

	createBuilder struct {
		asSelect  SelectBuilder
		fields    []schema.SearchField
		reference Reference
		schema    string
		meta      Metadata
	}

	createBuilderRule = func(builder *createBuilder) bool
)

var (
	// 1. Cannot create a stream from a table.
	tableFromNotAggregatedStream = func(builder *createBuilder) bool {
		if builder.asSelect == nil {
			return true
		}
		return !(builder.reference == TABLE && builder.asSelect.Ref() == STREAM && !builder.asSelect.aggregated())
	}

	// 2. Cannot create a stream from a table.
	streamFromTable = func(builder *createBuilder) bool {
		if builder.asSelect == nil {
			return true
		}
		return !(builder.reference == STREAM && builder.asSelect.Ref() == TABLE)
	}

	// 3. Cannot create a table with a windowed select statement.
	tableFromWindowedStream = func(builder *createBuilder) bool {
		if builder.asSelect == nil {
			return true
		}
		return !(builder.reference == TABLE && builder.asSelect.windowed())
	}

	createRuleSet = []createBuilderRule{
		tableFromNotAggregatedStream,
		streamFromTable,
		tableFromWindowedStream,
	}
)

func Create(typ Reference, schema string) CreateBuilder {
	return &createBuilder{
		reference: typ,
		schema:    schema,
		meta:      Metadata{},
		asSelect:  nil,
	}
}

func (c *createBuilder) Type() Reference {
	return c.reference
}

func (c *createBuilder) Schema() string {
	return c.schema
}

func (c *createBuilder) With(meta Metadata) CreateBuilder {
	c.meta = meta
	return c
}

func (c *createBuilder) AsSelect(builder SelectBuilder) CreateBuilder {
	c.asSelect = builder
	return c
}

func (c *createBuilder) SchemaFields(
	fields ...schema.SearchField,
) CreateBuilder {
	c.fields = append(c.fields, fields...)
	return c
}

func (c *createBuilder) SchemaFromStruct(
	schemaName string,
	schemaStruct any,
) CreateBuilder {
	c.fields = append(c.fields, schema.ParseStructToFields(schemaName, schemaStruct)...)

	return c
}

func (c *createBuilder) SchemaFromRemoteStruct(
	schemaName string,
	schemaStruct reflect.Type,
) CreateBuilder {

	c.fields = append(c.fields, schema.ParseReflectStructToFields(schemaName, schemaStruct)...)
	return c
}

func (c *createBuilder) Expression() (string, bool) {
	builder := new(strings.Builder)

	// If there are no fields and no AS SELECT, we cannot build a valid CREATE statement.
	if len(c.fields) == 0 && c.asSelect == nil {
		return "", false
	}

	// Queries can only be built using AS SELECT or Field Enumeration.
	// They cannot be combined.
	if len(c.fields) > 0 && c.asSelect != nil {
		return "", false
	}

	switch c.reference {
	case STREAM:
		builder.WriteString("CREATE STREAM ")
	case TABLE:
		builder.WriteString("CREATE TABLE ")
	default:
		return "", false
	}

	if len(c.Schema()) == 0 {
		return "", false
	}

	for idx := range createRuleSet {
		if !createRuleSet[idx](c) {
			return "", false
		}
	}

	builder.WriteString(c.Schema())

	if len(c.fields) > 0 {
		builder.WriteString(" (")

		for idx := range c.fields {
			item := c.fields[idx]

			//if len(item.Relation) != 0 {
			//	builder.WriteString(item.Relation + ".")
			//}

			builder.WriteString(item.Name + " " + item.Kind.GetKafkaRepresentation())

			if item.Tag != "" && c.reference != STREAM {
				if item.Tag == "primary" {
					builder.WriteString(" PRIMARY KEY ")
				}
			}

			if idx != len(c.fields)-1 {
				builder.WriteString(", ")
			}
		}
		builder.WriteString(")")
	}

	metaExpression := c.meta.Expression()
	if len(metaExpression) > 0 {
		builder.WriteString(" ")
		builder.WriteString(metaExpression)
	}

	if c.asSelect != nil {
		expr, ok := c.asSelect.Expression()
		if !ok {
			return "", false
		}
		builder.WriteString(" AS ")
		builder.WriteString(expr)
		return builder.String(), true
	}

	builder.WriteString(";")

	return builder.String(), true
}
