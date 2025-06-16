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

	create struct {
		asSelect SelectBuilder
		fields   []schema.SearchField
		typ      Reference
		schema   string
		meta     Metadata
	}
)

func Create(typ Reference, schema string) CreateBuilder {
	return &create{
		typ:      typ,
		schema:   schema,
		meta:     Metadata{},
		asSelect: nil,
	}
}

func (c *create) Type() Reference {
	return c.typ
}

func (c *create) Schema() string {
	return c.schema
}

func (c *create) With(meta Metadata) CreateBuilder {
	c.meta = meta
	return c
}

func (c *create) AsSelect(builder SelectBuilder) CreateBuilder {
	c.asSelect = builder
	return c
}

func (c *create) SchemaFields(
	fields ...schema.SearchField,
) CreateBuilder {
	c.fields = append(c.fields, fields...)
	return c
}

func (c *create) SchemaFromStruct(
	schemaName string,
	schemaStruct any,
) CreateBuilder {
	c.fields = append(c.fields, schema.ParseStructToFields(schemaName, schemaStruct)...)

	return c
}

func (c *create) SchemaFromRemoteStruct(
	schemaName string,
	schemaStruct reflect.Type,
) CreateBuilder {

	c.fields = append(c.fields, schema.ParseReflectStructToFields(schemaName, schemaStruct)...)
	return c
}

func (c *create) Expression() (string, bool) {
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

	switch c.typ {
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

	builder.WriteString(c.Schema())

	if len(c.fields) > 0 {
		builder.WriteString(" (")

		for idx := range c.fields {
			item := c.fields[idx]

			//if len(item.Relation) != 0 {
			//	builder.WriteString(item.Relation + ".")
			//}

			builder.WriteString(item.Name + " " + item.Kind.GetKafkaRepresentation())

			if item.Tag != "" && c.typ != STREAM {
				if item.Tag == "PRIMARYKEY" {
					builder.WriteString(" " + item.Tag + " ")
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
