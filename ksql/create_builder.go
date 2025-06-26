package ksql

import (
	"errors"
	"fmt"
	"ksql/schema"
	"strings"
)

type (
	CreateBuilder interface {
		Expression() (string, error)
		AsSelect(builder SelectBuilder) CreateBuilder
		SchemaFields(fields ...schema.SearchField) CreateBuilder
		SchemaFromStruct(schemaName string, schemaStruct any) CreateBuilder
		SchemaFromRemoteStruct(fields schema.LintedFields) CreateBuilder
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

	createBuilderRule struct {
		ruleFn      func(builder *createBuilder) bool
		description string
	}
)

var (
	// 1. Cannot create a stream from a table.
	tableFromNotAggregatedStream = createBuilderRule{
		ruleFn: func(builder *createBuilder) bool {
			if builder.asSelect == nil {
				return true
			}
			return !(builder.reference == TABLE && builder.asSelect.Ref() == STREAM && !builder.asSelect.aggregated())
		},
		description: "Cannot create a table from a non-aggregated stream",
	}

	// 2. Cannot create a stream from a table.
	streamFromTable = createBuilderRule{
		ruleFn: func(builder *createBuilder) bool {
			if builder.asSelect == nil {
				return true
			}
			return !(builder.reference == STREAM && builder.asSelect.Ref() == TABLE)
		},
		description: "Cannot create a stream from a table",
	}

	// 3. Cannot create a table with a windowed select statement.
	tableFromWindowedStream = createBuilderRule{
		ruleFn: func(builder *createBuilder) bool {
			if builder.asSelect == nil {
				return true
			}
			return !(builder.reference == TABLE && builder.asSelect.windowed())
		},
		description: "Cannot create a table from a windowed stream",
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
	fields, err := schema.NativeStructRepresentation(schemaStruct)
	if err != nil {
		return c
	}

	fieldsList := fields.Array()

	c.fields = append(c.fields, fieldsList...)

	return c
}

func (c *createBuilder) SchemaFromRemoteStruct(
	fields schema.LintedFields,
) CreateBuilder {
	fieldsList := fields.Array()

	c.fields = append(c.fields, fieldsList...)
	return c
}

func (c *createBuilder) Expression() (string, error) {
	builder := new(strings.Builder)

	// If there are no fields and no AS SELECT, we cannot build a valid CREATE statement.
	if len(c.fields) == 0 && c.asSelect == nil {
		return "", fmt.Errorf("invalid create statement: no fields or AS SELECT provided")
	}

	// Queries can only be built using AS SELECT or Field Enumeration.
	// They cannot be combined.
	if len(c.fields) > 0 && c.asSelect != nil {
		return "", fmt.Errorf("invalid create statement: cannot use both fields and AS SELECT")
	}

	switch c.reference {
	case STREAM:
		builder.WriteString("CREATE STREAM ")
	case TABLE:
		builder.WriteString("CREATE TABLE ")
	default:
		return "", errors.New("invalid create statement: unsupported reference type")
	}

	if len(c.Schema()) == 0 {
		return "", fmt.Errorf("invalid create statement: schema name cannot be empty")
	}

	for idx := range createRuleSet {
		if !createRuleSet[idx].ruleFn(c) {
			return "", fmt.Errorf("invalid create statement: %s", createRuleSet[idx].description)
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
				if strings.Contains(item.Tag, "primary") {
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
		expr, err := c.asSelect.Expression()
		if err != nil {
			return "", fmt.Errorf("AS SELECT expression: %w", err)
		}
		builder.WriteString(" AS ")
		builder.WriteString(expr)
		return builder.String(), nil
	}

	builder.WriteString(";")

	return builder.String(), nil
}
