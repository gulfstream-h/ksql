package ksql

import (
	"errors"
	"fmt"
	"github.com/gulfstream-h/ksql/internal/schema"
	"strings"
)

type (
	// CreateBuilder - contract for building CREATE statements in KSQL
	CreateBuilder interface {
		Expression() (string, error)
		AsSelect(builder SelectBuilder) CreateBuilder
		SchemaFields(fields ...schema.SearchField) CreateBuilder
		SchemaFromStruct(schemaStruct any) CreateBuilder
		With(metadata Metadata) CreateBuilder
		Type() Reference
		Schema() string
	}

	// createBuilderCtx holds the context for the create builder, including any errors encountered during construction
	createBuilderCtx struct {
		err error
	}

	// createBuilder implements the CreateBuilder interface for constructing CREATE statements
	createBuilder struct {
		ctx       createBuilderCtx
		asSelect  SelectBuilder
		fields    []schema.SearchField
		reference Reference
		schema    string
		meta      Metadata
	}

	// createBuilderRule defines a rule for validating create statements.
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

	// createRuleSet contains the rules for validating create statements.
	createRuleSet = []createBuilderRule{
		tableFromNotAggregatedStream,
		streamFromTable,
		tableFromWindowedStream,
	}
)

// Create initializes a new CreateBuilder for creating streams or tables in KSQL.
func Create(typ Reference, schema string) CreateBuilder {
	return &createBuilder{
		reference: typ,
		schema:    schema,
		meta:      Metadata{},
		asSelect:  nil,
	}
}

// Type returns the reference type of the create operation, such as STREAM or TABLE.
func (c *createBuilder) Type() Reference {
	return c.reference
}

// Schema returns the schema name for the create operation.
func (c *createBuilder) Schema() string {
	return c.schema
}

// With sets the metadata for the create operation, allowing additional properties to be specified.
func (c *createBuilder) With(meta Metadata) CreateBuilder {
	c.meta = meta
	return c
}

// AsSelect sets the select builder for the create operation, allowing the creation of a stream or table from a SELECT statement.
func (c *createBuilder) AsSelect(builder SelectBuilder) CreateBuilder {
	c.asSelect = builder
	return c
}

// SchemaFields appends one or more fields to the create builder.
func (c *createBuilder) SchemaFields(
	fields ...schema.SearchField,
) CreateBuilder {
	c.fields = append(c.fields, fields...)
	return c
}

// SchemaFromStruct takes a struct and appends its fields to the create builder.
func (c *createBuilder) SchemaFromStruct(
	schemaStruct any,
) CreateBuilder {
	fields, err := schema.NativeStructRepresentation(c.schema, schemaStruct)
	if err != nil {
		c.ctx.err = fmt.Errorf("cannot get fields from struct %T: %w", schemaStruct, err)
		return c
	}
	fieldsList := fields.Array()

	c.fields = append(c.fields, fieldsList...)

	return c
}

// Expression builds the CREATE statement based on the provided fields, AS SELECT, and metadata.
func (c *createBuilder) Expression() (string, error) {
	builder := new(strings.Builder)

	if c.ctx.err != nil {
		return "", c.ctx.err
	}

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

			builder.WriteString(item.Name + " " + item.Kind.GetKafkaRepresentation())

			if c.reference != STREAM && item.IsPrimary {
				builder.WriteString(" PRIMARY KEY ")
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
