package ksql

import "fmt"

// FromExpression - common contract for all FROM expressions
type FromExpression interface {
	Expression

	Schema() string
	From(string) FromExpression
	As(alias string) FromExpression
	Ref() Reference
	Alias() string
}

// from implements the FromExpression interface, representing a FROM clause in KSQL
type from struct {
	schema string
	ref    Reference
	alias  string
}

// Schema creates a new FromExpression with the specified schema name and reference type
func Schema(schemaName string, reference Reference) FromExpression {
	return &from{
		schema: schemaName,
		ref:    reference,
		alias:  "",
	}
}

// NewFromExpression creates a new FromExpression with default values that should be later configured
func NewFromExpression() FromExpression {
	return &from{}
}

// Ref returns the reference type of the FROM expression.
func (f *from) Ref() Reference { return f.ref }

// Alias returns the alias of the FROM expression.
func (f *from) Alias() string {
	return f.alias
}

// As sets the alias for the FROM expression.
func (f *from) As(alias string) FromExpression {
	f.alias = alias
	return f
}

// Schema returns the schema of the FROM expression.
func (f *from) Schema() string {
	return f.schema
}

// From sets the schema for the FROM expression.
func (f *from) From(schema string) FromExpression {
	f.schema = schema
	return f
}

// Expression returns the KSQL expression for the FROM clause.
func (f *from) Expression() (string, error) {
	if len(f.schema) == 0 {
		return "", fmt.Errorf("schema cannot be empty")
	}

	if len(f.alias) != 0 {
		return fmt.Sprintf("FROM %s AS %s", f.schema, f.alias), nil
	}

	return fmt.Sprintf("FROM %s", f.schema), nil
}
