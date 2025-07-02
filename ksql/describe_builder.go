package ksql

import (
	"errors"
)

type (
	// DescribeBuilder - common contract for all DESCRIBE expressions
	DescribeBuilder interface {
		Expression() (string, error)
		Type() Reference
		Schema() string
	}

	// describe - base implementation of the DescribeBuilder interface
	describe struct {
		typ    Reference
		schema string
	}
)

// Describe creates a new DescribeBuilder for describing a stream, table, or topic
func Describe(typ Reference, schema string) DescribeBuilder {
	return &describe{
		typ:    typ,
		schema: schema,
	}
}

// Type returns the type of the reference being described, such as STREAM, TABLE, or TOPIC
func (d *describe) Type() Reference {
	return d.typ
}

// Schema returns the schema of the stream, table, or topic being described
func (d *describe) Schema() string {
	return d.schema
}

// Expression returns the KSQL expression for describing a stream, table, or topic
func (d *describe) Expression() (string, error) {
	var operation string

	switch d.typ {
	case STREAM:
		operation = "DESCRIBE "
	case TABLE:
		operation = "DESCRIBE "
	case TOPIC:
		operation = "DESCRIBE "
	default:
		return "", errors.New("unsupported reference type for describe operation")
	}

	return operation + d.Schema() + ";", nil
}
