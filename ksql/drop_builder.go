package ksql

import "fmt"

type (
	// DropBuilder - common contract for all DROP expressions
	DropBuilder interface {
		Expression() (string, error)
		Schema() string
	}

	// drop - base implementation of the DropBuilder interface
	drop struct {
		schema string
		typ    Reference
	}
)

// Drop creates a new DropBuilder for dropping a stream, table, or topic
func Drop(typ Reference, schema string) DropBuilder {
	return &drop{
		typ:    typ,
		schema: schema,
	}
}

// Schema returns the schema of the stream, table, or topic being dropped
func (d *drop) Schema() string {
	return d.schema
}

// Expression returns the KSQL expression for dropping a stream, table, or topic
func (d *drop) Expression() (string, error) {
	var operation string

	switch d.typ {
	case STREAM:
		operation = "DROP STREAM "
	case TABLE:
		operation = "DROP TABLE "
	case TOPIC:
		operation = "DROP TOPIC "
	default:
		return "", fmt.Errorf("unsupported reference type")
	}

	return operation + d.Schema() + ";", nil
}
