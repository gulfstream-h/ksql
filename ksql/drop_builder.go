package ksql

import "fmt"

type (
	DropBuilder interface {
		Expression() (string, error)
		Schema() string
	}

	drop struct {
		schema string
		typ    Reference
	}
)

func Drop(typ Reference, schema string) DropBuilder {
	return &drop{
		typ:    typ,
		schema: schema,
	}
}

func (d *drop) Schema() string {
	return d.schema
}

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
