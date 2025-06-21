package ksql

import (
	"errors"
)

type (
	DescribeBuilder interface {
		Expression() (string, error)
		Type() Reference
		Schema() string
	}

	describe struct {
		typ    Reference
		schema string
	}
)

func Describe(typ Reference, schema string) DescribeBuilder {
	return &describe{
		typ:    typ,
		schema: schema,
	}
}

func (d *describe) Type() Reference {
	return d.typ
}

func (d *describe) Schema() string {
	return d.schema
}

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
