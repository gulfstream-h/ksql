package ksql

type (
	DescribeBuilder interface {
		Expression() (string, bool)
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

func (d *describe) Expression() (string, bool) {
	var operation string

	switch d.typ {
	case STREAM:
		operation = "DESCRIBE "
	case TABLE:
		operation = "DESCRIBE "
	case TOPIC:
		operation = "DESCRIBE "
	default:
		return "", false
	}

	return operation + d.Schema() + ";", true
}
