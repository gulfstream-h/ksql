package ksql

import "fmt"

type FromExpression interface {
	Schema() string
	From(string) FromExpression
	Expression() string
}

type from struct {
	schema string
}

func NewFromExpression() FromExpression {
	return &from{}
}

func (f *from) Schema() string {
	return f.schema
}

func (f *from) From(schema string) FromExpression {
	f.schema = schema
	return f
}

func (f *from) Expression() string {
	if len(f.schema) == 0 {
		return ""
	}

	return fmt.Sprintf("FROM %s", f.schema)
}
