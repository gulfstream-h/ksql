package ksql

import "fmt"

type FromExpression interface {
	Schema() string
	From(string) FromExpression
	Expression() (string, error)
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

func (f *from) Expression() (string, error) {
	if len(f.schema) == 0 {
		return "", fmt.Errorf("schema cannot be empty")
	}

	return fmt.Sprintf("FROM %s", f.schema), nil
}
