package ksql

import "fmt"

type FromExpression interface {
	Schema() string
	From(string) FromExpression
	As(alias string) FromExpression
	Alias() string
	Expression() (string, error)
}

type from struct {
	schema string
	alias  string
}

func NewFromExpression() FromExpression {
	return &from{}
}

func (f *from) Alias() string {
	return f.alias
}

func (f *from) As(alias string) FromExpression {
	f.alias = alias
	return f
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
