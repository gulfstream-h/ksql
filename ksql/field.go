package ksql

import (
	"fmt"
	"strings"
)

type Field interface {
	Comparable
	Ordered
	Nullable
	ComparableArray

	Schema() string
	Column() string
	Alias() string
	As(alias string) Field
	Copy() Field
	Expression() (string, bool)
}

var _ Field = (*field)(nil)

type field struct {
	alias  string
	schema string
	col    string
}

func (f *field) String() string {
	return fmt.Sprintf("schema: %s, column: %s, alias: %s", f.schema, f.col, f.alias)
}

func F(s string) Field {
	f := field{}
	f.parse(s)

	return &f
}

func (f *field) As(alias string) Field {
	f.alias = alias
	return f
}

func (f *field) Alias() string {
	return f.alias
}
func (f *field) Expression() (string, bool) {
	if len(f.col) == 0 && len(f.schema) == 0 {
		return "", false
	}

	if len(f.schema) != 0 {
		return fmt.Sprintf("%s.%s", f.schema, f.col), true
	}

	return f.col, true
}

func (f *field) Equal(val any) Expression {
	return NewBooleanExp(f.Copy(), val, equal)
}

func (f *field) NotEqual(val any) Expression {
	return NewBooleanExp(f.Copy(), val, notEqual)
}

func (f *field) Greater(val any) Expression {
	return NewBooleanExp(f.Copy(), val, more)

}

func (f *field) Less(val any) Expression {
	return NewBooleanExp(f.Copy(), val, less)

}

func (f *field) GreaterEq(val any) Expression {
	return NewBooleanExp(f.Copy(), val, moreEqual)

}

func (f *field) LessEq(val any) Expression {
	return NewBooleanExp(f.Copy(), val, lessEqual)

}

func (f *field) IsNotNull() Expression {
	return NewBooleanExp(f.Copy(), nil, isNotNull)
}

func (f *field) In(val ...any) Expression {
	return NewBooleanExp(f.Copy(), val, in)
}

func (f *field) NotIn(val ...any) Expression {
	return NewBooleanExp(f.Copy(), val, notIn)
}

func (f *field) IsNull() Expression {
	return NewBooleanExp(f.Copy(), nil, isNull)
}

func (f *field) Schema() string { return f.schema }

func (f *field) Column() string { return f.col }

func (f *field) Copy() Field {
	return &field{
		schema: f.schema,
		col:    f.col,
	}
}

func (f *field) parse(s string) {

	if len(s) == 0 {
		return
	}

	if s[0] == '.' || s[len(s)-1] == '.' {
		return
	}

	tokens := strings.Split(s, ".")

	if len(tokens) == 2 {
		f.col = tokens[1]
		f.schema = tokens[0]
		return
	}
	if len(tokens) == 1 {
		f.col = tokens[0]
	}
}
