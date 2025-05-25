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
	Copy() Field
	Expression() string
}

var _ Field = (*field)(nil)

type field struct {
	schema string
	col    string
}

func F(s string) Field {
	f := field{}
	f.parse(s)

	return &f
}

func (f *field) Expression() string {
	if len(f.schema) != 0 {
		return fmt.Sprintf("%s.%s", f.schema, f.col)
	}

	return f.col
}

func (f *field) Equal(val any) BooleanExpression {
	return NewBooleanExp(f.Copy(), val, equal)
}

func (f *field) NotEqual(val any) BooleanExpression {
	return NewBooleanExp(f.Copy(), val, notEqual)
}

func (f *field) Greater(val any) BooleanExpression {
	return NewBooleanExp(f.Copy(), val, more)

}

func (f *field) Less(val any) BooleanExpression {
	return NewBooleanExp(f.Copy(), val, less)

}

func (f *field) GreaterEq(val any) BooleanExpression {
	return NewBooleanExp(f.Copy(), val, moreEqual)

}

func (f *field) LessEq(val any) BooleanExpression {
	return NewBooleanExp(f.Copy(), val, lessEqual)

}

func (f *field) IsNotNull() BooleanExpression {
	return NewBooleanExp(f.Copy(), nil, isNotNull)
}

func (f *field) In(val ...any) BooleanExpression {
	return NewBooleanExp(f.Copy(), val, in)
}

func (f *field) NotIn(val ...any) BooleanExpression {
	return NewBooleanExp(f.Copy(), val, notIn)
}

func (f *field) Copy() Field {
	return &field{
		schema: f.schema,
		col:    f.col,
	}
}

func AnyOf[T comparable](val T, comparableVals ...T) bool {
	for i := 0; i < len(comparableVals); i++ {
		if val == comparableVals[i] {
			return true
		}
	}

	return false
}

func (f *field) parse(s string) {
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

func (f *field) Schema() string { return f.schema }
func (f *field) Column() string { return f.col }

func (f *field) IsNull() BooleanExpression {
	return NewBooleanExp(f.Copy(), nil, isNull)
}
