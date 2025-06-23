package ksql

import (
	"fmt"
	"strings"
)

type (
	Field interface {
		Comparable
		Ordered
		Nullable
		ComparableArray

		Schema() string
		Column() string
		Alias() string
		As(alias string) Field
		Copy() Field
		Expression() (string, error)
	}

	field struct {
		alias  string
		schema string
		col    string
	}

	aggregatedField struct {
		alias string
		fn    AggregateFunction
		Field
	}
)

var (
	_ Field = (*field)(nil)
	_ Field = (*aggregatedField)(nil)
)

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
func (f *field) Expression() (string, error) {
	if len(f.col) == 0 && len(f.schema) == 0 {
		return "", fmt.Errorf("field is not defined")
	}

	if len(f.schema) != 0 {
		return fmt.Sprintf("%s.%s", f.schema, f.col), nil
	}

	return f.col, nil
}

func (f *field) Equal(val any) Conditional {
	return NewBooleanExp(f.Copy(), val, equal)
}

func (f *field) NotEqual(val any) Conditional {
	return NewBooleanExp(f.Copy(), val, notEqual)
}

func (f *field) Greater(val any) Conditional {
	return NewBooleanExp(f.Copy(), val, more)

}

func (f *field) Less(val any) Conditional {
	return NewBooleanExp(f.Copy(), val, less)

}

func (f *field) GreaterEq(val any) Conditional {
	return NewBooleanExp(f.Copy(), val, moreEqual)

}

func (f *field) LessEq(val any) Conditional {
	return NewBooleanExp(f.Copy(), val, lessEqual)

}

func (f *field) IsNotNull() Conditional {
	return NewBooleanExp(f.Copy(), nil, isNotNull)
}

func (f *field) In(val ...any) Conditional {
	return NewBooleanExp(f.Copy(), val, in)
}

func (f *field) NotIn(val ...any) Conditional {
	return NewBooleanExp(f.Copy(), val, notIn)
}

func (f *field) IsNull() Conditional {
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

func (f *field) Asc() OrderedExpression {
	return newOrderedExpression(f, Ascending)
}
func (f *field) Desc() OrderedExpression {
	return newOrderedExpression(f, Descending)
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

func NewAggregatedField(fn AggregateFunction) Field {
	if fn == nil {
		return nil
	}

	return &aggregatedField{
		fn: fn,
	}
}

func (af *aggregatedField) Greater(val any) Conditional {
	return NewBooleanExp(af, val, more)
}

func (af *aggregatedField) Less(val any) Conditional {
	return NewBooleanExp(af, val, less)
}

func (af *aggregatedField) GreaterEq(val any) Conditional {
	return NewBooleanExp(af, val, moreEqual)
}

func (af *aggregatedField) LessEq(val any) Conditional {
	return NewBooleanExp(af, val, lessEqual)
}

func (af *aggregatedField) IsNotNull() Conditional {
	return NewBooleanExp(af, nil, isNotNull)
}

func (af *aggregatedField) In(val ...any) Conditional {
	return NewBooleanExp(af, val, in)
}

func (af *aggregatedField) NotIn(val ...any) Conditional {
	return NewBooleanExp(af, val, notIn)
}

func (af *aggregatedField) IsNull() Conditional {
	return NewBooleanExp(af, nil, isNull)
}

func (af *aggregatedField) Asc() OrderedExpression {
	return newOrderedExpression(af, Ascending)
}

func (af *aggregatedField) Desc() OrderedExpression {
	return newOrderedExpression(af, Descending)
}

func (af *aggregatedField) Schema() string {
	return ""
}

func (af *aggregatedField) Column() string {
	return ""
}

func (af *aggregatedField) As(a string) Field {
	af.alias = a
	return af
}

func (af *aggregatedField) Alias() string {
	if len(af.alias) > 0 {
		return af.alias
	}

	if af.fn == nil {
		return ""
	}

	return af.fn.Alias()
}

func (af *aggregatedField) Expression() (string, error) {
	if af.fn == nil {
		return "", fmt.Errorf("aggregate function is not defined")
	}

	return af.fn.Expression()
}
