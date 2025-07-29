package ksql

import (
	"fmt"
	"strings"
)

type (
	// Field - common contract for all fields in KSQL
	Field interface {
		Comparable
		Ordered
		Nullable
		ComparableArray
		Expression

		Schema() string
		Column() string
		Alias() string
		As(alias string) Field
		Copy() Field
	}

	// field - base implementation of the Field interface
	field struct {
		alias  string
		schema string
		col    string
	}

	// aggregatedField - implementation of the Field interface for aggregated fields
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

// // String returns a string representation of the field, including schema, column, and alias
func (f *field) String() string {
	return fmt.Sprintf("schema: %s, column: %s, alias: %s", f.schema, f.col, f.alias)
}

// F creates a new Field instance
func F(s string) Field {
	f := field{}
	f.parse(s)

	return &f
}

// As sets the alias for the field and returns the field itself
func (f *field) As(alias string) Field {
	f.alias = alias
	return f
}

// Alias returns the alias of the field
func (f *field) Alias() string {
	return f.alias
}

// Expression returns the KSQL query for the field
func (f *field) Expression() (string, error) {
	var (
		strs []string
	)
	if len(f.col) == 0 && len(f.schema) == 0 {
		return "", fmt.Errorf("field is not defined")
	}

	if len(f.schema) != 0 {
		strs = append(strs, fmt.Sprintf("%s.%s", f.schema, f.col))
	} else {
		strs = append(strs, f.col)
	}
	if len(f.alias) > 0 {
		strs = append(strs, fmt.Sprintf("AS %s", f.alias))
	}

	return strings.Join(strs, " "), nil
}

// Equal returns a Conditional expression for equality comparison
func (f *field) Equal(val any) Conditional {
	return NewBooleanExp(f.Copy(), val, equal)
}

// NotEqual returns a Conditional expression for inequality comparison
func (f *field) NotEqual(val any) Conditional {
	return NewBooleanExp(f.Copy(), val, notEqual)
}

// Greater returns a Conditional expression for greater than comparison
func (f *field) Greater(val any) Conditional {
	return NewBooleanExp(f.Copy(), val, more)

}

// Less returns a Conditional expression for less than comparison
func (f *field) Less(val any) Conditional {
	return NewBooleanExp(f.Copy(), val, less)

}

// GreaterEq returns a Conditional expression for greater than or equal to comparison
func (f *field) GreaterEq(val any) Conditional {
	return NewBooleanExp(f.Copy(), val, moreEqual)

}

// LessEq returns a Conditional expression for less than or equal to comparison
func (f *field) LessEq(val any) Conditional {
	return NewBooleanExp(f.Copy(), val, lessEqual)

}

// IsNotNull returns a Conditional expression to check if the field is not null
func (f *field) IsNotNull() Conditional {
	return NewBooleanExp(f.Copy(), nil, isNotNull)
}

// In returns a Conditional expression to check if the field's value is in the provided values
func (f *field) In(val ...any) Conditional {
	return NewBooleanExp(f.Copy(), val, in)
}

// NotIn returns a Conditional expression to check if the field's value is not in the provided values
func (f *field) NotIn(val ...any) Conditional {
	return NewBooleanExp(f.Copy(), val, notIn)
}

// IsNull returns a Conditional expression to check if the field is null
func (f *field) IsNull() Conditional {
	return NewBooleanExp(f.Copy(), nil, isNull)
}

// Schema returns the schema of the field
func (f *field) Schema() string { return f.schema }

// Column returns the column name of the field
func (f *field) Column() string { return f.col }

// Copy creates a copy of the field with the same schema and column
func (f *field) Copy() Field {
	return &field{
		schema: f.schema,
		col:    f.col,
	}
}

// Asc returns an OrderedExpression for ascending order
func (f *field) Asc() OrderedExpression {
	return newOrderedExpression(f, Ascending)
}

// Desc returns an OrderedExpression for descending order
func (f *field) Desc() OrderedExpression {
	return newOrderedExpression(f, Descending)
}

// parse splits the field string into schema and column parts
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

// NewAggregatedField creates a new aggregated field using the provided AggregateFunction
func NewAggregatedField(fn AggregateFunction) Field {
	if fn == nil {
		return nil
	}

	return &aggregatedField{
		fn:    fn,
		Field: fn,
	}
}

// Greater returns a Conditional expression for greater than comparison
func (af *aggregatedField) Greater(val any) Conditional {
	return NewBooleanExp(af, val, more)
}

// Less returns a Conditional expression for less than comparison
func (af *aggregatedField) Less(val any) Conditional {
	return NewBooleanExp(af, val, less)
}

// GreaterEq returns a Conditional expression for greater than or equal to comparison
func (af *aggregatedField) GreaterEq(val any) Conditional {
	return NewBooleanExp(af, val, moreEqual)
}

// LessEq returns a Conditional expression for less than or equal to comparison
func (af *aggregatedField) LessEq(val any) Conditional {
	return NewBooleanExp(af, val, lessEqual)
}

// IsNotNull returns a Conditional expression to check if the aggregated field is not null
func (af *aggregatedField) IsNotNull() Conditional {
	return NewBooleanExp(af, nil, isNotNull)
}

// In returns a Conditional expression to check if the aggregated field's value is in the provided values
func (af *aggregatedField) In(val ...any) Conditional {
	return NewBooleanExp(af, val, in)
}

// NotIn returns a Conditional expression to check if the aggregated field's value is not in the provided values
func (af *aggregatedField) NotIn(val ...any) Conditional {
	return NewBooleanExp(af, val, notIn)
}

// IsNull returns a Conditional expression to check if the aggregated field is null
func (af *aggregatedField) IsNull() Conditional {
	return NewBooleanExp(af, nil, isNull)
}

// Asc returns an OrderedExpression for ascending order
func (af *aggregatedField) Asc() OrderedExpression {
	return newOrderedExpression(af, Ascending)
}

// Desc returns an OrderedExpression for descending order
func (af *aggregatedField) Desc() OrderedExpression {
	return newOrderedExpression(af, Descending)
}

// Schema returns the schema of the aggregated field
func (af *aggregatedField) Schema() string {
	return af.Field.Schema()
}

// Column returns the column name of the aggregated field
func (af *aggregatedField) Column() string {
	return af.Field.Column()
}

// As sets the alias for the aggregated field and returns the field itself
func (af *aggregatedField) As(a string) Field {
	af.alias = a
	return af
}

// Alias returns the alias of the aggregated field.
func (af *aggregatedField) Alias() string {
	if len(af.alias) > 0 {
		return af.alias
	}

	if af.fn == nil {
		return ""
	}

	return af.fn.Alias()
}

// Expression returns the KSQL query for the aggregated field
func (af *aggregatedField) Expression() (string, error) {
	if af.fn == nil {
		return "", fmt.Errorf("aggregate function is not defined")
	}

	expression, err := af.fn.Expression()
	if err != nil {
		return "", fmt.Errorf("aggregate function expression: %w", err)
	}
	if len(af.alias) > 0 {
		return fmt.Sprintf("%s AS %s", expression, af.alias), nil
	}
	return expression, nil
}
