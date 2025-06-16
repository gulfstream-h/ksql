package ksql

import "strings"

const (
	Ascending = OrderDirection(iota)
	Descending
)

type (
	OrderByExpression interface {
		OrderBy(expressions ...OrderedExpression) OrderByExpression
		OrderedExpressions() []OrderedExpression
		IsEmpty() bool
		Expression() (string, bool)
	}

	OrderedExpression interface {
		Field() Field
		Direction() OrderDirection
		Expression() (string, bool)
	}

	OrderDirection int

	orderedExpression struct {
		field     Field
		direction OrderDirection
	}

	orderby struct {
		expressions []OrderedExpression
	}
)

func newOrderedExpression(field Field, direction OrderDirection) OrderedExpression {
	if field == nil {
		return nil
	}

	if direction != Ascending && direction != Descending {
		return nil
	}

	return &orderedExpression{
		field:     field,
		direction: direction,
	}
}

func (o *orderedExpression) Field() Field {
	return o.field
}

func (o *orderedExpression) Direction() OrderDirection {
	return o.direction
}

func (o *orderedExpression) Expression() (string, bool) {
	if o.field == nil {
		return "", false
	}

	expression, ok := o.field.Expression()
	if !ok {
		return "", false
	}

	switch o.direction {
	case Ascending:
		return expression + " ASC", true
	case Descending:
		return expression + " DESC", true
	}

	return "", false
}

func NewOrderByExpression() OrderByExpression {
	return &orderby{}
}

func (o *orderby) OrderBy(expressions ...OrderedExpression) OrderByExpression {
	o.expressions = append(o.expressions, expressions...)
	return o
}

func (o *orderby) OrderedExpressions() []OrderedExpression {
	return o.expressions
}

func (o *orderby) Expression() (string, bool) {
	if len(o.expressions) == 0 {
		return "", false
	}

	var builder strings.Builder

	builder.WriteString("ORDER BY ")
	for i, expr := range o.expressions {
		if i > 0 {
			builder.WriteString(", ")
		}
		expression, ok := expr.Expression()
		if !ok {
			return "", false
		}
		builder.WriteString(expression)
	}

	return builder.String(), true
}

func (o *orderby) IsEmpty() bool {
	return len(o.expressions) == 0
}
