package ksql

import (
	"errors"
	"fmt"
	"strings"
)

const (
	Ascending = OrderDirection(iota)
	Descending
)

type (
	OrderByExpression interface {
		OrderBy(expressions ...OrderedExpression) OrderByExpression
		OrderedExpressions() []OrderedExpression
		IsEmpty() bool
		Expression() (string, error)
	}

	OrderedExpression interface {
		Field() Field
		Direction() OrderDirection
		Expression() (string, error)
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

func (o *orderedExpression) Expression() (string, error) {
	if o.field == nil {
		return "", errors.New("field cannot be nil")
	}

	expression, err := o.field.Expression()
	if err != nil {
		return "", fmt.Errorf("field expression: %w", err)
	}

	switch o.direction {
	case Ascending:
		return expression + " ASC", nil
	case Descending:
		return expression + " DESC", nil
	default:
		return "", errors.New("invalid order direction, must be Ascending or Descending")
	}
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

func (o *orderby) Expression() (string, error) {
	if len(o.expressions) == 0 {
		return "", errors.New("cannot create ORDER BY expression with no expressions")
	}

	var builder strings.Builder

	builder.WriteString("ORDER BY ")
	for i, expr := range o.expressions {
		if i > 0 {
			builder.WriteString(", ")
		}
		expression, err := expr.Expression()
		if err != nil {
			return "", fmt.Errorf("ordered expression: %w", err)
		}
		builder.WriteString(expression)
	}

	return builder.String(), nil
}

func (o *orderby) IsEmpty() bool {
	return len(o.expressions) == 0
}
