package ksql

import (
	"errors"
	"fmt"
	"strings"
)

const (
	// Ascending - represents ascending order direction
	Ascending = OrderDirection(iota)
	// Descending - represents descending order direction
	Descending
)

type (
	// OrderByExpression - common contract for all ORDER BY expressions
	OrderByExpression interface {
		Expression

		OrderBy(expressions ...OrderedExpression) OrderByExpression
		OrderedExpressions() []OrderedExpression
		IsEmpty() bool
	}

	// OrderedExpression - represents an expression for ordering results in a query
	OrderedExpression interface {
		Expression

		Field() Field
		Direction() OrderDirection
	}

	// OrderDirection - represents the direction of ordering
	OrderDirection int

	// orderedExpression - implementation of the OrderedExpression interface
	orderedExpression struct {
		field     Field
		direction OrderDirection
	}

	// orderby - base implementation of the OrderByExpression interface
	orderby struct {
		expressions []OrderedExpression
	}
)

// NewOrderedExpression creates a new OrderedExpression instance with the specified field and direction.
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

// Field returns the sorted field of the ordered expression.
func (o *orderedExpression) Field() Field {
	return o.field
}

// Direction returns the order direction of the ordered expression.
func (o *orderedExpression) Direction() OrderDirection {
	return o.direction
}

// Expression builds the ORDER BY expression for the ordered field with its direction.
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

// NewOrderByExpression creates OrderByExpression instance
func NewOrderByExpression() OrderByExpression {
	return &orderby{}
}

// OrderBy adds ordered expressions to the ORDER BY clause.
func (o *orderby) OrderBy(expressions ...OrderedExpression) OrderByExpression {
	o.expressions = append(o.expressions, expressions...)
	return o
}

// OrderedExpressions returns a copy of the ordered expressions in the ORDER BY clause.
func (o *orderby) OrderedExpressions() []OrderedExpression {
	return o.expressions
}

// Expression builds the ORDER BY clause expression from accumulated ordered expressions
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

// IsEmpty checks if the ORDER BY clause has no expressions.
func (o *orderby) IsEmpty() bool {
	return len(o.expressions) == 0
}
