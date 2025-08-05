package ksql

import (
	"errors"
	"fmt"
	"strings"
)

// WhereExpression - common contract for all WHERE expressions
type WhereExpression interface {
	Expression

	IsEmpty() bool
	Conditionals() []Conditional
	Where(exps ...Conditional) WhereExpression
}

// where - base implementation of the WhereExpression interface
type where struct {
	conditionals []Conditional
}

// NewWhereExpression creates a new instance of WhereExpression.
func NewWhereExpression() WhereExpression {
	return &where{}
}

// IsEmpty checks if the WHERE clause has no conditionals.
func (w *where) IsEmpty() bool {
	return len(w.conditionals) == 0
}

// Expression builds the WHERE clause expression from the conditionals.
func (w *where) Expression() (string, error) {
	if len(w.conditionals) == 0 {
		return "", errors.New("where expression cannot be empty")
	}

	var (
		builder = new(strings.Builder)
		isFirst = true
	)

	builder.WriteString("WHERE ")

	for i := range w.conditionals {
		ex, err := w.conditionals[i].Expression()
		if err != nil {
			return "", fmt.Errorf("conditional expression: %w", err)
		}

		if !isFirst {
			builder.WriteString(" AND ")
		}

		builder.WriteString(ex)
		isFirst = false
	}

	return builder.String(), nil
}

// Conditionals returns a copy of the conditionals in the WHERE clause.
func (w *where) Conditionals() []Conditional {
	conditionals := make([]Conditional, len(w.conditionals))
	copy(conditionals, w.conditionals)
	return conditionals
}

// Where adds conditional expressions to the WHERE clause.
func (w *where) Where(exps ...Conditional) WhereExpression {
	w.conditionals = append(w.conditionals, exps...)
	return w
}
