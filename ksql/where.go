package ksql

import (
	"errors"
	"fmt"
	"strings"
)

type WhereExpression interface {
	IsEmpty() bool
	Expression() (string, error)
	Conditionals() []Conditional
	Where(exps ...Conditional) WhereExpression
}

type where struct {
	conditionals []Conditional
}

func NewWhereExpression() WhereExpression {
	return &where{}
}

func (w *where) IsEmpty() bool {
	return len(w.conditionals) == 0
}

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

func (w *where) Conditionals() []Conditional {
	conditionals := make([]Conditional, len(w.conditionals))
	copy(conditionals, w.conditionals)
	return conditionals
}

func (w *where) Where(exps ...Conditional) WhereExpression {
	w.conditionals = append(w.conditionals, exps...)
	return w

}
