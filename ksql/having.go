package ksql

import (
	"errors"
	"fmt"
	"strings"
)

type HavingExpression interface {
	Expression() (string, error)
	Conditionals() []Conditional
	IsEmpty() bool
	Having(exps ...Conditional) HavingExpression
}

type having struct {
	conditionals []Conditional
}

func NewHavingExpression() HavingExpression {
	return &having{}
}

func (h *having) Having(exps ...Conditional) HavingExpression {
	h.conditionals = append(h.conditionals, exps...)
	return h
}

func (h *having) IsEmpty() bool {
	return len(h.conditionals) == 0
}

func (h *having) Conditionals() []Conditional {
	conditionals := make([]Conditional, len(h.conditionals))
	copy(conditionals, h.conditionals)
	return conditionals
}

func (h *having) Expression() (string, error) {
	if len(h.conditionals) == 0 {
		return "", errors.New("cannot create HAVING expression with no conditionals")
	}

	var (
		builder = new(strings.Builder)
		isFirst = true
	)

	builder.WriteString("HAVING ")

	for i := range h.conditionals {

		ex, err := h.conditionals[i].Expression()
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
