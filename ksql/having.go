package ksql

import (
	"errors"
	"fmt"
	"strings"
)

// HavingExpression - common contract for all HAVING expressions
type HavingExpression interface {
	Expression() (string, error)
	Conditionals() []Conditional
	IsEmpty() bool
	Having(exps ...Conditional) HavingExpression
}

// having implements the HavingExpression interface for constructing HAVING clauses in KSQL
type having struct {
	conditionals []Conditional
}

// NewHavingExpression creates a new instance of HavingExpression
func NewHavingExpression() HavingExpression {
	return &having{}
}

// Having adds one or more conditionals to the HAVING expression
func (h *having) Having(exps ...Conditional) HavingExpression {
	h.conditionals = append(h.conditionals, exps...)
	return h
}

// IsEmpty checks if the HAVING expression has no conditionals
func (h *having) IsEmpty() bool {
	return len(h.conditionals) == 0
}

// Conditionals returns a copy of the conditionals used in the HAVING expression
func (h *having) Conditionals() []Conditional {
	conditionals := make([]Conditional, len(h.conditionals))
	copy(conditionals, h.conditionals)
	return conditionals
}

// Expression generates the HAVING SQL expression based on the conditionals provided
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
