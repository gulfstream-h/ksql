package ksql

import "strings"

type HavingExpression interface {
	Expression() (string, bool)
	Conditionals() []Expression
	IsEmpty() bool
	Having(exps ...Expression) HavingExpression
}

type having struct {
	conditionals []Expression
}

func NewHavingExpression() HavingExpression {
	return &having{}
}

func (h *having) Having(exps ...Expression) HavingExpression {
	h.conditionals = append(h.conditionals, exps...)
	return h
}

func (h *having) IsEmpty() bool {
	return len(h.conditionals) == 0
}

func (h *having) Conditionals() []Expression {
	conditionals := make([]Expression, len(h.conditionals))
	copy(conditionals, h.conditionals)
	return conditionals
}

func (h *having) Expression() (string, bool) {
	if len(h.conditionals) == 0 {
		return "", false
	}

	var (
		builder = new(strings.Builder)
		isFirst = true
	)

	builder.WriteString("HAVING ")

	for i := range h.conditionals {

		ex, ok := h.conditionals[i].Expression()
		if !ok {
			return "", false
		}

		if i != len(h.conditionals)-1 && !isFirst {
			builder.WriteString(" AND ")
		}

		builder.WriteString(ex)
		isFirst = false
	}

	return builder.String(), true
}
