package ksql

import "strings"

type HavingExpression interface {
	Expression() string
	Conditionals() []BooleanExpression
	Having(exps ...BooleanExpression) HavingExpression
}

type having struct {
	conditionals []BooleanExpression
}

func NewHavingExpression() HavingExpression {
	return &having{}
}

func (h *having) Having(exps ...BooleanExpression) HavingExpression {
	h.conditionals = append(h.conditionals, exps...)
	return h
}

func (h *having) Conditionals() []BooleanExpression {
	conditionals := make([]BooleanExpression, len(h.conditionals))
	copy(conditionals, h.conditionals)
	return conditionals
}

func (h *having) Expression() string {
	if len(h.conditionals) == 0 {
		return ""
	}

	var (
		builder = new(strings.Builder)
		isFirst = true
	)

	builder.WriteString("HAVING ")

	for i := range h.conditionals {

		ex := h.conditionals[i].Expression()
		if len(ex) == 0 {
			continue
		}

		if i != len(h.conditionals)-1 && !isFirst {
			builder.WriteString(" AND ")
		}

		builder.WriteString(ex)
		isFirst = false
	}

	return builder.String()
}
