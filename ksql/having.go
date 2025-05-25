package ksql

import "strings"

type HavingExpression interface {
	Expression() string
	AggregatesConditionals() []BooleanExpression
}

type having struct {
	conditionals []BooleanExpression
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
