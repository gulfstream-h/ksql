package ksql

import "strings"

type GroupExpression interface {
	Expression() string
	GroupedFields() []Field
}

type group struct {
	fields []Field
}

func (g *group) Expression() string {
	if len(g.fields) == 0 {
		return ""
	}

	var (
		builder = new(strings.Builder)
		isFirst = false
	)

	builder.WriteString("GROUP BY ")

	for i := range g.fields {
		ex := g.fields[i].Expression()

		if i != len(g.fields)-1 && !isFirst {
			builder.WriteString(", ")
		}

		builder.WriteString(ex)
		isFirst = true
	}

	return builder.String()
}
