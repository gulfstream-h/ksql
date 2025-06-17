package ksql

import "strings"

type GroupExpression interface {
	Expression() (string, bool)
	GroupedFields() []Field
	IsEmpty() bool
	GroupBy(fields ...Field) GroupExpression
}

type group struct {
	fields []Field
}

func NewGroupByExpression() GroupExpression {
	return &group{}
}

func (g *group) IsEmpty() bool {
	return len(g.fields) == 0
}

func (g *group) GroupedFields() []Field {
	fields := make([]Field, len(g.fields))
	copy(fields, g.fields)
	return fields
}

func (g *group) GroupBy(fields ...Field) GroupExpression {
	g.fields = append(g.fields, fields...)
	return g
}

func (g *group) Expression() (string, bool) {
	if len(g.fields) == 0 {
		return "", false
	}

	var (
		builder = new(strings.Builder)
		isFirst = true
	)

	builder.WriteString("GROUP BY ")

	for i := range g.fields {
		ex, ok := g.fields[i].Expression()
		if !ok {
			return "", false
		}

		if !isFirst {
			builder.WriteString(", ")
		}

		builder.WriteString(ex)
		isFirst = false
	}

	return builder.String(), true
}
