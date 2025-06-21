package ksql

import (
	"errors"
	"fmt"
	"strings"
)

type GroupExpression interface {
	Expression() (string, error)
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

func (g *group) Expression() (string, error) {
	if len(g.fields) == 0 {
		return "", errors.New("cannot create GROUP BY expression with no fields")
	}

	var (
		builder = new(strings.Builder)
		isFirst = true
	)

	builder.WriteString("GROUP BY ")

	for i := range g.fields {
		ex, err := g.fields[i].Expression()
		if err != nil {
			return "", fmt.Errorf("field expression: %w", err)
		}

		if !isFirst {
			builder.WriteString(", ")
		}

		builder.WriteString(ex)
		isFirst = false
	}

	return builder.String(), nil
}
