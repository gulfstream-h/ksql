package ksql

import (
	"errors"
	"fmt"
	"strings"
)

// GroupExpression - common contract for all GROUP BY expressions
type GroupExpression interface {
	Expression

	GroupedFields() []Field
	IsEmpty() bool
	GroupBy(fields ...Field) GroupExpression
}

// group implements the GroupExpression interface for constructing GROUP BY clauses in KSQL
type group struct {
	fields []Field
}

// NewGroupByExpression creates a new GroupExpression instance
func NewGroupByExpression() GroupExpression {
	return &group{}
}

// IsEmpty checks if the GROUP BY expression has no fields
func (g *group) IsEmpty() bool {
	return len(g.fields) == 0
}

// GroupedFields returns a copy of the fields used in the GROUP BY expression
func (g *group) GroupedFields() []Field {
	fields := make([]Field, len(g.fields))
	copy(fields, g.fields)
	return fields
}

// GroupBy adds one or more fields to the GROUP BY expression
func (g *group) GroupBy(fields ...Field) GroupExpression {
	g.fields = append(g.fields, fields...)
	return g
}

// Expression generates the GROUP BY SQL expression based on the fields provided
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
