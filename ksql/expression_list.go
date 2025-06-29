package ksql

import (
	"fmt"
	"strings"
)

type (
	ExpressionList interface {
		Conditional
		Conditionals() []Conditional
	}

	BooleanOperationType int

	expressionList struct {
		expressions []Conditional
		opType      BooleanOperationType
	}
)

const (
	OrType = BooleanOperationType(iota)
	AndType
)

func Or(exps ...Conditional) ExpressionList {
	return &expressionList{
		expressions: exps,
		opType:      OrType,
	}
}

func And(exps ...Conditional) ExpressionList {
	return &expressionList{
		expressions: exps,
		opType:      AndType,
	}
}

func (el *expressionList) Conditionals() []Conditional {
	exps := make([]Conditional, len(el.expressions))
	copy(exps, el.expressions)
	return exps
}

func (el *expressionList) Left() []Field {
	fields := make([]Field, 0, len(el.expressions))
	for _, exp := range el.expressions {
		fields = append(fields, exp.Left()...)
	}
	return fields
}

func (el *expressionList) Right() []any {
	rights := make([]any, 0, len(el.expressions))
	for _, exp := range el.expressions {
		rights = append(rights, exp.Right()...)
	}
	return rights
}

func (el *expressionList) Expression() (string, error) {
	var (
		operation string

		builder = new(strings.Builder)
		isFirst = true
	)

	if len(el.expressions) == 0 {
		return "", fmt.Errorf("cannot create expression list with no expressions")
	}

	switch el.opType {
	case OrType:
		operation = " OR "
	case AndType:
		operation = " AND "
	default:
		return "", fmt.Errorf("unsupported boolean operation type: %d", el.opType)
	}

	builder.WriteString("( ")

	for idx := range el.expressions {
		exp, err := el.expressions[idx].Expression()
		if err != nil {
			return "", fmt.Errorf("conditional expression: %w", err)
		}

		if !isFirst {
			builder.WriteString(operation)
		}

		builder.WriteString(exp)
		isFirst = false

	}

	builder.WriteString(" )")

	return builder.String(), nil
}
