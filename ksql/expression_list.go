package ksql

import (
	"fmt"
	"strings"
)

type (
	// ExpressionList - common contract for all boolean expressions that combine multiple conditionals
	ExpressionList interface {
		Conditional
		Conditionals() []Conditional
	}

	// BooleanOperationType - represents the type of boolean operation used in the expression list
	BooleanOperationType int

	// expressionList - implementation of the ExpressionList interface
	expressionList struct {
		expressions []Conditional
		opType      BooleanOperationType
	}
)

const (
	OrType = BooleanOperationType(iota)
	AndType
)

// Or creates a new ExpressionList with the specified conditionals combined using OR operation
func Or(exps ...Conditional) ExpressionList {
	return &expressionList{
		expressions: exps,
		opType:      OrType,
	}
}

// And creates a new ExpressionList with the specified conditionals combined using AND operation
func And(exps ...Conditional) ExpressionList {
	return &expressionList{
		expressions: exps,
		opType:      AndType,
	}
}

// Conditionals returns all the conditionals in the expression list
func (el *expressionList) Conditionals() []Conditional {
	exps := make([]Conditional, len(el.expressions))
	copy(exps, el.expressions)
	return exps
}

// Left returns the left side of the expressions in the list
func (el *expressionList) Left() []Field {
	fields := make([]Field, 0, len(el.expressions))
	for _, exp := range el.expressions {
		fields = append(fields, exp.Left()...)
	}
	return fields
}

// Right returns the right side of the expressions in the list.
func (el *expressionList) Right() []any {
	rights := make([]any, 0, len(el.expressions))
	for _, exp := range el.expressions {
		rights = append(rights, exp.Right()...)
	}
	return rights
}

// Expression returns the KSQL expression for the list of conditionals
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
