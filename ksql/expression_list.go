package ksql

import (
	"fmt"
	"strings"
)

type (
	ExpressionList interface {
		Expression() (string, error)
		ExpressionList() []Expression
	}

	BooleanOperationType int

	expressionList struct {
		expressions []Expression
		opType      BooleanOperationType
	}
)

const (
	OrType = BooleanOperationType(iota)
	AndType
)

func Or(exps ...Expression) ExpressionList {
	return &expressionList{
		expressions: exps,
		opType:      OrType,
	}
}

func And(exps ...Expression) ExpressionList {
	return &expressionList{
		expressions: exps,
		opType:      AndType,
	}
}

func (el *expressionList) ExpressionList() []Expression {
	exps := make([]Expression, len(el.expressions))
	copy(exps, el.expressions)
	return exps
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
			return "", fmt.Errorf("create expression: %w", err)
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
