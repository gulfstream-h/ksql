package ksql

import "strings"

type (
	ExpressionList interface {
		Expression() string
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

func (el *expressionList) Expression() string {
	var (
		operation string

		builder = new(strings.Builder)
		isFirst = true
	)

	switch el.opType {
	case OrType:
		operation = " OR "
	case AndType:
		operation = " AND "
	}

	for idx := range el.expressions {
		exp := el.expressions[idx].Expression()
		if len(exp) == 0 {
			// todo add err
			return ""
		}

		if idx != len(el.expressions)-1 && !isFirst {
			builder.WriteString(operation)
		}

		builder.WriteString(exp)
		isFirst = false

	}

	return builder.String()
}
