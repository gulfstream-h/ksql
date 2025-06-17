package ksql

import "strings"

type (
	ExpressionList interface {
		Expression() (string, bool)
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

func (el *expressionList) Expression() (string, bool) {
	var (
		operation string

		builder = new(strings.Builder)
		isFirst = true
	)

	if len(el.expressions) == 0 {
		return "", false
	}

	switch el.opType {
	case OrType:
		operation = " OR "
	case AndType:
		operation = " AND "
	}

	builder.WriteString("( ")

	for idx := range el.expressions {
		exp, ok := el.expressions[idx].Expression()
		if !ok {
			return "", false
		}

		if !isFirst {
			builder.WriteString(operation)
		}

		builder.WriteString(exp)
		isFirst = false

	}

	builder.WriteString(" )")

	return builder.String(), true
}
