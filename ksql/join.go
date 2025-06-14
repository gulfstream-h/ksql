package ksql

import (
	"fmt"
)

type (
	JoinExpression interface {
		Schema() string
		On() Expression
		Type() JoinType
		Expression() (string, bool)
	}

	join struct {
		on        Expression
		schema    string
		operation JoinType
	}

	JoinType int
)

const (
	Left = JoinType(iota)
	Right
	Inner
	Outer
	Cross
)

func Join(schema string, on Expression, joinType JoinType) JoinExpression {
	return &join{
		on:        on,
		schema:    schema,
		operation: joinType,
	}
}

func (j *join) Schema() string {
	return j.schema
}

func (j *join) On() Expression {
	return j.on
}

func (j *join) Type() JoinType {
	return j.operation
}

func (j *join) Expression() (string, bool) {
	var (
		operationString string
	)

	if len(j.schema) == 0 || j.on == nil {
		return "", false
	}

	expression, ok := j.on.Expression()
	if !ok {
		return "", false
	}

	switch j.operation {
	case Left:
		operationString = "LEFT JOIN"
	case Inner:
		operationString = "JOIN"
	case Right:
		operationString = "RIGHT JOIN"
	case Outer:
		operationString = "OUTER JOIN"
	case Cross:
		operationString = "CROSS JOIN"
	default:
		return "", false
	}
	return fmt.Sprintf(
		"%s %s ON %s",
		operationString, j.schema, expression,
	), true

}
