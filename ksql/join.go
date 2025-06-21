package ksql

import (
	"errors"
	"fmt"
)

type (
	JoinExpression interface {
		Schema() string
		On() Expression
		Type() JoinType
		Expression() (string, error)
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

func (j *join) Expression() (string, error) {
	var (
		operationString string
	)

	if len(j.schema) == 0 || j.on == nil {
		return "", errors.New("join schema and expression cannot be empty")
	}

	expression, err := j.on.Expression()
	if err != nil {
		return "", fmt.Errorf("join expression: %w", err)
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
		return "", errors.New("invalid join type")
	}
	return fmt.Sprintf(
		"%s %s ON %s",
		operationString, j.schema, expression,
	), nil

}
