package ksql

import (
	"errors"
	"fmt"
)

type (
	JoinExpression interface {
		Schema() string
		On() Conditional
		Type() JoinType
		Expression() (string, error)
	}

	join struct {
		on        Conditional
		fromEx    FromExpression
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

func Join(schema FromExpression, on Conditional, joinType JoinType) JoinExpression {
	return &join{
		on:        on,
		fromEx:    schema,
		operation: joinType,
	}
}

func (j *join) Schema() string {
	return j.fromEx.Schema()
}

func (j *join) On() Conditional {
	return j.on
}

func (j *join) Type() JoinType {
	return j.operation
}

func (j *join) Expression() (string, error) {
	var (
		operationString string
	)

	if len(j.fromEx.Schema()) == 0 || j.on == nil {
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

	if len(j.fromEx.Alias()) != 0 {
		return fmt.Sprintf(
			"%s %s AS %s ON %s",
			operationString, j.fromEx.Schema(),
			j.fromEx.Alias(), expression,
		), nil
	}

	return fmt.Sprintf(
		"%s %s ON %s",
		operationString, j.fromEx, expression,
	), nil

}
