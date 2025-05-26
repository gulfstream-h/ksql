package ksql

import (
	"fmt"
)

type (
	JoinExpression interface {
		Schema() string
		On() BooleanExpression
	}

	join struct {
		on        BooleanExpression
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

func Join(schema string, on BooleanExpression, joinType JoinType) JoinExpression {
	return &join{
		on:        on,
		schema:    schema,
		operation: joinType,
	}
}

func (j *join) Schema() string {
	return j.schema
}

func (j *join) On() BooleanExpression {
	return j.on
}

func (j *join) Expression() string {
	var (
		operationString string
	)

	if len(j.schema) == 0 || j.on == nil {
		return ""
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
	}
	return fmt.Sprintf(
		"%s %s ON %s",
		operationString, j.schema, j.on.Expression(),
	)

}
