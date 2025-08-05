package ksql

import (
	"errors"
	"fmt"
)

type (
	// JoinExpression - common contract for all JOIN expressions
	JoinExpression interface {
		Expression

		Schema() string
		On() Conditional
		Type() JoinType
	}

	// Conditional - represents a conditional expression used in joins
	join struct {
		on        Conditional
		fromEx    FromExpression
		operation JoinType
	}
	// JoinType - represents the type of join merge algorithm
	JoinType int
)

const (
	Left = JoinType(iota)
	Right
	Inner
	Outer
	Cross
)

// Join creates a new JoinExpression with the specified FROM expression, ON condition, and join type.
func Join(schema FromExpression, on Conditional, joinType JoinType) JoinExpression {
	return &join{
		on:        on,
		fromEx:    schema,
		operation: joinType,
	}
}

// Schema returns the schema of the join expression, which is the schema of the FROM expression.
func (j *join) Schema() string {
	return j.fromEx.Schema()
}

// On returns the conditional expression used for the join.
func (j *join) On() Conditional {
	return j.on
}

// Type returns the type of the join operation.
func (j *join) Type() JoinType {
	return j.operation
}

// Expression returns the KSQL expression for the JOIN operation.
func (j *join) Expression() (string, error) {
	var (
		operationString string
	)

	if j.fromEx == nil || j.on == nil {
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
		operationString, j.fromEx.Schema(), expression,
	), nil

}
