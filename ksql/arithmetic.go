package ksql

import (
	"errors"
	"fmt"
	"github.com/gulfstream-h/ksql/internal/util"
	"strings"
)

type (
	arithmeticExpr struct {
		Field
		right     any
		operation ArithmeticOperation
	}

	Arithmetic interface {
		Add(val any) Field
		Sub(val any) Field
		Mul(val any) Field
		Div(val any) Field
		Mod(val any) Field
	}
)

func newArithmetic(left Field, right any, op ArithmeticOperation) Field {
	return &arithmeticExpr{
		Field:     left,
		right:     right,
		operation: op,
	}
}

func (a *arithmeticExpr) Expression() (string, error) {
	if a.Field == nil {
		return "", errors.New("left is nil")
	}

	if a.right == nil {
		return "", errors.New("right is nil")
	}

	var (
		operation       string
		rightExpression string
	)

	switch a.operation {
	case plus:
		operation = "+"
	case minus:
		operation = "-"
	case multiply:
		operation = "*"
	case divide:
		operation = "/"
	case modulo:
		operation = "%"
	default:
		return "", errors.New("invalid operation type")
	}

	leftExpression, err := a.Field.Expression()
	if err != nil {
		return "", fmt.Errorf("left exression: %w", err)
	}

	if val, ok := a.right.(Field); ok {
		rightExpression, err = val.Expression()
		if err != nil {
			return "", fmt.Errorf("right expression: %w", err)
		}
	} else {
		rightExpression = util.Serialize(a.right)
		if len(rightExpression) == 0 {
			return "", errors.New("serialize right error")
		}
	}

	return strings.Join([]string{"(", leftExpression, operation, rightExpression, ")"}, " "), nil
}
