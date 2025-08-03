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
		left      any
		right     any
		operation ArithmeticOperation

		alias string
	}

	ArithmeticFunc interface {
		Field
		Operation() ArithmeticOperation
		Right() any
	}
)

func Add(left any, right any) ArithmeticFunc {
	return newArithmetic(left, right, plus)
}

func Sub(left any, right any) ArithmeticFunc {
	return newArithmetic(left, right, minus)
}

func Mul(left any, right any) ArithmeticFunc {
	return newArithmetic(left, right, multiply)
}

func Div(left any, right any) ArithmeticFunc {
	return newArithmetic(left, right, divide)
}

func Mod(left any, right any) ArithmeticFunc {
	return newArithmetic(left, right, modulo)
}

func newArithmetic(left any, right any, op ArithmeticOperation) ArithmeticFunc {
	return &arithmeticExpr{
		Field:     new(field),
		left:      left,
		right:     right,
		operation: op,
	}
}

func (a *arithmeticExpr) As(alias string) Field {
	a.alias = alias
	return a
}

func (a *arithmeticExpr) Alias() string {
	return a.alias
}

func (a *arithmeticExpr) Operation() ArithmeticOperation {
	return a.operation
}

func (a *arithmeticExpr) Right() any {
	return a.right
}

func (a *arithmeticExpr) InnerRelations() []Relational {

	var (
		relations []Relational
	)

	if f, ok := a.left.(Field); ok {
		relations = append(relations, f.InnerRelations()...)
		if !f.derived() {
			relations = append(relations, f)
		}
	}

	if f, ok := a.right.(Field); ok {
		relations = append(relations, f.InnerRelations()...)
		if !f.derived() {
			relations = append(relations, f)
		}
	}

	return relations
}

func (a *arithmeticExpr) derived() bool {
	return true
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
		leftExpression  string
		err             error
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

	if val, ok := a.left.(Expression); ok {
		leftExpression, err = val.Expression()
		if err != nil {
			return "", fmt.Errorf("left expression: %w", err)
		}
	} else {
		leftExpression = util.Serialize(a.left)
		if len(leftExpression) == 0 {
			return "", errors.New("serialize left error")
		}
	}

	if val, ok := a.right.(Expression); ok {
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

	result := []string{"(", leftExpression, operation, rightExpression, ")"}

	if len(a.alias) != 0 {
		result = append(result, "AS "+a.alias)
	}

	return strings.Join(result, " "), nil
}
