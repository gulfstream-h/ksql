package ksql

import (
	"errors"
	"fmt"
	"github.com/gulfstream-h/ksql/internal/util"
	"strings"
)

type (
	CaseConditional interface {
		Expression
		Conditional

		Then() any
	}

	CaseField interface {
		Expression
		Field

		Conditionals() []CaseConditional
		Else(val any) CaseField
	}

	caseExpression struct {
		Field
		conds         []CaseConditional
		defaultReturn any
		alias         string
	}

	caseConditional struct {
		Conditional
		then any
	}
)

func Case(alias string, conds ...CaseConditional) CaseField {
	c := new(caseExpression)
	ff := new(field)
	ff.col = "CASE"
	c.Field = ff
	c.conds = append(c.conds, conds...)
	c.alias = alias
	return c
}

func CaseWhen(
	cond Conditional,
	then any,
) CaseConditional {
	return &caseConditional{
		Conditional: cond,
		then:        then,
	}
}

func (c *caseExpression) Alias() string {
	return c.alias
}

func (c *caseExpression) Conditionals() []CaseConditional {
	return c.conds
}

func (c *caseExpression) Else(val any) CaseField {
	c.defaultReturn = val
	return c
}

func (c *caseExpression) Expression() (string, error) {
	var (
		builder strings.Builder
	)

	if len(c.conds) == 0 {
		return "", errors.New("no conditionals set")
	}

	if len(c.alias) == 0 {
		return "", errors.New("case field must have an alias")
	}

	builder.WriteString("CASE ")
	for idx := range c.conds {
		condExpression, err := c.conds[idx].Expression()
		if err != nil {
			return "", fmt.Errorf("case confition expression: %w", err)
		}
		builder.WriteString(condExpression)
		builder.WriteString(" ")
	}

	defaultExpression := util.Serialize(c.defaultReturn)

	if len(defaultExpression) == 0 {
		return "", errors.New("else expression invalid type")
	}

	builder.WriteString("ELSE ")
	builder.WriteString(defaultExpression)

	builder.WriteString(" END AS ")
	builder.WriteString(c.alias)

	return builder.String(), nil
}

func (c *caseConditional) Then() any {
	return c.then
}

func (c *caseConditional) Expression() (string, error) {
	if c.Conditional == nil {
		return "", errors.New("nil conditional")
	}

	conditionalExpression, err := c.Conditional.Expression()
	if err != nil {
		return "", fmt.Errorf("conditional expression: %w", err)
	}

	thenExpression := util.Serialize(c.then)
	if len(thenExpression) == 0 {
		return "", errors.New("invalid then expression")
	}

	return strings.Join([]string{
		"WHEN", conditionalExpression, "THEN", thenExpression,
	}, " "), nil
}
