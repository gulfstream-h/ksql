package ksql

import "strings"

type WhereExpression interface {
	Expression() string
	Conditionals() []Expression
	Where(exps ...Expression) WhereExpression
}

type where struct {
	conditionals []Expression
}

func NewWhereExpression() WhereExpression {
	return &where{}
}

func (w *where) Expression() string {
	if len(w.conditionals) == 0 {
		return ""
	}

	var (
		builder = new(strings.Builder)
		isFirst = true
	)

	builder.WriteString("WHERE ")

	for i := range w.conditionals {
		ex := w.conditionals[i].Expression()
		if len(ex) == 0 {
			continue
		}

		if i != len(w.conditionals)-1 && !isFirst {
			builder.WriteString(" AND ")
		}

		builder.WriteString(ex)
		isFirst = false
	}

	return builder.String()
}

func (w *where) Conditionals() []Expression {
	conditionals := make([]Expression, len(w.conditionals))
	copy(conditionals, w.conditionals)
	return conditionals
}

func (w *where) Where(exps ...Expression) WhereExpression {
	w.conditionals = append(w.conditionals, exps...)
	return w

}
