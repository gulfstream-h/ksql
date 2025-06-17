package ksql

import "strings"

type WhereExpression interface {
	IsEmpty() bool
	Expression() (string, bool)
	Conditionals() []Expression
	Where(exps ...Expression) WhereExpression
}

type where struct {
	conditionals []Expression
}

func NewWhereExpression() WhereExpression {
	return &where{}
}

func (w *where) IsEmpty() bool {
	return len(w.conditionals) == 0
}

func (w *where) Expression() (string, bool) {
	if len(w.conditionals) == 0 {
		return "", false
	}

	var (
		builder = new(strings.Builder)
		isFirst = true
	)

	builder.WriteString("WHERE ")

	for i := range w.conditionals {
		ex, ok := w.conditionals[i].Expression()
		if !ok {
			return "", false
		}

		if !isFirst {
			builder.WriteString(" AND ")
		}

		builder.WriteString(ex)
		isFirst = false
	}

	return builder.String(), true
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
