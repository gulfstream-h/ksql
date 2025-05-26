package ksql

import "strings"

type SelectBuilder interface {
	Where(expressions ...BooleanExpression) SelectBuilder
	Having(expressions ...BooleanExpression) SelectBuilder
	GroupBy(fields ...Field) SelectBuilder
	Expression() string
}

type selectBuilder struct {
	whereEx   WhereExpression
	havingEx  HavingExpression
	groupByEx GroupExpression
}

func (s *selectBuilder) Having(expressions ...BooleanExpression) SelectBuilder {
	s.havingEx = s.havingEx.Having(expressions...)
	return s
}

func (s *selectBuilder) GroupBy(fields ...Field) SelectBuilder {
	s.groupByEx = s.groupByEx.GroupBy(fields...)
	return s
}

func (s *selectBuilder) Where(expressions ...BooleanExpression) SelectBuilder {
	s.whereEx = s.whereEx.Where(expressions...)
	return s
}

func (s *selectBuilder) Expression() string {
	var (
		builder = new(strings.Builder)
	)

	// todo handle errors on build

	whereString := s.whereEx.Expression()
	if len(whereString) != 0 {
		builder.WriteString(whereString)
	}

	havingString := s.havingEx.Expression()
	if len(havingString) != 0 {
		builder.WriteString("\n")
		builder.WriteString(havingString)
	}

	groupByString := s.groupByEx.Expression()
	if len(groupByString) != 0 {
		builder.WriteString("\n")
		builder.WriteString(groupByString)
	}

}
