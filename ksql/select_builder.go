package ksql

import "strings"

type SelectBuilder interface {
	From(schema string) SelectBuilder
	Where(expressions ...BooleanExpression) SelectBuilder
	Having(expressions ...BooleanExpression) SelectBuilder
	GroupBy(fields ...Field) SelectBuilder
	Expression() string
}

type selectBuilder struct {
	fields    []Field
	fromEx    FromExpression
	whereEx   WhereExpression
	havingEx  HavingExpression
	groupByEx GroupExpression
}

func newSelectBuilder() *selectBuilder {
	return &selectBuilder{
		fields:    nil,
		fromEx:    NewFromExpression(),
		whereEx:   NewWhereExpression(),
		havingEx:  NewHavingExpression(),
		groupByEx: NewGroupByExpression(),
	}
}

func Select(fields ...Field) SelectBuilder {
	sb := newSelectBuilder()

	sb.fields = make([]Field, len(fields))
	copy(sb.fields, fields)

	return sb
}

func (sb *selectBuilder) Select(fields ...Field) SelectBuilder {
	sb.fields = append(sb.fields, fields...)
	return sb
}

func (s *selectBuilder) From(schema string) SelectBuilder {
	s.fromEx = s.fromEx.From(schema)
	return s
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
	return builder.String()
}
