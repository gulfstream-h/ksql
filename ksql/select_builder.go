package ksql

import "strings"

type (
	SelectBuilder interface {
		Joiner

		As(alias string) SelectBuilder
		Alias() string
		Select(fields ...Field) SelectBuilder
		From(schema string) SelectBuilder
		Where(expressions ...BooleanExpression) SelectBuilder
		Having(expressions ...BooleanExpression) SelectBuilder
		GroupBy(fields ...Field) SelectBuilder
		Expression() string
	}

	Joiner interface {
		LeftJoin(
			schema string,
			on BooleanExpression,
		) SelectBuilder
		Join(
			schema string,
			on BooleanExpression,
		) SelectBuilder
		RightJoin(
			schema string,
			on BooleanExpression,
		) SelectBuilder
		OuterJoin(
			schema string,
			on BooleanExpression,
		) SelectBuilder
		CrossJoin(
			schema string,
			on BooleanExpression,
		) SelectBuilder
	}
)

type selectBuilder struct {
	alias     string
	with      []SelectBuilder
	fields    []Field
	fromEx    FromExpression
	joinEx    JoinExpression
	whereEx   WhereExpression
	havingEx  HavingExpression
	groupByEx GroupExpression
}

func newSelectBuilder() *selectBuilder {
	return &selectBuilder{
		fields:    nil,
		joinEx:    Join("", nil, -1),
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

func (s *selectBuilder) As(alias string) SelectBuilder {
	s.alias = alias
	return s
}

func (s *selectBuilder) Alias() string {
	return s.alias
}

func (s *selectBuilder) Select(fields ...Field) SelectBuilder {
	s.fields = append(s.fields, fields...)
	return s
}

func (s *selectBuilder) Join(
	schema string,
	on BooleanExpression,
) SelectBuilder {
	s.joinEx = Join(schema, on, Inner)
	return s
}

func (s *selectBuilder) LeftJoin(
	schema string,
	on BooleanExpression,
) SelectBuilder {
	s.joinEx = Join(schema, on, Left)
	return s
}

func (s *selectBuilder) RightJoin(
	schema string,
	on BooleanExpression,
) SelectBuilder {
	s.joinEx = Join(schema, on, Right)
	return s
}

func (s *selectBuilder) OuterJoin(
	schema string,
	on BooleanExpression,
) SelectBuilder {
	s.joinEx = Join(schema, on, Outer)
	return s
}

func (s *selectBuilder) CrossJoin(
	schema string,
	on BooleanExpression,
) SelectBuilder {
	s.joinEx = Join(schema, on, Cross)
	return s
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

func (s *selectBuilder) With(
	inner SelectBuilder,
) SelectBuilder {
	s.with = append(s.with, inner)
	return s
}

func (s *selectBuilder) Expression() string {
	var (
		builder      = new(strings.Builder)
		cteIsFirst   = true
		fieldIsFirst = true
	)

	// write CTEs recursively
	if len(s.with) > 0 {
		for i := range s.with {
			alias := s.with[i].Alias()
			if len(alias) == 0 {
				// todo: add error
				return ""
			}

			expression := s.with[i].Expression()
			if len(expression) == 0 {
				// todo: add error
				return ""
			}

			if i != len(s.with)-1 && !cteIsFirst {
				builder.WriteString(",")
			}

			builder.WriteString(alias)
			builder.WriteString(" AS ")
			builder.WriteString("(\n")
			builder.WriteString(expression)
			builder.WriteString("\n)")
			cteIsFirst = false

		}
	}

	// SELECT ..fields section
	if len(s.fields) == 0 {
		// todo add err
		return ""
	}

	builder.WriteString("SELECT ")
	for idx := range s.fields {
		expression := s.fields[idx].Expression()
		if len(expression) == 0 {
			// todo add err
			return ""
		}

		if idx != len(s.fields) && !fieldIsFirst {
			builder.WriteString(", ")
		}
		builder.WriteString(expression)
		fieldIsFirst = false

		alias := s.fields[idx].Alias()
		if len(alias) > 0 {
			builder.WriteString(" AS ")
			builder.WriteString(alias)
		}

	}

	fromString := s.fromEx.Expression()
	if len(fromString) == 0 {
		// todo add err
		return ""
	}

	builder.WriteString("\n")
	builder.WriteString(fromString)

	// todo handle errors on build

	whereString := s.whereEx.Expression()
	if len(whereString) != 0 {
		builder.WriteString("\n")
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
