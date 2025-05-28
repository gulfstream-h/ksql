package ksql

import (
	"ksql/schema"
	"reflect"
	"strings"
)

type (
	SelectBuilder interface {
		Joiner

		As(alias string) SelectBuilder
		Alias() string
		WithCTE(inner SelectBuilder) SelectBuilder
		Select(fields ...Field) SelectBuilder
		SelectStruct(val any) SelectBuilder
		From(schema string) SelectBuilder
		Where(expressions ...Expression) SelectBuilder
		Having(expressions ...Expression) SelectBuilder
		GroupBy(fields ...Field) SelectBuilder
		Expression() (string, bool)
	}

	Joiner interface {
		LeftJoin(
			schema string,
			on Expression,
		) SelectBuilder
		Join(
			schema string,
			on Expression,
		) SelectBuilder
		RightJoin(
			schema string,
			on Expression,
		) SelectBuilder
		OuterJoin(
			schema string,
			on Expression,
		) SelectBuilder
		CrossJoin(
			schema string,
			on Expression,
		) SelectBuilder
	}
)

type selectBuilder struct {
	alias     string
	meta      Metadata
	with      []SelectBuilder
	fields    []Field
	joinExs   []JoinExpression
	fromEx    FromExpression
	whereEx   WhereExpression
	havingEx  HavingExpression
	groupByEx GroupExpression
}

func newSelectBuilder() *selectBuilder {
	return &selectBuilder{
		fields:    nil,
		joinExs:   nil,
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

func (s *selectBuilder) SelectStruct(val any) SelectBuilder {
	t := reflect.TypeOf(val)
	structFields := schema.ParseStructToFields(t.Name(), t)

	fields := make([]Field, len(structFields))

	for i := range structFields {
		f := field{
			schema: structFields[i].Relation,
			col:    structFields[i].Name,
		}
		fields = append(fields, &f)
	}

	return s.Select(fields...)

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
	on Expression,
) SelectBuilder {
	s.joinExs = append(s.joinExs, Join(schema, on, Inner))
	return s
}

func (s *selectBuilder) LeftJoin(
	schema string,
	on Expression,
) SelectBuilder {
	s.joinExs = append(s.joinExs, Join(schema, on, Left))
	return s
}

func (s *selectBuilder) RightJoin(
	schema string,
	on Expression,
) SelectBuilder {
	s.joinExs = append(s.joinExs, Join(schema, on, Right))
	return s
}

func (s *selectBuilder) OuterJoin(
	schema string,
	on Expression,
) SelectBuilder {
	s.joinExs = append(s.joinExs, Join(schema, on, Outer))
	return s
}

func (s *selectBuilder) CrossJoin(
	schema string,
	on Expression,
) SelectBuilder {
	s.joinExs = append(s.joinExs, Join(schema, on, Cross))
	return s
}

func (s *selectBuilder) From(schema string) SelectBuilder {
	s.fromEx = s.fromEx.From(schema)
	return s
}

func (s *selectBuilder) Having(expressions ...Expression) SelectBuilder {
	s.havingEx = s.havingEx.Having(expressions...)
	return s
}

func (s *selectBuilder) GroupBy(fields ...Field) SelectBuilder {
	s.groupByEx = s.groupByEx.GroupBy(fields...)
	return s
}

func (s *selectBuilder) Where(expressions ...Expression) SelectBuilder {
	s.whereEx = s.whereEx.Where(expressions...)
	return s
}

func (s *selectBuilder) WithCTE(
	inner SelectBuilder,
) SelectBuilder {
	s.with = append(s.with, inner)
	return s
}

func (s *selectBuilder) WithMeta(
	with Metadata,
) SelectBuilder {
	s.meta = with
	return s
}

func (s *selectBuilder) Expression() (string, bool) {
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
				return "", false
			}

			expression, ok := s.with[i].Expression()
			if !ok {
				return "", false
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
		return "", false
	}

	builder.WriteString("SELECT ")
	for idx := range s.fields {
		expression, ok := s.fields[idx].Expression()
		if !ok {
			return "", false
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

	fromString, ok := s.fromEx.Expression()
	if !ok {
		return "", false
	}

	builder.WriteString("\n")
	builder.WriteString(fromString)

	for idx := range s.joinExs {
		expression, ok := s.joinExs[idx].Expression()
		if !ok {
			return "", false
		}

		builder.WriteString("\n")
		builder.WriteString(expression)
	}

	if !s.whereEx.IsEmpty() {
		whereString, ok := s.whereEx.Expression()
		if !ok {
			return "", false
		}

		builder.WriteString("\n")
		builder.WriteString(whereString)
	}

	if !s.havingEx.IsEmpty() {
		havingString, ok := s.havingEx.Expression()
		if !ok {
			return "", false
		}
		builder.WriteString("\n")
		builder.WriteString(havingString)
	}

	if !s.groupByEx.IsEmpty() {
		groupByString, ok := s.groupByEx.Expression()
		if !ok {
			return "", false
		}

		builder.WriteString("\n")
		builder.WriteString(groupByString)
	}

	builder.WriteString(s.meta.Expression())

	return builder.String(), true
}
