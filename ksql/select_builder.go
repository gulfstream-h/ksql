package ksql

import (
	"ksql/schema"
	"ksql/static"
	"reflect"
	"strings"
)

type (
	SelectBuilder interface {
		Joiner

		SchemaFields() []schema.SearchField
		As(alias string) SelectBuilder
		Alias() string
		WithCTE(inner SelectBuilder) SelectBuilder
		WithMeta(with Metadata) SelectBuilder
		Select(fields ...Field) SelectBuilder
		SelectStruct(name string, val reflect.Type) SelectBuilder
		From(schema string) SelectBuilder
		Where(expressions ...Expression) SelectBuilder
		Windowed(window WindowExpression) SelectBuilder
		Having(expressions ...Expression) SelectBuilder
		GroupBy(fields ...Field) SelectBuilder
		OrderBy(expressions ...OrderedExpression) SelectBuilder
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
	}

	selectBuilderContext interface {
		AddFields(fields ...schema.SearchField)
		Fields() []schema.SearchField
	}

	selectBuilder struct {
		ctx selectBuilderContext

		alias     string
		meta      Metadata
		with      []SelectBuilder
		fields    []Field
		joinExs   []JoinExpression
		fromEx    FromExpression
		whereEx   WhereExpression
		windowEx  WindowExpression
		havingEx  HavingExpression
		groupByEx GroupExpression
		orderByEx OrderByExpression
	}

	selectBuilderCtx struct {
		schemaRel []schema.SearchField
	}
)

func (sbc *selectBuilderCtx) AddFields(fields ...schema.SearchField) {
	sbc.schemaRel = append(sbc.schemaRel, fields...)
}

func (sbc *selectBuilderCtx) Fields() []schema.SearchField {
	fields := make([]schema.SearchField, len(sbc.schemaRel))
	copy(fields, sbc.schemaRel)
	return fields
}

func newSelectBuilder() SelectBuilder {
	var (
		ctx selectBuilderContext
	)

	if static.ReflectionFlag {
		ctx = &selectBuilderCtx{}
	}

	return &selectBuilder{
		ctx:       ctx,
		fields:    nil,
		joinExs:   nil,
		fromEx:    NewFromExpression(),
		whereEx:   NewWhereExpression(),
		havingEx:  NewHavingExpression(),
		groupByEx: NewGroupByExpression(),
		orderByEx: NewOrderByExpression(),
	}
}

func Select(fields ...Field) SelectBuilder {
	sb := newSelectBuilder()

	return sb.Select(fields...)
}

func SelectAsStruct(name string, val reflect.Type) SelectBuilder {
	sb := newSelectBuilder()
	return sb.SelectStruct(name, val)
}

func (s *selectBuilder) Windowed(window WindowExpression) SelectBuilder {
	s.windowEx = window
	return s
}

func (s *selectBuilder) SchemaFields() []schema.SearchField {
	if s.ctx == nil {
		return []schema.SearchField{}
	}
	return s.ctx.Fields()
}

func (s *selectBuilder) SelectStruct(name string, val reflect.Type) SelectBuilder {
	structFields := schema.ParseReflectStructToFields(val.Name(), val)

	if s.ctx != nil {
		s.ctx.AddFields(structFields...)
	}

	fields := make([]Field, 0, len(structFields))

	for i := range structFields {
		f := field{
			schema: name,
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

	structFields := make([]schema.SearchField, 0, len(fields))
	for idx := range fields {
		f := schema.SearchField{
			Name:     fields[idx].Column(),
			Relation: fields[idx].Schema(),
		}
		structFields = append(structFields, f)
	}

	if s.ctx != nil {
		s.ctx.AddFields(structFields...)
	}

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

func (s *selectBuilder) From(sch string) SelectBuilder {
	//if s.ctx != nil {
	//	t := schema.SerializeProvidedStruct(sch)
	//	fieldMap := schema.ParseStructToFieldsDictionary(sch, t)
	//	for _, field := range fieldMap {
	//		s.ctx.AddFields(field)
	//	}
	//}

	s.fromEx = s.fromEx.From(sch)
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

func (s *selectBuilder) OrderBy(expressions ...OrderedExpression) SelectBuilder {
	s.orderByEx.OrderBy(expressions...)
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

	builder.WriteString(" ")
	builder.WriteString(fromString)

	for idx := range s.joinExs {
		expression, ok := s.joinExs[idx].Expression()
		if !ok {
			return "", false
		}

		builder.WriteString(" ")
		builder.WriteString(expression)
	}

	if !s.whereEx.IsEmpty() {
		whereString, ok := s.whereEx.Expression()
		if !ok {
			return "", false
		}

		builder.WriteString(" ")
		builder.WriteString(whereString)
	}

	if s.windowEx != nil {
		windowString, ok := s.windowEx.Expression()
		if !ok {
			return "", false
		}

		builder.WriteString(" ")
		builder.WriteString(windowString)
	}

	if !s.groupByEx.IsEmpty() {
		groupByString, ok := s.groupByEx.Expression()
		if !ok {
			return "", false
		}

		builder.WriteString(" ")
		builder.WriteString(groupByString)
	}

	if !s.havingEx.IsEmpty() {
		havingString, ok := s.havingEx.Expression()
		if !ok {
			return "", false
		}
		builder.WriteString(" ")
		builder.WriteString(havingString)
	}

	if !s.orderByEx.IsEmpty() {
		orderByString, ok := s.orderByEx.Expression()
		if !ok {
			return "", false
		}

		builder.WriteString(" ")
		builder.WriteString(orderByString)
	}

	builder.WriteString(s.meta.Expression())
	builder.WriteString(";")

	return builder.String(), true
}
