package ksql

import (
	"errors"
	"fmt"
	"ksql/schema"
	"ksql/static"
	"reflect"
	"strings"
)

type (
	SelectBuilder interface {
		Joiner
		aggregated() bool
		windowed() bool

		SchemaFields() []schema.SearchField
		As(alias string) SelectBuilder
		Ref() Reference
		Alias() string
		WithCTE(inner SelectBuilder) SelectBuilder
		WithMeta(with Metadata) SelectBuilder
		Select(fields ...Field) SelectBuilder
		SelectStruct(name string, val reflect.Type) SelectBuilder
		From(schema string, reference Reference) SelectBuilder
		Where(expressions ...Expression) SelectBuilder
		Windowed(window WindowExpression) SelectBuilder
		Having(expressions ...Expression) SelectBuilder
		GroupBy(fields ...Field) SelectBuilder
		OrderBy(expressions ...OrderedExpression) SelectBuilder
		EmitChanges() SelectBuilder
		EmitFinal() SelectBuilder
		Expression() (string, error)
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
		ctx         selectBuilderContext
		ref         Reference
		emitChanges bool
		emitFinal   bool

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

	selectBuilderRule struct {
		ruleFn      func(builder *selectBuilder) (valid bool)
		description string
	}
)

// BUILDER RULES
// These rules are used to validate the select builder before generating the SQL expression.
// They ensure that the generated SQL is valid according to KSQL syntax and semantics.
var (
	// 1. GROUP BY requires WINDOW clause on streams
	groupByWindowed = selectBuilderRule{
		ruleFn: func(builder *selectBuilder) (valid bool) {
			return !(builder.ref == STREAM && builder.windowEx == nil && !builder.emitChanges)
		},
		description: `GROUP BY requires WINDOW clause on streams`,
	}

	// 2. No HAVING without GROUP BY
	havingWithGroupBy = selectBuilderRule{
		ruleFn: func(builder *selectBuilder) (valid bool) {
			return !(!builder.havingEx.IsEmpty() && builder.groupByEx.IsEmpty())
		},
		description: `HAVING clause requires GROUP BY clause`,
	}

	// 3. aggregated functions should be used with GROUP BY clause
	aggregatedWithGroupBy = selectBuilderRule{
		ruleFn: func(builder *selectBuilder) (valid bool) {
			return !(builder.withAggregatedFields() && builder.groupByEx.IsEmpty() && builder.onlyAggregated())
		},
		description: `Aggregated functions require GROUP BY clause`,
	}

	// 4. EMIT CHANGES can be used only with streams
	emitChangesWithStream = selectBuilderRule{
		ruleFn: func(builder *selectBuilder) (valid bool) {
			return !(builder.ref != STREAM && builder.emitChanges)
		},
		description: `EMIT CHANGES can be used only with streams`,
	}

	// 5. Windowed expressions are not allowed in TABLE references
	windowInTable = selectBuilderRule{
		ruleFn: func(builder *selectBuilder) (valid bool) {
			return !(builder.ref == TABLE && builder.windowEx != nil)
		},
		description: `Windowed expressions are not allowed in TABLE references`,
	}

	// 6. EMIT FINAL can be used only with tables
	emitFinalWithTable = selectBuilderRule{
		ruleFn: func(builder *selectBuilder) (valid bool) {
			return !(builder.ref != TABLE && builder.emitFinal)
		},
		description: `EMIT FINAL can be used only with tables`,
	}

	// 7. EMIT FINAL and EMIT CHANGES cannot be used together
	emitFinalAndChanges = selectBuilderRule{
		ruleFn: func(builder *selectBuilder) (valid bool) {
			return !(builder.emitFinal && builder.emitChanges)
		},
		description: `EMIT FINAL and EMIT CHANGES cannot be used together`,
	}

	selectRuleSet = []selectBuilderRule{
		groupByWindowed,
		havingWithGroupBy,
		aggregatedWithGroupBy,
		emitChangesWithStream,
		windowInTable,
		emitFinalWithTable,
		emitFinalAndChanges,
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

func (s *selectBuilder) EmitFinal() SelectBuilder {
	s.emitFinal = true
	return s
}

func (s *selectBuilder) aggregated() bool {
	return s.withAggregatedFields() || s.withAggregatedOperators() || s.windowed()
}

func (s *selectBuilder) EmitChanges() SelectBuilder {
	s.emitChanges = true
	return s
}

func (s *selectBuilder) Ref() Reference {
	return s.ref
}

func (s *selectBuilder) windowed() bool {
	return s.windowEx != nil
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

func (s *selectBuilder) From(schema string, reference Reference) SelectBuilder {
	s.ref = reference
	//if s.ctx != nil {
	//	t := schema.SerializeProvidedStruct(sch)
	//	fieldMap := schema.ParseStructToFieldsDictionary(sch, t)
	//	for _, field := range fieldMap {
	//		s.ctx.AddFields(field)
	//	}
	//}

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

func (s *selectBuilder) OrderBy(expressions ...OrderedExpression) SelectBuilder {
	s.orderByEx.OrderBy(expressions...)
	return s
}

func (s *selectBuilder) withAggregatedFields() bool {
	for idx := range s.fields {
		_, ok := s.fields[idx].(*aggregatedField)
		if ok {
			return true
		}
	}
	return false
}

func (s *selectBuilder) onlyAggregated() bool {
	for idx := range s.fields {
		if _, ok := s.fields[idx].(*aggregatedField); !ok {
			return false
		}
	}
	return len(s.fields) > 0
}

func (s *selectBuilder) withAggregatedOperators() bool {
	return s.groupByEx != nil || s.havingEx != nil
}

func (s *selectBuilder) Expression() (string, error) {
	var (
		builder      = new(strings.Builder)
		cteIsFirst   = true
		fieldIsFirst = true
	)

	// validate reference
	switch s.ref {
	case TABLE, STREAM, TOPIC:
	default:
		return "", errors.New("invalid reference type for select builder, must be TABLE, STREAM or TOPIC")
	}

	// validate build rules
	for idx := range selectRuleSet {
		if !selectRuleSet[idx].ruleFn(s) {
			return "", fmt.Errorf("invalid select builder: %s", selectRuleSet[idx].description)
		}
	}

	// write CTEs recursively
	if len(s.with) > 0 {
		for i := range s.with {
			alias := s.with[i].Alias()
			if len(alias) == 0 {
				return "", fmt.Errorf("invalid CTE alias: %s", alias)
			}

			expression, err := s.with[i].Expression()
			if err != nil {
				return "", fmt.Errorf("CTE expression: %s", expression)
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

	// SELECT ...fields section
	if len(s.fields) == 0 {
		return "", errors.New("no fields selected, use Select() method to add fields")
	}

	builder.WriteString("SELECT ")
	for idx := range s.fields {
		expression, err := s.fields[idx].Expression()
		if err != nil {
			return "", fmt.Errorf("field expression: %w", err)
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

	fromString, err := s.fromEx.Expression()
	if err != nil {
		return "", fmt.Errorf("FROM expression: %w", err)
	}

	builder.WriteString(" ")
	builder.WriteString(fromString)

	for idx := range s.joinExs {
		expression, err := s.joinExs[idx].Expression()
		if err != nil {
			return "", fmt.Errorf("JOIN expression: %s", expression)
		}

		builder.WriteString(" ")
		builder.WriteString(expression)
	}

	if !s.whereEx.IsEmpty() {
		whereString, err := s.whereEx.Expression()
		if err != nil {
			return "", fmt.Errorf("WHERE expression: %w", err)
		}

		builder.WriteString(" ")
		builder.WriteString(whereString)
	}

	if !s.groupByEx.IsEmpty() {
		groupByString, err := s.groupByEx.Expression()
		if err != nil {
			return "", fmt.Errorf("GROUP BY expression: %w", err)
		}

		builder.WriteString(" ")
		builder.WriteString(groupByString)
	}

	if s.windowEx != nil {
		windowString, err := s.windowEx.Expression()
		if err != nil {
			return "", fmt.Errorf("WINDOW expression: %w", err)
		}

		builder.WriteString(" ")
		builder.WriteString(windowString)
	}

	if !s.havingEx.IsEmpty() {
		havingString, err := s.havingEx.Expression()
		if err != nil {
			return "", fmt.Errorf("HAVING expression: %w", err)
		}
		builder.WriteString(" ")
		builder.WriteString(havingString)
	}

	if !s.orderByEx.IsEmpty() {
		orderByString, err := s.orderByEx.Expression()
		if err != nil {
			return "", fmt.Errorf("ORDER BY expression: %w", err)
		}

		builder.WriteString(" ")
		builder.WriteString(orderByString)
	}

	if s.emitChanges {
		builder.WriteString(" EMIT CHANGES")
	}

	if s.emitFinal {
		builder.WriteString(" EMIT FINAL")
	}

	builder.WriteString(s.meta.Expression())
	builder.WriteString(";")

	return builder.String(), nil
}
