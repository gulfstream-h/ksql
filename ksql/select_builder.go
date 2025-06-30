package ksql

import (
	"errors"
	"fmt"
	"ksql/schema"
	"ksql/static"
	"strings"
	"sync"
)

type (
	SelectBuilder interface {
		Joiner
		aggregated() bool
		windowed() bool

		Returns() schema.LintedFields
		RelationReport() map[string]schema.LintedFields

		As(alias string) SelectBuilder
		Ref() Reference
		Alias() string
		WithCTE(inner SelectBuilder) SelectBuilder
		WithMeta(with Metadata) SelectBuilder
		Select(fields ...Field) SelectBuilder
		SelectStruct(name string, val any) SelectBuilder
		From(from FromExpression) SelectBuilder
		Where(expressions ...Conditional) SelectBuilder
		Windowed(window WindowExpression) SelectBuilder
		Having(expressions ...Conditional) SelectBuilder
		GroupBy(fields ...Field) SelectBuilder
		OrderBy(expressions ...OrderedExpression) SelectBuilder
		EmitChanges() SelectBuilder
		EmitFinal() SelectBuilder
		Expression() (string, error)
	}

	Joiner interface {
		LeftJoin(
			from FromExpression,
			on Conditional,
		) SelectBuilder
		Join(
			from FromExpression,
			on Conditional,
		) SelectBuilder
		RightJoin(
			from FromExpression,
			on Conditional,
		) SelectBuilder
		OuterJoin(
			from FromExpression,
			on Conditional,
		) SelectBuilder
	}

	selectBuilder struct {
		ctx         selectBuilderCtx
		ref         Reference
		emitChanges bool
		emitFinal   bool

		// relationStorage contains all relations that were added to the select builder
		// it is used to validate reflection when static.ReflectionFlag is enabled
		relationStorage map[string]schema.LintedFields

		// virtualSchemas contains pairs of alias and schema name
		// used to resolve schema names in the select builder
		// and  returning relation with real schema names
		virtualSchemas map[string]string

		// virtualColumns contains pairs if alias and column name
		// it is used to resolve virtual columns in the select builder
		virtualColumns map[string]string

		// aliasRefresher update defaultSchemaName to the real schema name
		// received from From() method immediately when RelationStorage() call
		aliasRefresher sync.Once

		returnTypeMapper map[string][]returnNameMeta

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
		err error
	}

	selectBuilderRule struct {
		ruleFn      func(builder *selectBuilder) (valid bool)
		description string
	}

	returnNameMeta struct {
		relation string
		alias    string
	}
)

const (
	// defaultSchemaName is used for fields that were added
	// before the From() method call without any schema or schema alias provided
	// once RelationReport() is called, the schema name will be replaced with the actual schema name
	defaultSchemaName = "from.ksql"
)

var (
	// BUILDER RULES
	// These rules are used to validate the select builder before generating the SQL expression.
	// They ensure that the generated SQL is valid according to KSQL syntax and semantics.

	// 1. GROUP BY requires WINDOW clause on streams
	groupByWindowed = selectBuilderRule{
		ruleFn: func(builder *selectBuilder) (valid bool) {
			return !(builder.ref == STREAM && !builder.groupByEx.IsEmpty() && builder.windowEx == nil && !builder.emitChanges)
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

func newSelectBuilder() SelectBuilder {

	return &selectBuilder{
		ctx:              selectBuilderCtx{},
		fields:           nil,
		joinExs:          nil,
		fromEx:           NewFromExpression(),
		whereEx:          NewWhereExpression(),
		havingEx:         NewHavingExpression(),
		groupByEx:        NewGroupByExpression(),
		orderByEx:        NewOrderByExpression(),
		relationStorage:  make(map[string]schema.LintedFields),
		virtualSchemas:   make(map[string]string),
		virtualColumns:   make(map[string]string),
		returnTypeMapper: make(map[string][]returnNameMeta),
	}
}

func Select(fields ...Field) SelectBuilder {
	sb := newSelectBuilder()

	return sb.Select(fields...)
}

func SelectAsStruct(name string, val any) SelectBuilder {
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

func (s *selectBuilder) SelectStruct(name string, val any) SelectBuilder {
	relation, err := schema.NativeStructRepresentation(name, val)
	if err != nil {
		s.ctx.err = fmt.Errorf("cannot create relation from struct: %w", err)
		return s
	}

	fieldsList := relation.Array()

	fields := make([]Field, 0, len(fieldsList))

	for i := range fieldsList {
		f := field{
			schema: name,
			col:    fieldsList[i].Name,
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

	if static.ReflectionFlag {
		for idx := range fields {
			f := schema.SearchField{
				Name:     fields[idx].Column(),
				Relation: s.parseRelationName(fields[idx]),
			}

			s.addSearchField(f)

			meta := returnNameMeta{
				relation: f.Relation,
				alias:    fields[idx].Alias(),
			}

			if len(fields[idx].Alias()) > 0 {
				meta.alias = fields[idx].Alias()
			}

			sl, _ := s.returnTypeMapper[f.Name]
			sl = append(sl, meta)
			s.returnTypeMapper[f.Name] = sl
		}

	}

	return s
}

func (s *selectBuilder) Join(
	from FromExpression,
	on Conditional,
) SelectBuilder {
	if len(from.Alias()) != 0 {
		s.virtualSchemas[from.Alias()] = from.Schema()
	}

	return s.join(from, on, Inner)
}

func (s *selectBuilder) LeftJoin(
	from FromExpression,
	on Conditional,
) SelectBuilder {
	if len(from.Alias()) != 0 {
		s.virtualSchemas[from.Alias()] = from.Schema()
	}

	return s.join(from, on, Left)
}

func (s *selectBuilder) RightJoin(
	from FromExpression,
	on Conditional,
) SelectBuilder {

	if len(from.Alias()) != 0 {
		s.virtualSchemas[from.Alias()] = from.Schema()
	}

	return s.join(from, on, Right)
}

func (s *selectBuilder) OuterJoin(
	from FromExpression,
	on Conditional,
) SelectBuilder {
	if len(from.Alias()) != 0 {
		s.virtualSchemas[from.Alias()] = from.Schema()
	}
	return s.join(from, on, Outer)
}

// todo:
//  make join conditional ?

func (s *selectBuilder) join(
	schemaName FromExpression,
	on Conditional,
	joinType JoinType,
) SelectBuilder {

	if static.ReflectionFlag {
		fields := s.parseSearchFieldsFromCond(on)
		for idx := range fields {
			s.addSearchField(fields[idx])
		}
	}

	// append join expression to the select builder
	s.joinExs = append(s.joinExs, Join(schemaName, on, joinType))
	return s
}

func (s *selectBuilder) From(from FromExpression) SelectBuilder {
	s.ref = from.Ref()
	s.fromEx = from

	if len(s.fromEx.Alias()) != 0 {
		s.virtualSchemas[s.fromEx.Alias()] = s.fromEx.Schema()
	}

	// fields that was added to the select builder
	// before the From() method call has alias defaultSchemaName
	// to prevent conflicts with other schemas
	// we should add the defaultSchemaName schema to the alias mapper
	s.virtualSchemas[defaultSchemaName] = s.fromEx.Schema()
	return s
}

func (s *selectBuilder) Having(expressions ...Conditional) SelectBuilder {
	if static.ReflectionFlag {
		// for every expression try parse Field
		// and add them to the relation storage
		for idx := range expressions {
			fields := s.parseSearchFieldsFromCond(expressions[idx])
			for idx := range fields {
				s.addSearchField(fields[idx])
			}
		}
	}
	s.havingEx = s.havingEx.Having(expressions...)
	return s
}

func (s *selectBuilder) GroupBy(fields ...Field) SelectBuilder {
	if static.ReflectionFlag {
		for idx := range fields {
			// parse relation name from the field
			relationName := s.parseRelationName(fields[idx])
			if len(relationName) == 0 {
				continue
			}
			s.addSearchField(schema.SearchField{
				Name:     fields[idx].Column(),
				Relation: relationName,
			})
		}
	}
	s.groupByEx = s.groupByEx.GroupBy(fields...)
	return s
}

func (s *selectBuilder) Where(expressions ...Conditional) SelectBuilder {
	if static.ReflectionFlag {
		// for every expression try parse Field
		// and add them to the relation storage
		for idx := range expressions {
			fields := s.parseSearchFieldsFromCond(expressions[idx])
			for idx := range fields {
				s.addSearchField(fields[idx])
			}
		}
	}
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
	if static.ReflectionFlag {
		for idx := range expressions {
			field := expressions[idx].Field()
			if field == nil {
				continue
			}

			relationName := s.parseRelationName(field)
			if len(relationName) == 0 {
				continue
			}
			s.addSearchField(schema.SearchField{
				Name:     field.Column(),
				Relation: relationName,
			})
		}
	}
	s.orderByEx.OrderBy(expressions...)
	return s
}

func (s *selectBuilder) Expression() (string, error) {
	var (
		builder      = new(strings.Builder)
		cteIsFirst   = true
		fieldIsFirst = true
	)

	if s.ctx.err != nil {
		return "", fmt.Errorf("select builder error: %w", s.ctx.err)
	}

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
	metaExpression := s.meta.Expression()
	if len(metaExpression) > 0 {
		builder.WriteString(" " + s.meta.Expression())
	}
	builder.WriteString(";")

	return builder.String(), nil
}

func (s *selectBuilder) Returns() schema.LintedFields {
	result := schema.NewLintedFields()

	for fieldName, metaSlice := range s.returnTypeMapper {
		for _, meta := range metaSlice {
			rel, ok := s.relationStorage[meta.relation]
			if !ok {
				// if relation is gone
				// there was a aliased storage
				continue
			}

			v, _ := rel.Get(fieldName)

			if len(meta.alias) != 0 {
				v.Name = meta.alias
				v.Relation = ""
			}

			if realRel, ok := s.virtualSchemas[meta.relation]; ok {
				v.Relation = realRel
			}

			result.Set(v)
		}
	}

	return result
}

func (s *selectBuilder) RelationReport() map[string]schema.LintedFields {
	if static.ReflectionFlag {
		if s.fromEx.Schema() == "orders" && s.fromEx.Alias() == "o" {
			fmt.Println("here")

			fmt.Println(s.returnTypeMapper)
			fmt.Println()
			fmt.Println()
			fmt.Println(s.virtualSchemas)
			fmt.Println()
			fmt.Println()
			fmt.Println(s.virtualColumns)
			fmt.Println()
			fmt.Println()
			fmt.Println(s.relationStorage)
			fmt.Println()
		}

		s.aliasRefresher.Do(func() {
			// update defaultSchemaName to the real schema name
			// received from From() method
			if len(s.fromEx.Schema()) > 0 {
				s.virtualSchemas[defaultSchemaName] = s.fromEx.Schema()
			}
			// refresh relation storage with real schema names
			for alias, schemaName := range s.virtualSchemas {

				// if we have a relation with the alias,
				// we should replace it with the real schema name
				// and remove the alias from the relation storage
				if _, exists := s.relationStorage[alias]; exists {
					s.relationStorage[schemaName] = s.relationStorage[alias]
					for _, v := range s.relationStorage[schemaName].Map() {
						v.Relation = schemaName
						s.relationStorage[schemaName].Set(v)
					}
					delete(s.relationStorage, alias)
					fmt.Println(s.relationStorage)
					continue
				}
			}
		})
		return s.relationStorage
	}
	return nil
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

func (s *selectBuilder) addRelation(
	relationName string,
	relation schema.LintedFields,
) {
	if relation == nil {
		return
	}

	if _, exists := s.relationStorage[relationName]; exists {
		for _, field := range relation.Map() {
			s.relationStorage[relationName].Set(field)
		}
		return
	}

	s.relationStorage[relationName] = relation
}

func (s *selectBuilder) addSearchField(
	field schema.SearchField,
) {
	if field.Relation == "" {
		return
	}

	if _, ok := s.virtualSchemas[field.Relation]; ok {
		return
	}
	if s.relationStorage[field.Relation] == nil {
		s.relationStorage[field.Relation] = schema.NewLintedFields()
	}
	s.relationStorage[field.Relation].Set(field)
}

func (s *selectBuilder) parseRelationName(f Field) string {
	if len(f.Alias()) > 0 {
		s.virtualColumns[f.Alias()] = f.Schema()
		return f.Schema()
	}

	if len(f.Schema()) == 0 {
		// check column is already an alias
		_, exist := s.virtualColumns[f.Column()]
		if exist {
			return ""
		}

		//if schema is not set, we should use defaultSchemaName
		//to prevent conflicts with other schemas
		s.virtualSchemas[f.Column()] = defaultSchemaName
		return defaultSchemaName
	}
	return f.Schema()
}

func (s *selectBuilder) parseSearchFieldsFromCond(
	cond Conditional,
) []schema.SearchField {
	result := make([]schema.SearchField, 0, 2)

	// parse relation name from the left side of the join condition
	for _, f := range cond.Left() {
		if f == nil {
			continue
		}

		relationName := s.parseRelationName(f)
		if len(relationName) == 0 {
			continue
		}

		result = append(result, schema.SearchField{
			Name:     f.Column(),
			Relation: relationName,
		})
	}

	for _, right := range cond.Right() {
		if right == nil {
			continue
		}

		// if the right side of the join condition is a field,
		// we should parse the relation name from it
		if rightField, ok := right.(Field); ok {
			relationName := s.parseRelationName(rightField)
			if len(relationName) == 0 {
				continue
			}
			result = append(result, schema.SearchField{
				Name:     rightField.Column(),
				Relation: relationName,
			})
		}
	}

	return result
}
