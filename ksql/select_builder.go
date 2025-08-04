package ksql

import (
	"errors"
	"fmt"
	"github.com/gulfstream-h/ksql/internal/schema"
	"github.com/gulfstream-h/ksql/static"
	"strings"
	"sync"
)

type (
	// SelectBuilder - common contract for all SELECT statements
	SelectBuilder interface {
		Expression
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
	}

	// Joiner - common contract for all JOIN operations in SELECT statements
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

	// selectBuilder implements the SelectBuilder interface for constructing SELECT statements
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

	// selectBuilderCtx - context for the select builder
	selectBuilderCtx struct {
		err error
	}
	// selectBuilderRule - defines a rule for validating select statements
	selectBuilderRule struct {
		ruleFn      func(builder *selectBuilder) (valid bool)
		description string
	}
	// returnNameMeta - metadata for the return type mapper
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

	derivedSchemaName = "derived.ksql" // special schema name for derived fields

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

	// 4. Windowed expressions are not allowed in TABLE references
	windowInTable = selectBuilderRule{
		ruleFn: func(builder *selectBuilder) (valid bool) {
			return !(builder.ref == TABLE && builder.windowEx != nil)
		},
		description: `Windowed expressions are not allowed in TABLE references`,
	}

	// 5. EMIT FINAL can be used only with tables
	emitFinalWithTable = selectBuilderRule{
		ruleFn: func(builder *selectBuilder) (valid bool) {
			return !(builder.ref != TABLE && builder.emitFinal)
		},
		description: `EMIT FINAL can be used only with tables`,
	}

	// 6. EMIT FINAL and EMIT CHANGES cannot be used together
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
		windowInTable,
		emitFinalWithTable,
		emitFinalAndChanges,
	}
)

// predefined names for the select builder
var (
	reserved = map[string]struct{}{
		"from.ksql": {}, // default schema name
		"CASE":      {}, // reserved for CASE expressions
	}
)

// newSelectBuilder initializes a new SelectBuilder with default values
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

// Select initializes a new SelectBuilder with the provided fields
func Select(fields ...Field) SelectBuilder {
	sb := newSelectBuilder()

	return sb.Select(fields...)
}

// SelectAsStruct initializes a new SelectBuilder with a struct representation
// from which fields will be extracted
func SelectAsStruct(name string, val any) SelectBuilder {
	sb := newSelectBuilder()
	return sb.SelectStruct(name, val)
}

// EmitFinal sets the select builder to emit final results
func (s *selectBuilder) EmitFinal() SelectBuilder {
	s.emitFinal = true
	return s
}

// aggregated checks if the select builder has aggregated fields or operators
func (s *selectBuilder) aggregated() bool {
	return s.withAggregatedFields() || s.withAggregatedOperators() || s.windowed()
}

// EmitChanges sets the select builder to emit changes
func (s *selectBuilder) EmitChanges() SelectBuilder {
	s.emitChanges = true
	return s
}

// Ref returns the reference type of the select builder
func (s *selectBuilder) Ref() Reference {
	return s.ref
}

// windowed checks if the select builder has a window expression
func (s *selectBuilder) windowed() bool {
	return s.windowEx != nil
}

// Windowed sets a window expression for the select builder
func (s *selectBuilder) Windowed(window WindowExpression) SelectBuilder {
	s.windowEx = window
	return s
}

// SelectStruct creates a select builder from a struct representation
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

// As sets an alias for the select builder
func (s *selectBuilder) As(alias string) SelectBuilder {
	s.alias = alias
	return s
}

// Alias returns the alias of the select builder
func (s *selectBuilder) Alias() string {
	return s.alias
}

// Select adds fields to the select builder
func (s *selectBuilder) Select(fields ...Field) SelectBuilder {
	s.fields = append(s.fields, fields...)

	if static.ReflectionFlag {
		var (
			rels []Relational
			// slice of relation that parsed from derived fields (see Relation interface derived method)
			// inner relations participate only in reflection report
			// and do not in return schema reflection check
			innerRels []Relational
		)

		for idx := range fields {
			rels = append(rels, fields[idx])
			innerRels = append(innerRels, fields[idx].InnerRelations()...)
		}

		for idx := range rels {
			s.processRelation(rels[idx], true)
		}
		for idx := range innerRels {
			s.processRelation(innerRels[idx], false)
		}

	}

	return s
}

// Join adds a JOIN to the select builder
func (s *selectBuilder) Join(
	from FromExpression,
	on Conditional,
) SelectBuilder {
	if len(from.Alias()) != 0 {
		s.virtualSchemas[from.Alias()] = from.Schema()
	}

	return s.join(from, on, Inner)
}

// LeftJoin adds a LEFT JOIN to the select builder
func (s *selectBuilder) LeftJoin(
	from FromExpression,
	on Conditional,
) SelectBuilder {
	if len(from.Alias()) != 0 {
		s.virtualSchemas[from.Alias()] = from.Schema()
	}

	return s.join(from, on, Left)
}

// RightJoin adds a RIGHT JOIN to the select builder
func (s *selectBuilder) RightJoin(
	from FromExpression,
	on Conditional,
) SelectBuilder {

	if len(from.Alias()) != 0 {
		s.virtualSchemas[from.Alias()] = from.Schema()
	}

	return s.join(from, on, Right)
}

// OuterJoin adds an OUTER JOIN to the select builder
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

// add support for join with multiple fields
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

// From sets the FROM clause for the select builder
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

// Having adds HAVING expressions to the select builder
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

// GroupBy adds GROUP BY expressions to the select builder
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

// Where adds WHERE expressions to the select builder
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

// WithCTE adds a Common Table Expression (CTE) to the select builder
func (s *selectBuilder) WithCTE(
	inner SelectBuilder,
) SelectBuilder {
	s.with = append(s.with, inner)
	return s
}

// WithMeta adds metadata to the select builder
func (s *selectBuilder) WithMeta(
	with Metadata,
) SelectBuilder {
	s.meta = with
	return s
}

// OrderBy adds ORDER BY expressions to the select builder
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

// Expression generates the SQL expression for the SELECT statement
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
			builder.WriteString("(")
			builder.WriteString(expression[:len(expression)-1])
			builder.WriteString(")")
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

	if s.windowEx != nil {
		windowString, err := s.windowEx.Expression()
		if err != nil {
			return "", fmt.Errorf("WINDOW expression: %w", err)
		}

		builder.WriteString(" ")
		builder.WriteString(windowString)
	}

	if !s.groupByEx.IsEmpty() {
		groupByString, err := s.groupByEx.Expression()
		if err != nil {
			return "", fmt.Errorf("GROUP BY expression: %w", err)
		}

		builder.WriteString(" ")
		builder.WriteString(groupByString)
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

// Returns - function that returns all fields that were selected in the select builder
// it also checks for fields existence in the relation storage
func (s *selectBuilder) Returns() schema.LintedFields {
	result := schema.NewLintedFields()

	s.buildRelationReport()

	for fieldName, metaSlice := range s.returnTypeMapper {
		for _, meta := range metaSlice {
			if realRel, ok := s.virtualSchemas[meta.relation]; ok {
				// if the relation is aliased, we should use the real schema name
				meta.relation = realRel
			}

			if meta.relation == derivedSchemaName {
				result.Set(schema.SearchField{
					Name:     meta.alias,
					Relation: "",
				})
			}

			rel, ok := s.relationStorage[meta.relation]
			if !ok {
				// if relation is gone
				// there was an aliased storage
				continue
			}

			v, _ := rel.Get(fieldName)

			if len(meta.alias) != 0 {
				v.Name = meta.alias
			}

			if realRel, ok := s.virtualSchemas[meta.relation]; ok {
				v.Relation = realRel
			}

			result.Set(v)
		}
	}

	return result
}

// RelationReport - sets real relation names to aliased fields
// if the reflection flag is enabled. Then it returns all processed fields
func (s *selectBuilder) RelationReport() map[string]schema.LintedFields {
	if static.ReflectionFlag {
		s.buildRelationReport()
		return s.relationStorage
	}
	return nil
}

func (s *selectBuilder) buildRelationReport() {
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
				continue
			}
		}
	})
}

// withAggregatedFields checks if the select builder has any aggregated fields
func (s *selectBuilder) withAggregatedFields() bool {
	for idx := range s.fields {
		_, ok := s.fields[idx].(*aggregatedField)
		if ok {
			return true
		}
	}
	return false
}

// onlyAggregated checks if all fields in the select builder are aggregated fields
func (s *selectBuilder) onlyAggregated() bool {
	for idx := range s.fields {
		if _, ok := s.fields[idx].(*aggregatedField); !ok {
			return false
		}
	}
	return len(s.fields) > 0
}

// withAggregatedOperators checks if the select builder has aggregated operators
func (s *selectBuilder) withAggregatedOperators() bool {
	return s.groupByEx != nil || s.havingEx != nil
}

func (s *selectBuilder) processRelation(rel Relational, returnCheck bool) {
	// if the field is already reserved, we should skip it
	if _, ok := reserved[rel.Column()]; ok {
		return
	}

	// if relation is computing during query
	// it should be added just for return schema
	// and ignored in relation report
	if rel.derived() {

		if len(rel.Alias()) == 0 {
			s.ctx.err = fmt.Errorf("derived field should have an alias")
			return
		}

		meta := returnNameMeta{
			relation: derivedSchemaName,
			alias:    rel.Alias(),
		}

		sl, _ := s.returnTypeMapper[meta.alias]
		sl = append(sl, meta)
		s.returnTypeMapper[meta.alias] = sl

		return
	}

	f := schema.SearchField{
		Name:     rel.Column(),
		Relation: s.parseRelationName(rel),
	}

	s.addSearchField(f)

	// if field participates in query as a parameter for
	// aggregated function, conditional, etc.,
	// it's not necessary to add it to return schema reflection check
	if !returnCheck {
		return
	}

	meta := returnNameMeta{
		relation: f.Relation,
		alias:    rel.Alias(),
	}

	if len(rel.Alias()) > 0 {
		meta.alias = rel.Alias()
	}

	sl, _ := s.returnTypeMapper[f.Name]
	sl = append(sl, meta)
	s.returnTypeMapper[f.Name] = sl
}

// addRelation adds a relation to the relation storage
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

// addSearchField adds a search field to the relation storage
func (s *selectBuilder) addSearchField(
	field schema.SearchField,
) {

	if _, ok := s.virtualSchemas[field.Relation]; ok {
		return
	}
	if s.relationStorage[field.Relation] == nil {
		s.relationStorage[field.Relation] = schema.NewLintedFields()
	}
	s.relationStorage[field.Relation].Set(field)
}

// parseRelationName parses the relation name from the field
func (s *selectBuilder) parseRelationName(f Relational) string {

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

	if len(f.Alias()) > 0 {
		s.virtualColumns[f.Alias()] = f.Schema()
	}
	return f.Schema()
}

// parseSearchFieldsFromCond parses search fields from the conditional expression
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
