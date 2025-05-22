package ksql

import (
	"ksql/schema"
	"ksql/static"
	"strings"
)

type (
	Builder int
)

var (
	SelectBuilder Builder
)

type builderContext struct {
	query     Query
	fields    FullSchema
	join      Join
	cond      Cond
	groupedBy []string
	with      With
	cte       []static.QueryPlan
}

type cteLayer struct {
	ctx *builderContext
}

type fieldsLayer struct {
	ctx *builderContext
}

type joinLayer struct {
	ctx *builderContext
}

type fromLayer struct {
	ctx *builderContext
}

type condLayer struct {
	ctx *builderContext
}

type metadataLayer struct {
	ctx *builderContext
}

func (sb Builder) WithCTE(ctes ...static.QueryPlan) cteLayer {
	return cteLayer{
		ctx: &builderContext{
			cte: ctes,
		},
	}
}

func (sb Builder) Fields(fields ...string) fieldsLayer {
	var (
		searchFields []schema.SearchField
	)

	for _, field := range fields {
		searchField := schema.SearchField{}

		if strings.Contains(field, ".") {
			parts := strings.Split(field, ".")
			if len(parts) != 2 {
				continue
			}
			searchField.Name = parts[1]
			searchField.Relation = parts[0]
		} else {
			searchField.Name = field
		}

		searchFields = append(searchFields, searchField)
	}

	return fieldsLayer{
		ctx: &builderContext{
			fields: FullSchema{fields: searchFields},
		},
	}
}

func (cl cteLayer) Fields(fields ...string) fieldsLayer {
	var (
		searchFields []schema.SearchField
	)

	for _, field := range fields {
		searchField := schema.SearchField{}

		if strings.Contains(field, ".") {
			parts := strings.Split(field, ".")

			if len(parts) != 2 {
				continue
			}

			searchField.Name = parts[1]
			searchField.Relation = parts[0]
		} else {
			searchField.Name = field
		}

		searchFields = append(searchFields, searchField)
	}

	cl.ctx.fields = FullSchema{fields: searchFields}
	return fieldsLayer{ctx: cl.ctx}
}

func (fl fieldsLayer) From(from string, kind Reference) fromLayer {
	fields := fl.ctx.fields.fields

	var (
		detailedScheme []schema.SearchField
	)

	scheme, err := schema.FindRelationFields(from)
	if err != nil {
		return fromLayer{}
	}

	for _, field := range fields {
		schemeField, exists := scheme[field.Name]
		if !exists {
			return fromLayer{}
		}

		detailedScheme = append(detailedScheme, schemeField)
	}

	fl.ctx.query = Query{
		Query: SELECT,
		Ref:   kind,
		Name:  from,
	}

	return fromLayer{ctx: fl.ctx}
}

func (f fieldsLayer) Join(joinKind Joins, selectField, joinField JoinEx) joinLayer {
	var (
		sf schema.SearchField
		jf schema.SearchField
	)

	var (
		exists bool
	)

	selectScheme, err := schema.FindRelationFields(selectField.RefName)
	if err != nil {
		return joinLayer{}
	}

	sf, exists = selectScheme[selectField.Field]
	if !exists {
		return joinLayer{}
	}

	joinScheme, err := schema.FindRelationFields(joinField.RefName)
	if err != nil {
		return joinLayer{}
	}

	jf, exists = joinScheme[joinField.Field]
	if !exists {
		return joinLayer{}
	}

	f.ctx.join = Join{
		Kind:        joinKind,
		SelectField: sf,
		JoinField:   jf,
	}

	return joinLayer{ctx: f.ctx}
}

func (jl joinLayer) From(from string, kind Reference) fromLayer {
	fields := jl.ctx.fields.fields

	var (
		detailedScheme []schema.SearchField
	)

	scheme, err := schema.FindRelationFields(from)
	if err != nil {
		return fromLayer{}
	}

	for _, field := range fields {
		switch field.Relation == "" {
		case true:
			schemeField, exists := scheme[field.Name]
			if !exists {
				return fromLayer{}
			}
			detailedScheme = append(detailedScheme, schemeField)
		case false:
			outerScheme, err := schema.FindRelationFields(field.Relation)
			if err != nil {
				return fromLayer{}
			}

			schemeField, exists := outerScheme[field.Name]
			if !exists {
				return fromLayer{}
			}

			detailedScheme = append(detailedScheme, schemeField)
		}
	}

	jl.ctx.query = Query{
		Query: SELECT,
		Ref:   kind,
		Name:  from,
	}

	return fromLayer{ctx: jl.ctx}
}

func (fl fromLayer) GroupBy(groupBy ...string) condLayer {
	var (
		processedFields []string
	)

	scheme, err := schema.FindRelationFields(fl.ctx.query.Name)
	if err != nil {
		return condLayer{}
	}

	for _, field := range groupBy {
		_, exists := scheme[field]
		if !exists {
			continue
		}

		processedFields = append(processedFields, field)
	}

	fl.ctx.groupedBy = processedFields
	return condLayer{ctx: fl.ctx}
}

func (fl fromLayer) Where(where ...WhereEx) condLayer {
	var (
		processedFields []WhereEx
	)

	scheme, err := schema.FindRelationFields(fl.ctx.query.Name)
	if err != nil {
		return condLayer{}
	}

	for _, field := range where {
		searchField, exists := scheme[field.FieldName]
		if !exists {
			continue
		}

		field.schema = searchField
		processedFields = append(processedFields, field)
	}

	fl.ctx.cond = Cond{
		WhereClause: processedFields,
	}

	return condLayer{ctx: fl.ctx}
}

func (cl condLayer) Where(where ...WhereEx) condLayer {
	var (
		processedFields []WhereEx
	)

	scheme, err := schema.FindRelationFields(cl.ctx.query.Name)
	if err != nil {
		return condLayer{}
	}

	for _, field := range where {
		searchField, exists := scheme[field.FieldName]
		if !exists {
			continue
		}

		field.schema = searchField
		processedFields = append(processedFields, field)
	}

	cl.ctx.cond.WhereClause = where
	return condLayer{ctx: cl.ctx}
}

func (cl condLayer) Having(having ...HavingEx) condLayer {
	var (
		processedFields []HavingEx
	)

	scheme, err := schema.FindRelationFields(cl.ctx.query.Name)
	if err != nil {
		return condLayer{}
	}

	for _, field := range having {
		searchField, exists := scheme[field.FieldName]
		if !exists {
			continue
		}

		field.schema = searchField

		processedFields = append(processedFields, field)
	}

	cl.ctx.cond.HavingClause = having
	return condLayer{ctx: cl.ctx}
}

func (cl condLayer) With(with With) metadataLayer {
	cl.ctx.with = with
	return metadataLayer{ctx: cl.ctx}
}

func (m metadataLayer) Build() (plan static.QueryPlan) {
	plan.QueryAlgo = m.ctx.query
	plan.SchemaAlgo = m.ctx.fields.fields
	plan.JoinAlgo = m.ctx.join
	plan.CondAlgo = m.ctx.cond
	plan.GroupBy = m.ctx.fields.fields
	plan.MetadataAlgo = m.ctx.with

	return
}
