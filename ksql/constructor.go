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
	scheme, err := schema.FindRelationFields(from)
	if err != nil {
		return fromLayer{}
	}

	for _, field := range fields {
		schemeFields, exists := scheme[field.Name]
		if !exists {
			return fromLayer{}
		}

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

	selectScheme := schema.GetSchemeFields(selectField.RefName, schema.ResourceKind(selectField.Ref))
	for _, field := range selectScheme {
		if field.FieldName == selectField.Field {
			sf = field
			break
		}
	}

	joinScheme := schema.GetSchemeFields(selectField.RefName, schema.ResourceKind(selectField.Ref))
	for _, field := range joinScheme {
		if field.FieldName == selectField.Field {
			jf = field
			break
		}
	}

	analysis, err := schema.CompareFields(sf, jf)
	if err != nil {
		return joinLayer{}
	}

	if !analysis.CompatibilityByType {
		return joinLayer{}
	}

	f.ctx.join = Join{
		Kind: joinKind,
		SelectField: schema.SearchField{
			FieldName: selectField.Field,
			Relation:  selectField.RefName,
		},
		JoinField: schema.SearchField{
			FieldName: joinField.Field,
			Relation:  joinField.RefName,
		},
	}
	return joinLayer{ctx: f.ctx}
}

func (jl joinLayer) From(from string, kind Reference) fromLayer {
	fields := jl.ctx.fields.fields
	scheme := schema.GetSchemeFields(from, schema.ResourceKind(kind))
	joinScheme := schema.GetSchemeFields(jl.ctx.join.JoinField.Relation, schema.ResourceKind(kind))

	for _, field := range fields {
		switch strings.Contains(field.FieldName, ".") {
		case true:
			for _, schemaField := range scheme {
				if field.FieldName == schemaField.FieldName {
					field.Relation = from
					field.KsqlKind = schemaField.KsqlKind
					break
				}
			}
		case false:
			for _, schemaField := range joinScheme {
				if field.FieldName == schemaField.FieldName {
					field.Relation = from
					field.KsqlKind = schemaField.KsqlKind
					break
				}
			}
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
	scheme := schema.GetSchemeFields(fl.ctx.query.Name, schema.ResourceKind(fl.ctx.query.Ref))

	for _, groupField := range groupBy {
		for _, field := range scheme {
			if field.FieldName == groupField {
				fl.ctx.groupedBy = append(fl.ctx.groupedBy, field.FieldName)
			}
		}
	}

	return condLayer{ctx: fl.ctx}
}

func (fl fromLayer) Where(cond ...WhereEx) condLayer {
	var (
		processedFields []WhereEx
	)

	for _, field := range cond {
		for _, schemaField := range fl.ctx.fields.fields {
			if field.FieldName == schemaField.FieldName {
				field.schema = schemaField
				break
			}
		}

		processedFields = append(processedFields, field)
	}

	fl.ctx.cond = Cond{
		WhereClause: processedFields,
	}

	return condLayer{ctx: fl.ctx}
}

func (cl condLayer) Where(cond ...WhereEx) condLayer {
	var (
		processedFields []WhereEx
	)

	for _, field := range cond {
		for _, schemaField := range cl.ctx.fields.fields {
			if field.FieldName == schemaField.FieldName {
				field.schema = schemaField
				break
			}
		}

		processedFields = append(processedFields, field)
	}

	cl.ctx.cond.WhereClause = cond
	return condLayer{ctx: cl.ctx}
}

func (cl condLayer) Having(having ...HavingEx) condLayer {
	var (
		processedFields []HavingEx
	)

	for _, field := range having {
		for _, schemaField := range cl.ctx.fields.fields {
			if field.FieldName == schemaField.FieldName {
				field.schema = schemaField
				break
			}
		}

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

func a()  {
	SelectBuilder.Fields("field1", "field2").From("example", STREAM).
}