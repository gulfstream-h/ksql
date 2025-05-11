package ksql

import (
	"ksql/proxy"
	"ksql/schema"
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

func (sb Builder) Fields(fields ...string) fieldsLayer {
	return fieldsLayer{
		ctx: &builderContext{
			fields: FullSchema{fields: nil},
		},
	}
}

func (fl fieldsLayer) From(from string, kind Reference) fromLayer {
	fl.ctx.query = Query{
		Query: SELECT,
		Ref:   kind,
		Name:  from,
	}
	return fromLayer{ctx: fl.ctx}
}

func (f fieldsLayer) Join(joinKind Joins, selectField, joinField JoinEx) joinLayer {
	f.ctx.join = Join{
		Kind: joinKind,
		SelectField: schema.SearchField{
			FieldName: selectField.Field,
			Referer:   selectField.RefName,
		},
		JoinField: schema.SearchField{
			FieldName: joinField.Field,
			Referer:   joinField.RefName,
		},
	}
	return joinLayer{ctx: f.ctx}
}

func (jl joinLayer) From(from string, kind Reference) fromLayer {
	jl.ctx.query = Query{
		Query: SELECT,
		Ref:   kind,
		Name:  from,
	}
	return fromLayer{ctx: jl.ctx}
}

func (fl fromLayer) GroupBy(groupBy ...string) condLayer {
	fl.ctx.groupedBy = groupBy
	return condLayer{ctx: fl.ctx}
}

func (fl fromLayer) Where(cond ...WhereEx) condLayer {
	fl.ctx.cond = Cond{
		WhereClause: cond,
	}
	return condLayer{ctx: fl.ctx}
}

func (cl condLayer) Where(cond ...WhereEx) condLayer {
	cl.ctx.cond.WhereClause = cond
	return condLayer{ctx: cl.ctx}
}

func (cl condLayer) Having(having ...HavingEx) condLayer {
	cl.ctx.cond.HavingClause = having
	return condLayer{ctx: cl.ctx}
}

func (cl condLayer) With(with With) metadataLayer {
	cl.ctx.with = with
	return metadataLayer{ctx: cl.ctx}
}

func (m metadataLayer) Build() proxy.QueryPlan {
	return proxy.BuildQueryPlan(
		m.ctx.query,
		m.ctx.fields.fields,
		m.ctx.join,
		m.ctx.cond,
		m.ctx.fields.fields,
		m.ctx.with,
	)
}

func a() {
	SelectBuilder.
		Fields("name", "sum(amount)").
		Join(Left,
			JoinEx{Field: "name", RefName: "example_stream", Ref: STREAM},
			JoinEx{Field: "name", RefName: "join_table", Ref: TABLE},
		).
		From("example_stream", STREAM).
		GroupBy("name", "age", "salary").
		Where(WhereEx{FieldName: "name"}.Equal("my_name")).
		Having(HavingEx{FieldName: "sum(amount)"}.Equal("100")).
		With(With{
			Topic:       "example_topic",
			ValueFormat: "JSON",
		}).
		Build()
}
