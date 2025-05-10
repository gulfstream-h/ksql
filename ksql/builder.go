package ksql

import (
	"ksql/proxy"
	"ksql/schema"
)

const (
	SelectBuilder = Builder(iota)
)

type (
	Builder int
)

type (
	fieldsLayer struct {
		fields FullSchema
	}

	joinLayer struct {
		join   Join
		fields fieldsLayer
	}

	fromLayer struct {
		join   joinLayer
		fields fieldsLayer
		query  Query
	}

	condLayer struct {
		join      Join
		fields    fieldsLayer
		query     Query
		groupedBy []string
		cond      Cond
	}

	metadataLayer struct {
		query  Query
		fields FullSchema
		join   Join
		cond   Cond
		with   With
	}
)

func (sb Builder) Fields(fields ...string) fieldsLayer {
	return fieldsLayer{fields: FullSchema{fields: nil}}
}

func (fl fieldsLayer) From(from string, kind Reference) fromLayer {
	return fromLayer{
		joinLayer{},
		fl,
		Query{
			Query: SELECT,
			Ref:   kind,
			Name:  from,
		}}
}

func (f fieldsLayer) Join(joinKind Joins, selectField, joinField JoinEx) joinLayer {
	return joinLayer{
		Join{
			Kind: joinKind,
			SelectField: schema.SearchField{
				FieldName: selectField.Field,
				Referer:   selectField.RefName,
			},
			JoinField: schema.SearchField{
				FieldName: joinField.Field,
				Referer:   joinField.RefName,
			},
		},
		f,
	}
}

func (jl joinLayer) From(from string, kind Reference) fromLayer {
	return fromLayer{
		jl,
		jl.fields,
		Query{
			Query: SELECT,
			Ref:   kind,
			Name:  from,
		}}
}

func (fl fromLayer) With(with With) metadataLayer {
	return metadataLayer{
		fl.query,
		fl.fields.fields,
		fl.join.join,
		Cond{},
		with,
	}
}

func (fl fromLayer) GroupBy(groupBy ...string) condLayer {
	return condLayer{
		groupedBy: groupBy,
	}
}

func (fl fromLayer) Where(cond ...WhereEx) condLayer {
	return condLayer{
		join:      fl.join.join,
		fields:    fl.fields,
		query:     fl.query,
		groupedBy: nil,
		cond: Cond{
			cond,
			nil,
		},
	}
}

func (cl condLayer) Where(cond ...WhereEx) condLayer {
	return condLayer{
		join:      cl.join,
		fields:    cl.fields,
		query:     cl.query,
		groupedBy: cl.groupedBy,
		cond: Cond{
			WhereClause:  cond,
			HavingClause: cl.cond.HavingClause,
		},
	}
}

func (cl condLayer) Having(cond ...HavingEx) condLayer {
	return condLayer{
		join:      cl.join,
		fields:    cl.fields,
		query:     cl.query,
		groupedBy: cl.groupedBy,
		cond: Cond{
			WhereClause:  cl.cond.WhereClause,
			HavingClause: cond,
		},
	}
}

func (cl condLayer) With(with With) metadataLayer {
	return metadataLayer{
		query:  cl.query,
		fields: cl.fields.fields,
		join:   cl.join,
		cond:   cl.cond,
		with:   with,
	}
}

func (m metadataLayer) Build() proxy.QueryPlan {
	return proxy.
		BuildQueryPlan(
			m.query,
			m.fields.fields,
			m.join,
			m.cond,
			m.fields.fields,
			m.with,
		)
}

func a() {
	SelectBuilder.
		Fields("name", "sum(amount)").
		Join(Left,
			JoinEx{
				Field:   "name",
				RefName: "example_stream",
				Ref:     STREAM,
			},
			JoinEx{
				Field:   "name",
				RefName: "join_table",
				Ref:     TABLE,
			}).
		From("example_stream", STREAM).
		GroupBy("name").
		Where(WhereEx{}).
		Having(HavingEx{}).
		With(
			With{
				Topic:       "example_topic",
				ValueFormat: "JSON"}).
		Build()
}
