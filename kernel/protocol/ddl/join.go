package ddl

import (
	"ksql/ksql"
	"ksql/schema"
	"strings"
)

type (
	JoinRestAnalysis struct{}
)

func (ja JoinRestAnalysis) Deserialize(query string) ksql.Join {
	var (
		j ksql.Join
	)

	partialQuery, found := strings.CutPrefix(query, "INNER JOIN")
	if found {
		j.Kind = ksql.Inner
		j.SelectField, j.JoinField = parseJoin(partialQuery)
		return j
	}

	partialQuery, found = strings.CutPrefix(query, "LEFT JOIN")
	if found {
		j.Kind = ksql.Left
		j.SelectField, j.JoinField = parseJoin(partialQuery)
		return j
	}

	partialQuery, found = strings.CutPrefix(query, "RIGHT JOIN")
	if found {
		j.Kind = ksql.Right
		j.SelectField, j.JoinField = parseJoin(partialQuery)
		return j
	}

	return j
}

func parseJoin(
	partialQuery string) (
	schema.SearchField, schema.SearchField) {

	var (
		selected, joinable schema.SearchField
	)

	selectedField, joinableField, found := strings.Cut(partialQuery, "ON")
	if !found {
		return selected, joinable
	}

	alias, field, found := strings.Cut(selectedField, ".")
	if !found {
		selected.FieldName = field
	} else {
		selected.Referer = alias
		selected.FieldName = field
	}

	alias, field, found = strings.Cut(joinableField, ".")
	if !found {
		joinable.FieldName = field
	} else {
		joinable.Referer = alias
		joinable.FieldName = field
	}

	return selected, joinable
}
