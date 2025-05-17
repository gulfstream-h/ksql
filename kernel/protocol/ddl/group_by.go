package ddl

import (
	"ksql/schema"
	"strings"
)

type (
	GroupRestAnalysis struct{}
)

func (ga GroupRestAnalysis) Deserialize(partialQuery string) []schema.SearchField {
	query, found := strings.CutPrefix(partialQuery, "GROUP BY")
	if !found {
		return nil
	}

	groupSplits := strings.Split(query, ",")

	var (
		searchFields = make([]schema.SearchField, 0, len(groupSplits))
	)

	for _, f := range groupSplits {
		searchFields = append(searchFields, parseGroupBy(f))
	}

	return searchFields
}

func parseGroupBy(partialQuery string) schema.SearchField {
	var (
		f schema.SearchField
	)

	alias, field, found := strings.Cut(partialQuery, ".")
	if !found {
		f.FieldName = field
	} else {
		f.Referer = alias
		f.FieldName = field
	}

	return f
}
