package ddl

import (
	"ksql/ksql"
	"strings"
)

type (
	CondRestAnalysis struct{}
)

func (ca CondRestAnalysis) Deserialize(
	whereQuery, havingQuery string) ksql.Cond {
	var (
		c ksql.Cond
	)

	whereClause, found := strings.CutPrefix(whereQuery, "WHERE ")
	if !found {
		return c
	}

	c.WhereClause = parseWhere(whereClause)

	havingClause, found := strings.CutPrefix(havingQuery, "HAVING ")
	if !found {
		return c
	}

	c.HavingClause = parseHaving(havingClause)

	return c
}

func formatWhere(where string) ksql.WhereEx {
	whereLiterals := strings.Split(where, " ")
	if len(whereLiterals) < 3 {
		return ksql.WhereEx{}
	}

	switch whereLiterals[1] {
	case "=":
		return ksql.WhereEx{FieldName: whereLiterals[0]}.Equal(whereLiterals[2])
	default:
		//TODO: IMPLEMENT OTHER METHODS
		return ksql.WhereEx{}
	}
}

func parseWhere(whereClause string) []ksql.WhereEx {
	conditionals := strings.Split(whereClause, "AND")
	whereExes := make([]ksql.WhereEx, 0, len(conditionals))

	for _, conditional := range conditionals {
		whereExes = append(whereExes, formatWhere(conditional))
	}

	return whereExes
}

func formatHaving(having string) ksql.HavingEx {
	havingLiterals := strings.Split(having, " ")
	if len(havingLiterals) < 3 {
		return ksql.HavingEx{}
	}

	switch havingLiterals[1] {
	case "=":
		return ksql.HavingEx{FieldName: havingLiterals[0]}.Equal(havingLiterals[2])
	default:
		//TODO: IMPLEMENT OTHER METHODS
		return ksql.HavingEx{}
	}
}

func parseHaving(havingClause string) []ksql.HavingEx {
	conditionals := strings.Split(havingClause, "AND")
	havingExes := make([]ksql.HavingEx, 0, len(conditionals))

	for _, conditional := range conditionals {
		havingExes = append(havingExes, formatHaving(conditional))
	}

	return havingExes
}
