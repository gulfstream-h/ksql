package ddl

import (
	"ksql/kernel/protocol/proto"
	"strings"
)

type (
	CondRestAnalysis struct{}
)

func (ca CondRestAnalysis) Deserialize(
	whereQuery, havingQuery string) proto.Cond {
	var (
		c proto.Cond
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

func formatWhere(where string) proto.WhereEx {
	whereLiterals := strings.Split(where, " ")
	if len(whereLiterals) < 3 {
		return proto.WhereEx{}
	}

	switch whereLiterals[1] {
	case "=":
		return proto.WhereEx{FieldName: whereLiterals[0]}.Equal(whereLiterals[2])
	default:
		//TODO: IMPLEMENT OTHER METHODS
		return proto.WhereEx{}
	}
}

func parseWhere(whereClause string) []proto.WhereEx {
	conditionals := strings.Split(whereClause, "AND")
	whereExes := make([]proto.WhereEx, 0, len(conditionals))

	for _, conditional := range conditionals {
		whereExes = append(whereExes, formatWhere(conditional))
	}

	return whereExes
}

func formatHaving(having string) proto.HavingEx {
	havingLiterals := strings.Split(having, " ")
	if len(havingLiterals) < 3 {
		return proto.HavingEx{}
	}

	switch havingLiterals[1] {
	case "=":
		return proto.HavingEx{FieldName: havingLiterals[0]}.Equal(havingLiterals[2])
	default:
		//TODO: IMPLEMENT OTHER METHODS
		return proto.HavingEx{}
	}
}

func parseHaving(havingClause string) []proto.HavingEx {
	conditionals := strings.Split(havingClause, "AND")
	havingExes := make([]proto.HavingEx, 0, len(conditionals))

	for _, conditional := range conditionals {
		havingExes = append(havingExes, formatHaving(conditional))
	}

	return havingExes
}
