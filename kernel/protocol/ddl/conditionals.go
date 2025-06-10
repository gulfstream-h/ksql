package ddl

import (
	"ksql/kernel/protocol"
	"strings"
)

type (
	CondRestAnalysis struct{}
)

func (ca CondRestAnalysis) Deserialize(
	whereQuery, havingQuery string) protocol.Cond {
	var (
		c protocol.Cond
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

func formatWhere(where string) protocol.WhereEx {
	whereLiterals := strings.Split(where, " ")
	if len(whereLiterals) < 3 {
		return protocol.WhereEx{}
	}

	switch whereLiterals[1] {
	case "=":
		return protocol.WhereEx{FieldName: whereLiterals[0]}.Equal(whereLiterals[2])
	default:
		//TODO: IMPLEMENT OTHER METHODS
		return protocol.WhereEx{}
	}
}

func parseWhere(whereClause string) []protocol.WhereEx {
	conditionals := strings.Split(whereClause, "AND")
	whereExes := make([]protocol.WhereEx, 0, len(conditionals))

	for _, conditional := range conditionals {
		whereExes = append(whereExes, formatWhere(conditional))
	}

	return whereExes
}

func formatHaving(having string) protocol.HavingEx {
	havingLiterals := strings.Split(having, " ")
	if len(havingLiterals) < 3 {
		return protocol.HavingEx{}
	}

	switch havingLiterals[1] {
	case "=":
		return protocol.HavingEx{FieldName: havingLiterals[0]}.Equal(havingLiterals[2])
	default:
		//TODO: IMPLEMENT OTHER METHODS
		return protocol.HavingEx{}
	}
}

func parseHaving(havingClause string) []protocol.HavingEx {
	conditionals := strings.Split(havingClause, "AND")
	havingExes := make([]protocol.HavingEx, 0, len(conditionals))

	for _, conditional := range conditionals {
		havingExes = append(havingExes, formatHaving(conditional))
	}

	return havingExes
}
