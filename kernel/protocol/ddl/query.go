package ddl

import (
	"ksql/ksql"
	"strings"
)

type (
	QueryRestAnalysis struct{}
)

func parseListQuery(partialQuery string) ksql.Query {
	query := ksql.Query{
		Query: ksql.LIST,
	}

	if strings.Contains(partialQuery, "STREAMS") {
		query.Ref = ksql.STREAM
	}

	if strings.Contains(partialQuery, "TABLES") {
		query.Ref = ksql.TABLE
	}

	if strings.Contains(partialQuery, "TOPICS") {
		query.Ref = ksql.TOPIC
	}

	return query
}

func parseDescribeQuery(partialQuery string) ksql.Query {
	query := ksql.Query{
		Query: ksql.DESCRIBE,
	}

	words := strings.Split(partialQuery, " ")
	if len(words) < 2 {
		return query
	}
	query.Name = words[1]

	return query
}

func parseDropQuery(partialQuery string) ksql.Query {
	query := ksql.Query{
		Query: ksql.DROP,
	}

	if strings.Contains(partialQuery, "STREAM") {
		query.Ref = ksql.STREAM
		partialQuery, _ = strings.CutPrefix(partialQuery, "STREAM")
	}

	if strings.Contains(partialQuery, "TABLE") {
		query.Ref = ksql.TABLE
		partialQuery, _ = strings.CutPrefix(partialQuery, "TABLE")
	}

	if strings.Contains(partialQuery, "TOPIC") {
		query.Ref = ksql.TOPIC
		partialQuery, _ = strings.CutPrefix(partialQuery, "TOPIC")
	}

	query.Name = strings.TrimSpace(partialQuery)

	return query
}

func parseSelectQuery(partialQuery string) ksql.Query {
	query := ksql.Query{
		Query: ksql.SELECT,
	}

	name, found := strings.CutPrefix(partialQuery, "FROM")
	if !found {
		return query
	}

	query.Name = strings.TrimSpace(name)

	return query
}

func parseInsertQuery(partialQuery string) ksql.Query {
	query := ksql.Query{
		Query: ksql.INSERT,
	}

	_, buffer, found := strings.Cut(partialQuery, "INTO")
	if !found {
		return query
	}

	name, _, found := strings.Cut(buffer, "(")
	if !found {
		return query
	}

	query.Name = strings.TrimSpace(name)

	return query
}

func parseCreateQuery(partialQuery string) ksql.Query {
	query := ksql.Query{
		Query: ksql.CREATE,
	}

	buffer := strings.Split(partialQuery, " ")
	if len(buffer) < 3 {
		return query
	}

	query.Name = buffer[2]

	switch buffer[1] {
	case "STREAM":
		query.Ref = ksql.STREAM
	case "TABLE":
		query.Ref = ksql.TABLE
	}

	return query
}

func (qa QueryRestAnalysis) Deserialize(
	partialQuery string,
	queryType ksql.QueryType) ksql.Query {

	switch queryType {
	case ksql.LIST:
		return parseListQuery(partialQuery)
	case ksql.DESCRIBE:
		return parseDescribeQuery(partialQuery)
	case ksql.DROP:
		return parseDropQuery(partialQuery)
	case ksql.SELECT:
		return parseSelectQuery(partialQuery)
	case ksql.CREATE:
		return parseCreateQuery(partialQuery)
	case ksql.INSERT:
		return parseInsertQuery(partialQuery)
	default:
		return ksql.Query{}
	}
}
