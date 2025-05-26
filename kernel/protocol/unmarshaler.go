package protocol

import (
	"ksql/kernel/protocol/ddl"
	"ksql/ksql"
	"ksql/schema"
	"regexp"
	"strings"
)

// KafkaDeserializer - contains all parse algorithms
// that can be used translate string representation of
// stream/table to internal representation of KafkaSerializer
type KafkaDeserializer struct {
	QueryAlgo       QueryDeserializeAlgo
	SchemaAlgo      SchemaDeserializeAlgo
	JoinAlgo        JoinDeserializeAlgo
	GroupByAlgo     GroupByDeserializeAlgo
	ConditionalAlgo ConditionalDeserializeAlgo
	MetadataAlgo    MetadataDeserializeAlgo
}

// GetRestDeserializer - returns a new instance of KafkaDeserializer
// current realizations parses queries with strings package
func GetRestDeserializer() *KafkaDeserializer {
	return &KafkaDeserializer{
		QueryAlgo:       ddl.QueryRestAnalysis{},
		SchemaAlgo:      ddl.SchemaRestAnalysis{},
		JoinAlgo:        ddl.JoinRestAnalysis{},
		ConditionalAlgo: ddl.CondRestAnalysis{},
		GroupByAlgo:     ddl.GroupRestAnalysis{},
		MetadataAlgo:    ddl.MetadataRestAnalysis{},
	}
}

// Deserialize - is used to parse string representation of query
// at first query is parsed to blocks by regular expressions
// after parse algorithm handles block to ksql layer(reverse process of ksql.Builder)
// and finally constructs KafkaSerializer object. In ideal case, marshaling of
// KafkaSerializer object should be the same as original query
func (kd KafkaDeserializer) Deserialize(
	query string) KafkaSerializer {

	var (
		ks KafkaSerializer
	)

	reg := regexp.MustCompile(cteRegular)

	cteQuery := reg.FindString(query)

	var (
		cte map[string]KafkaSerializer
	)

	cteQuery, _ = strings.CutPrefix(cteQuery, "WITH")

	ks.CTE = deserializeCTE(cteQuery, cte, kd)

	partialQuery := reg.ReplaceAllString(query, "")

	var (
		qt ksql.QueryType
	)

	switch {
	case strings.Contains(partialQuery, "CREATE"):
		qt = ksql.CREATE
		reg = regexp.MustCompile(createRegular)
	case strings.Contains(partialQuery, "INSERT"):
		qt = ksql.INSERT
		reg = regexp.MustCompile(insertRegular)
	case strings.Contains(partialQuery, "SELECT"):
		qt = ksql.SELECT
		reg = regexp.MustCompile(selectRegular)
	default:
		return ks
	}

	ks.QueryAlgo = kd.QueryAlgo.Deserialize(reg.FindString(partialQuery), qt)
	partialQuery = reg.ReplaceAllString(partialQuery, "")

	reg = regexp.MustCompile(schemeRegular)
	ks.SchemaAlgo = kd.SchemaAlgo.Deserialize(reg.FindString(partialQuery), qt)
	partialQuery = reg.ReplaceAllString(partialQuery, "")

	reg = regexp.MustCompile(joinsRegular)

	ks.JoinAlgo = kd.JoinAlgo.Deserialize(reg.FindString(partialQuery))
	partialQuery = reg.ReplaceAllString(partialQuery, "")

	reg = regexp.MustCompile(groupByRegular)

	ks.GroupBy = kd.GroupByAlgo.Deserialize(reg.FindString(partialQuery))
	partialQuery = reg.ReplaceAllString(partialQuery, "")

	reg = regexp.MustCompile(whereRegular)

	whereClause := reg.FindString(partialQuery)
	partialQuery = reg.ReplaceAllString(partialQuery, "")

	reg = regexp.MustCompile(havingRegular)

	havingClause := reg.FindString(partialQuery)
	partialQuery = reg.ReplaceAllString(partialQuery, "")

	ks.CondAlgo = kd.ConditionalAlgo.Deserialize(whereClause, havingClause)

	reg = regexp.MustCompile(metadataRegular)

	ks.MetadataAlgo = kd.MetadataAlgo.Deserialize(reg.FindString(partialQuery))
	partialQuery = reg.ReplaceAllString(partialQuery, "")

	return ks
}

func deserializeCTE(
	partialQuery string,
	cte map[string]KafkaSerializer,
	kd KafkaDeserializer) map[string]KafkaSerializer {

	kd.Deserialize(partialQuery)

	return cte
}

const (
	selectRegular = `(?is)FROM\s+[a-zA-Z0-9_\.]+\s+([a-zA-Z0-9_]+)`
	createRegular = `(?is)\bCREATE\s+(TABLE|STREAM)\s+([a-zA-Z0-9_]+)\s*\((.*?)\)\s*(WITH\s*\(.*\))?`
	insertRegular = `(?is)\bINSERT\s+INTO\s+([a-zA-Z0-9_]+)\s*\((.*?)\)\s*VALUES\s*\((.*?)\)`
	cteRegular    = `(?is)\bWITH\s+((?:[a-zA-Z0-9_]+\s+AS\s*\((?:[^()]*|\([^()]*\))*\)\s*,?\s*)+)`
)

type QueryDeserializeAlgo interface {
	Deserialize(string, ksql.QueryType) ksql.Query
}

const (
	schemeRegular = `(?is)\bSELECT\s+(.*?)\s+FROM\b`
)

type SchemaDeserializeAlgo interface {
	Deserialize(string, ksql.QueryType) []schema.SearchField
}

const (
	joinsRegular = `(?i)\b(LEFT|INNER|RIGHT)?\s+JOIN\b\s+[^\n]+?\s+ON\s+[^\n;]+`
)

type JoinDeserializeAlgo interface {
	Deserialize(string) ksql.Join
}

const (
	groupByRegular = `(?is)\bGROUP\s+BY\b\s+(.*?)(?=\bHAVING\b|\bEMIT\b|\bWINDOW\b|\bLIMIT\b|\bORDER\s+BY\b|;|$)`
)

type GroupByDeserializeAlgo interface {
	Deserialize(string) []schema.SearchField
}

const (
	whereRegular  = `(?is)\bWHERE\b\s+(.*?)(?=\bGROUP\s+BY\b|\bHAVING\b|\bEMIT\b|\bWINDOW\b|\bLIMIT\b|\bORDER\s+BY\b|;|$)`
	havingRegular = `(?is)\bHAVING\b\s+(.*?)(?=\bGROUP\s+BY\b|\bEMIT\b|\bWINDOW\b|\bLIMIT\b|\bORDER\s+BY\b|;|$)`
)

type ConditionalDeserializeAlgo interface {
	Deserialize(string, string) ksql.Cond
}

const (
	metadataRegular = `(?is)\bWITH\s*\(\s*(.*?)\s*\)`
)

type MetadataDeserializeAlgo interface {
	Deserialize(string) ksql.With
}
