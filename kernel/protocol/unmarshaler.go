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

	reg, err := regexp.Compile(cteRegular)
	if err != nil {
		return ks
	}

	cteQuery := reg.FindString(query)

	deserializeCTE(cteQuery, kd)

	partialQuery := reg.ReplaceAllString(query, "")

	var (
		qt ksql.QueryType
	)

	switch {
	case strings.Contains(partialQuery, "CREATE"):
		qt = ksql.CREATE
		reg, err = regexp.Compile(createRegular)
	case strings.Contains(partialQuery, "INSERT"):
		qt = ksql.INSERT
		reg, err = regexp.Compile(insertRegular)
	case strings.Contains(partialQuery, "SELECT"):
		qt = ksql.SELECT
		reg, err = regexp.Compile(selectRegular)
	default:
		return ks
	}

	if err != nil {
		return ks
	}

	ks.QueryAlgo = kd.QueryAlgo.Deserialize(reg.FindString(partialQuery), qt)
	partialQuery = reg.ReplaceAllString(partialQuery, "")

	reg, err = regexp.Compile(schemeRegular)
	if err != nil {
		return ks
	}
	ks.SchemaAlgo = kd.SchemaAlgo.Deserialize(reg.FindString(partialQuery), qt)
	partialQuery = reg.ReplaceAllString(partialQuery, "")

	reg, err = regexp.Compile(joinsRegular)
	if err != nil {
		return ks
	}

	ks.JoinAlgo = kd.JoinAlgo.Deserialize(reg.FindString(partialQuery))
	partialQuery = reg.ReplaceAllString(partialQuery, "")

	reg, err = regexp.Compile(whereRegular)
	if err != nil {
		return ks
	}

	whereClause := reg.FindString(partialQuery)
	partialQuery = reg.ReplaceAllString(partialQuery, "")

	reg, err = regexp.Compile(havingRegular)
	if err != nil {
		return ks
	}

	havingClause := reg.FindString(partialQuery)
	partialQuery = reg.ReplaceAllString(partialQuery, "")

	ks.CondAlgo = kd.ConditionalAlgo.Deserialize(whereClause, havingClause)

	reg, err = regexp.Compile(groupByRegular)
	if err != nil {
		return ks
	}

	ks.GroupBy = kd.GroupByAlgo.Deserialize(reg.FindString(partialQuery))
	partialQuery = reg.ReplaceAllString(partialQuery, "")

	reg, err = regexp.Compile(metadataRegular)
	if err != nil {
		return ks
	}

	ks.MetadataAlgo = kd.MetadataAlgo.Deserialize(reg.FindString(partialQuery))
	partialQuery = reg.ReplaceAllString(partialQuery, "")

	return ks
}

func deserializeCTE(
	partialQuery string,
	kd KafkaDeserializer) map[string]KafkaSerializer {

	var (
		cte map[string]KafkaSerializer
	)

	return cte
}

func deserializeAs(
	partialQuery string,
	kd KafkaDeserializer) map[string]KafkaSerializer {

	return map[string]KafkaSerializer{"AS": kd.Deserialize(partialQuery)}
}

const (
	selectRegular = `(?is)\bSELECT\s+(.*?)(?=\s+FROM)`
	createRegular = `(?is)\bCREATE\s+(TABLE|STREAM)\s+([a-zA-Z0-9_]+)\s*\((.*?)\)\s*(WITH\s*\(.*\))?`
	insertRegular = `(?is)\bINSERT\s+INTO\s+([a-zA-Z0-9_]+)\s*\((.*?)\)\s*VALUES\s*\((.*?)\)`
	cteRegular    = `(?is)\bWITH\s+([a-zA-Z0-9_]+)\s+AS\s*\((.*?)\)`
)

type QueryDeserializeAlgo interface {
	Deserialize(string, ksql.QueryType) ksql.Query
}

const (
	schemeRegular = `(?i)SELECT\s+(.*?)\s+FROM`
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
