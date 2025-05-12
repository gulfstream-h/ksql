package protocol

import (
	"ksql/kernel/protocol/ddl"
	"ksql/ksql"
	"ksql/schema"
)

type KafkaDeserializer struct {
	QueryAlgo       QueryDeserializeAlgo
	SchemaAlgo      SchemaDeserializeAlgo
	JoinAlgo        JoinDeserializeAlgo
	GroupByAlgo     GroupByDeserializeAlgo
	ConditionalAlgo ConditionalDeserializeAlgo
	MetadataAlgo    MetadataDeserializeAlgo
}

func RestKafkaDeserializer() *KafkaDeserializer {
	return &KafkaDeserializer{
		QueryAlgo:       ddl.QueryRestAnalysis{},
		SchemaAlgo:      ddl.SchemaRestAnalysis{},
		JoinAlgo:        ddl.JoinRestAnalysis{},
		ConditionalAlgo: ddl.CondRestAnalysis{},
		GroupByAlgo:     ddl.SchemaRestAnalysis{},
		MetadataAlgo:    ddl.MetadataRestAnalysis{},
	}
}

func (kd KafkaDeserializer) Deserialize(
	query string) KafkaSerializer {

	var (
		ks KafkaSerializer
	)

	ks.QueryAlgo = kd.QueryAlgo.Deserialize("")
	ks.SchemaAlgo = kd.SchemaAlgo.Deserialize("")
	ks.JoinAlgo = kd.JoinAlgo.Deserialize("")
	ks.CondAlgo = kd.ConditionalAlgo.Deserialize("")
	ks.GroupBy = kd.GroupByAlgo.Deserialize("")
	ks.MetadataAlgo = kd.MetadataAlgo.Deserialize("")

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
	Deserialize(string) ksql.Query
}

const (
	schemeRegular = `(?i)SELECT\s+(.*?)\s+FROM`
)

type SchemaDeserializeAlgo interface {
	Deserialize(string) []schema.SearchField
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
	Deserialize(string) ksql.Cond
}

const (
	metadataRegular = `(?is)\bWITH\s*\(\s*(.*?)\s*\)`
)

type MetadataDeserializeAlgo interface {
	Deserialize(string) ksql.With
}
