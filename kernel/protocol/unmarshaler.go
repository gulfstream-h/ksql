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

	ks.CTE = nil
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

}

func deserializeAs(
	partialQuery string,
	kd KafkaDeserializer) map[string]KafkaSerializer {

	return map[string]KafkaSerializer{"AS": kd.Deserialize(partialQuery)}
}

type QueryDeserializeAlgo interface {
	Deserialize(string) ksql.Query
}

type SchemaDeserializeAlgo interface {
	Deserialize(string) []schema.SearchField
}

type JoinDeserializeAlgo interface {
	Deserialize(string) ksql.Join
}

type GroupByDeserializeAlgo interface {
	Deserialize(string) []schema.SearchField
}

type ConditionalDeserializeAlgo interface {
	Deserialize(string) ksql.Cond
}

type MetadataDeserializeAlgo interface {
	Deserialize(string) ksql.With
}
