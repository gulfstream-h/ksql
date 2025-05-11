package protocol

import (
	"errors"
	"ksql/kernel/protocol/ddl"
	"ksql/ksql"
	"ksql/schema"
)

type KafkaDeserializer struct {
	QueryAlgo       QueryDeserializeAlgo
	SchemaAlgo      SchemaDeserializeAlgo
	JoinAlgo        JoinDeserializeAlgo
	ConditionalAlgo ConditionalDeserializeAlgo
	MetadataAlgo    MetadataDeserializeAlgo
}

func RestKafkaDeserializer() *KafkaDeserializer {
	return &KafkaDeserializer{
		QueryAlgo:       ddl.QueryRestAnalysis{},
		SchemaAlgo:      ddl.SchemaRestAnalysis{},
		JoinAlgo:        ddl.JoinRestAnalysis{},
		ConditionalAlgo: ddl.CondRestAnalysis{},
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
	ks.GroupBy = nil
	ks.MetadataAlgo = kd.MetadataAlgo.Deserialize("")

	return ks
}

var (
	ErrUnprocessable = errors.New("unprocessable entity")
)

type QueryDeserializeAlgo interface {
	Deserialize(string) ksql.Query
}

type SchemaDeserializeAlgo interface {
	Deserialize(string) []schema.SearchField
}

type JoinDeserializeAlgo interface {
	Deserialize(string) ksql.Join
}

type ConditionalDeserializeAlgo interface {
	Deserialize(string) ksql.Cond
}

type MetadataDeserializeAlgo interface {
	Deserialize(string) ksql.With
}
