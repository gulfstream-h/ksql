package protocol

import (
	"errors"
	"ksql/ksql"
)

type KafkaDeserializer struct {
	QueryAlgo       QueryDeserializeAlgo
	SchemaAlgo      SchemaDeserializeAlgo
	JoinAlgo        JoinDeserializeAlgo
	ConditionalAlgo ConditionalDeserializeAlgo
	MetadataAlgo    MetadataDeserializeAlgo
}

var (
	ErrUnprocessable = errors.New("unprocessable entity")
)

type QueryDeserializeAlgo interface {
	Deserialize([]byte) ksql.Query
}

type SchemaDeserializeAlgo interface {
	Deserialize([]byte) ksql.FullSchema
}

type JoinDeserializeAlgo interface {
	Deserialize([]byte) ksql.Join
}

type ConditionalDeserializeAlgo interface {
	Deserialize([]byte) ksql.Cond
}

type MetadataDeserializeAlgo interface {
	Deserialize([]byte) ksql.With
}
