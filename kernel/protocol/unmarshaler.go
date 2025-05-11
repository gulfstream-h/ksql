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

func (kd KafkaDeserializer) Deserialize() {

}

var (
	ErrUnprocessable = errors.New("unprocessable entity")
)

type QueryDeserializeAlgo interface {
	Deserialize(string) ksql.Query
}

type SchemaDeserializeAlgo interface {
	Deserialize(string) ksql.FullSchema
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
