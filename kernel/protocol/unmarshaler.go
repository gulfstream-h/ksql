package protocol

import (
	"errors"
	"ksql/ksql"
	"ksql/schema"
)

type KafkaDeserializer struct {
	QueryAlgo    QueryDeserializeAlgo
	SchemaAlgo   SchemaDeserializeAlgo
	JoinAlgo     JoinDeserializeAlgo
	AggAlgo      AggDeserializeAlgo
	MetadataAlgo MetadataDeserializeAlgo
}

var (
	ErrUnprocessable = errors.New(
		"unprocessable kafka " +
			"response entity")
)

type (
	QueryDeserializeReport struct {
		Query ksql.Query
		From  string
	}

	SchemaDeserializeReport struct {
		fields map[string]schema.SearchField
	}

	JoinDeserializeReport struct {
	}

	AggDeserializeReport struct {
	}

	MetaDeserializeReport struct {
	}
)

type QueryDeserializeAlgo interface {
	Deserialize([]byte) QueryDeserializeReport
}

type SchemaDeserializeAlgo interface {
	Deserialize([]byte) SchemaDeserializeReport
}

type JoinDeserializeAlgo interface {
	Deserialize([]byte) JoinDeserializeReport
}

type AggDeserializeAlgo interface {
	Deserialize([]byte) AggDeserializeReport
}

type MetadataDeserializeAlgo interface {
	Deserialize([]byte) MetaDeserializeReport
}
