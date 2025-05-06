package protocol

import (
	"errors"
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

var (
	ErrUnprocessable = errors.New(
		"unprocessable kafka " +
			"response entity")
)

type (
	QueryDeserializeReport struct {
		Query          ksql.Query
		Ref            ksql.Reference
		Name           string
		RawSchema      string
		PostProcessing string
		CTE            map[string]QueryDeserializeReport
	}

	SchemaDeserializeReport struct {
		fields map[string]schema.SearchField
	}

	JoinDeserializeReport struct {
	}

	ConditionalDeserializeReport struct {
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

type ConditionalDeserializeAlgo interface {
	Deserialize([]byte) ConditionalDeserializeReport
}

type MetadataDeserializeAlgo interface {
	Deserialize([]byte) MetaDeserializeReport
}
