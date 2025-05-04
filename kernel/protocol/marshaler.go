package protocol

import "ksql/ksql"

type KafkaSerializer struct {
	QueryAlgo    SchemaSerializeAlgo
	SchemaAlgo   SchemaSerializeAlgo
	JoinAlgo     JoinSerializeAlgo
	AggAlgo      AggSerializeAlgo
	MetadataAlgo MetadataSerializeAlgo
}

type (
	QuerySerializeReport struct {
		Query ksql.Query
	}

	SchemaSerializeReport struct {
	}

	JoinSerializeReport struct {
	}

	AggSerializeReport struct {
	}

	MetaSerializeReport struct {
	}
)

type QuerySerializeAlgo interface {
	Serialize([]byte) QuerySerializeReport
}

type SchemaSerializeAlgo interface {
	Serialize([]byte) SchemaSerializeReport
}

type JoinSerializeAlgo interface {
	Serialize([]byte) JoinSerializeReport
}

type AggSerializeAlgo interface {
	Serialize([]byte) AggSerializeReport
}

type MetadataSerializeAlgo interface {
	Serialize([]byte) MetaSerializeReport
}
