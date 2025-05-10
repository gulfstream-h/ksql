package proxy

import (
	"ksql/kernel/protocol"
	"ksql/ksql"
	"ksql/schema"
	"ksql/streams"
	"ksql/tables"
	"ksql/topics"
)

// CRUTCH for import cycle not allowed :))))

type (
	Topic[S any]  topics.Topic[S]
	Stream[S any] streams.Stream[S]
	Table[S any]  tables.Table[S]
	QueryPlan     protocol.KafkaSerializer
)

func BuildQueryPlan(
	queryAlgo ksql.Query,
	fields []schema.SearchField,
	joinAlgo ksql.Join,
	CondAlgo ksql.Cond,
	GroupBy []schema.SearchField,
	MetadataAlgo ksql.With,
) QueryPlan {
	return QueryPlan(protocol.KafkaSerializer{
		QueryAlgo:    queryAlgo,
		SchemaAlgo:   fields,
		JoinAlgo:     joinAlgo,
		CondAlgo:     CondAlgo,
		GroupBy:      GroupBy,
		MetadataAlgo: MetadataAlgo,
		CTE:          nil,
	})
}

func CreateTopicFromStream[S any](topicName string, stream *streams.Stream[S]) Topic[S] {
	return Topic[S](topics.Topic[S]{})
}

func CreateTopicFromTable[S any](topicName string, table *tables.Table[S]) Topic[S] {
	return Topic[S](topics.Topic[S]{})
}

func CreateStreamFromTable[S any](streamName string, table *tables.Table[S]) Stream[S] {
	return Stream[S](streams.Stream[S]{})
}

func CreateTableFromStream[S any](tableName string, stream *streams.Stream[S]) Table[S] {
	return Table[S](tables.Table[S]{})
}
