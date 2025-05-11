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
	QueryPlan protocol.KafkaSerializer

	StreamSettings streams.StreamSettings
	TableSettings  tables.TableSettings

	Topic[S any]  topics.Topic[S]
	Stream[S any] streams.Stream[S]
	Table[S any]  tables.Table[S]
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

func FindStreamSettings(name string) (StreamSettings, error) {
	settings, err := streams.GetStreamProjection(name)
	if err != nil {
		return StreamSettings{}, err
	}

	return StreamSettings(settings), nil
}

func FindTableSettings(name string) (TableSettings, error) {
	settings, err := tables.GetTableProjection(name)
	if err != nil {
		return TableSettings{}, err
	}

	return TableSettings(settings), nil
}

func CreateTopicFromStream[S any](topicName string, stream *streams.Stream[S]) Topic[S] {
	return Topic[S](topics.Topic[S]{
		Name: topicName,
	})
}

func CreateTopicFromTable[S any](topicName string, table *tables.Table[S]) Topic[S] {
	return Topic[S](topics.Topic[S]{
		Name: topicName,
	})
}

func CreateStreamFromTable[S any](streamName string, table *tables.Table[S]) Stream[S] {
	return Stream[S](streams.Stream[S]{
		Name: streamName,
	})
}

func CreateTableFromStream[S any](tableName string, stream *streams.Stream[S]) Table[S] {
	return Table[S](tables.Table[S]{
		Name: tableName,
	})
}
