package topics

import (
	"context"
	"errors"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"ksql/kernel/network"
	"ksql/kernel/protocol"
	"ksql/kernel/protocol/dao"
	"ksql/kernel/protocol/dto"
	"ksql/ksql"
	"ksql/static"
	"ksql/streams"
	"ksql/tables"
)

type Topic[S any] struct {
	Name              string
	Partitions        int
	ReplicationFactor int
	ChildObjects      ChildTopicObjects
}

type ChildTopicObjects struct {
	Streams map[string]struct{}
	Tables  map[string]struct{}
}

func ListTopics(ctx context.Context) (dto.ShowTopics, error) {
	query := protocol.KafkaSerializer{
		QueryAlgo: ksql.Query{
			Query: ksql.LIST,
			Ref:   ksql.TOPIC,
		}}.Query()

	var (
		pipeline = make(chan []byte)
	)

	if err := network.Net.Perform(
		ctx,
		query,
		pipeline,
		network.ShortPolling{},
	); err != nil {
		err = fmt.Errorf("cannot perform request: %w", err)
		return dto.ShowTopics{}, err
	}

	select {
	case <-ctx.Done():
		return dto.ShowTopics{}, ctx.Err()
	case val, ok := <-pipeline:
		if !ok {
			return dto.ShowTopics{}, static.ErrMalformedResponse
		}

		var (
			topics dao.ShowTopics
		)

		if err := jsoniter.Unmarshal(val, &topics); err != nil {
			err = errors.Join(static.ErrUnserializableResponse, err)
			return dto.ShowTopics{}, err
		}

		return topics.DTO(), nil
	}
}

func (t *Topic[S]) RegisterStream(streamName string) streams.StreamSettings {
	partitions := uint8(t.Partitions)

	streamSettings := streams.StreamSettings{
		Name:        streamName,
		SourceTopic: &t.Name,
		Partitions:  &partitions,
		DeleteFunc: func(ctx context.Context) {
			delete(t.ChildObjects.Streams, streamName)
		},
	}

	static.StreamsProjections[streamName] = static.StreamSettings(streamSettings)

	t.ChildObjects.Streams[streamName] = struct{}{}

	return streamSettings
}

func (t *Topic[S]) RegisterTable(tableName string) tables.TableSettings {
	partitions := uint8(t.Partitions)

	tableSettings := tables.TableSettings{
		Name:        tableName,
		SourceTopic: &t.Name,
		Partitions:  &partitions,
		DeleteFunc: func(ctx context.Context) {
			delete(t.ChildObjects.Tables, tableName)
		},
	}

	static.TablesProjections[tableName] = static.TableSettings(tableSettings)

	t.ChildObjects.Tables[tableName] = struct{}{}

	return tableSettings
}

func (t *Topic[S]) GetAllStreamAdapters() map[string]struct{} {
	copyMap := make(map[string]struct{}, len(t.ChildObjects.Streams))
	for k := range t.ChildObjects.Streams {
		copyMap[k] = struct{}{}
	}
	return copyMap
}

func (t *Topic[S]) GetAllTableAdapters() map[string]struct{} {
	copyMap := make(map[string]struct{}, len(t.ChildObjects.Tables))
	for k := range t.ChildObjects.Tables {
		copyMap[k] = struct{}{}
	}
	return copyMap
}
