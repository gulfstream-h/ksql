package streams

import (
	"context"
	"errors"
	"ksql/kernel/network"
	"ksql/kernel/protocol"
	"ksql/ksql"
	"ksql/proxy"
	"ksql/schema"
	"net"
	"reflect"
)

type Stream[T any] struct {
	name         string
	sourceTopic  *string
	sourceStream *string
	partitions   *uint8
	remoteSchema *reflect.Type
	vf           schema.ValueFormat
}

var (
	existingStreams = make(map[string]StreamSettings)
)

var (
	ErrStreamDoesNotExist = errors.New("stream does not exist")
)

func ListStreams() {
	protocol.KafkaSerializer{
		QueryAlgo: ksql.Query{
			Query: ksql.LIST,
			Ref:   ksql.STREAM,
		},
	}.Query()
}

func GetStream[T any](
	ctx context.Context,
	stream string,
	settings StreamSettings) {

}

func CreateStream[T any](
	ctx context.Context,
	streamName string,
	settings StreamSettings) (*Stream[T], error) {

	protocol.KafkaSerializer{
		QueryAlgo: ksql.Query{
			Query: ksql.CREATE,
			Ref:   ksql.STREAM,
			Name:  streamName,
		},
		SchemaAlgo: nil,
		MetadataAlgo: ksql.With{
			Topic:       *settings.SourceTopic,
			ValueFormat: "JSON",
		},
	}.Query()

	return &Stream[T]{
		sourceTopic:  settings.SourceTopic,
		sourceStream: &streamName,
		partitions:   settings.Partitions,
		vf:           settings.format,
	}, nil
}

func CreateStreamAsSelect[T any](
	ctx context.Context,
	streamName string) {

}

func getStreamRemotely[T any](
	ctx context.Context,
	conn net.Conn,
	streamName string) (*Stream[T], error) {

	var (
		command = "DESCRIBE " + streamName
	)

	response, err := network.Perform(
		ctx,
		conn,
		conn.RemoteAddr().String(),
		len(command),
		command)
	if err != nil {
		return nil, err
	}

	var (
		dst T
	)

	kinds := schema.Deserialize(response, &dst)

	stream := &Stream[T]{}

	if _, ok := kinds["topic"]; ok {
		srcTopic := "topic"
		stream.sourceTopic = &srcTopic
	}

	if _, ok := kinds["stream"]; ok {
		srcStream := "stream"
		stream.sourceStream = &srcStream
	}

	if _, ok := kinds["partitions"]; ok {
		partitions := uint8(1)
		stream.partitions = &partitions
	}
	if _, ok := kinds["value_format"]; ok {
		vf := schema.Json
		stream.vf = vf
	}

	return stream, nil
}

func GetStreamProjection(
	streamName string) (StreamSettings, error) {

	settings, exists := existingStreams[streamName]
	if !exists {
		return StreamSettings{}, ErrStreamDoesNotExist
	}

	return settings, nil
}

func (s *Stream[S]) Describe() {
	protocol.KafkaSerializer{
		QueryAlgo: ksql.Query{
			Query: ksql.DESCRIBE,
			Name:  s.name,
		},
	}.Query()
}

func (s *Stream[S]) Drop() {
	protocol.KafkaSerializer{
		QueryAlgo: ksql.Query{
			Query: ksql.DROP,
			Ref:   ksql.STREAM,
			Name:  s.name,
		},
	}.Query()
}

func (s *Stream[S]) ToTopic(topicName string) proxy.Topic[S] {
	return proxy.CreateTopicFromStream[S](topicName, s)
}

func (s *Stream[S]) ToTable(tableName string) proxy.Table[S] {
	return proxy.CreateTableFromStream[S](tableName, s)
}

func (s *Stream[S]) SelectOnce(ctx context.Context, query string) (S, error) {
	var (
		value S
	)

	return value, nil
}

func (s *Stream[S]) SelectWithEmit(ctx context.Context, query string) (<-chan S, error) {

	var (
		value   S
		valuesC = make(chan S)
	)

	go func() {
		for {
			select {
			case <-ctx.Done():
				close(valuesC)
				return
			default:
				valuesC <- value
			}
		}
	}()

	return valuesC, nil
}

func (s *Stream[S]) Insert(ctx context.Context, fields map[string]string) error {
	protocol.KafkaSerializer{
		QueryAlgo: ksql.Query{
			Query: ksql.INSERT,
			Ref:   ksql.STREAM,
			Name:  s.name,
			CTE:   nil,
		},
		SchemaAlgo:   nil,
		JoinAlgo:     ksql.Join{},
		CondAlgo:     ksql.Cond{},
		GroupBy:      nil,
		MetadataAlgo: ksql.With{},
		CTE:          nil,
	}.Query()
}

func (s *Stream[S]) InsertAs(ctx context.Context, serializer protocol.KafkaSerializer) {

}
