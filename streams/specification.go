package streams

import (
	"context"
	"errors"
	"github.com/fatih/structs"
	"ksql/formats"
	"ksql/kernel/network"
	"ksql/schema"
	"net"
)

type Stream[T any] struct {
	sourceTopic  *string
	sourceStream *string
	partitions   *uint8
	vf           formats.ValueFormat
}

var (
	existingStreams = make(map[string]StreamSettings)
)

var (
	ErrStreamDoesNotExist = errors.New("stream does not exist")
)

func createStreamRemotely[T any](
	ctx context.Context,
	conn net.Conn,
	streamName string,
	settings StreamSettings) (*Stream[T], error) {

	fields := structs.Fields(settings)

	format, err := settings.format.GetName()
	if err != nil {
		return nil, err
	}

	var (
		command = "CREATE STREAM " + streamName + " (" + fields[0].Name() + " " + fields[0].Kind().String() + ")" +
			" WITH (" + *settings.SourceTopic + ", " + format + ")"
	)

	//CREATE STREAM input_stream (
	//	key VARCHAR,
	//	value VARCHAR
	//) WITH (
	//	KAFKA_TOPIC='input-topic',
	//	VALUE_FORMAT='JSON',
	//	KEY_FORMAT='KAFKA'
	//);

	response, err := network.Perform(
		ctx,
		conn,
		conn.RemoteAddr().String(),
		len(command),
		command)
	if err != nil {
		return nil, err
	}

	if err = validateResponse(response); err != nil {
		return nil, err
	}

	return &Stream[T]{
		sourceTopic:  settings.SourceTopic,
		sourceStream: &streamName,
		partitions:   settings.Partitions,
		vf:           settings.format,
	}, nil
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
		vf := formats.Json
		stream.vf = vf
	}

	return stream, nil
}

func getStreamProjection(
	ctx context.Context,
	streamName string) (*StreamSettings, error) {

	settings, exists := existingStreams[streamName]
	if !exists {
		return nil, ErrStreamDoesNotExist
	}

	return &settings, nil
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

func validateResponse(response []byte) error {
	if string(response) == "ERROR" {
		return errors.New("error creating stream")
	}

	return nil
}
