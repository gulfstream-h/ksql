package streams

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"ksql/constants"
	"ksql/kernel/network"
	"ksql/kernel/protocol"
	"ksql/ksql"
	"ksql/schema"
	"net/http"
	"reflect"
)

type Stream[S any] struct {
	Name         string
	sourceTopic  *string
	sourceStream *string
	partitions   *uint8
	remoteSchema *reflect.Type
	vf           schema.ValueFormat
}

func ListStreams(ctx context.Context) {
	query := []byte(
		protocol.KafkaSerializer{
			QueryAlgo: ksql.Query{
				Query: ksql.LIST,
				Ref:   ksql.STREAM,
			}}.
			Query())

	var (
		pipeline = make(chan []byte)
	)

	req, err := http.NewRequest(
		"POST",
		"localhost:8080",
		bytes.NewReader(query))
	if err != nil {
		return
	}

	req.Header.Set(
		"Content-Type",
		"application/json")

	go func() {
		network.Net.PerformRequest(
			req,
			&network.SingeHandler{
				MaxRPS:   100,
				Pipeline: pipeline,
			},
		)
	}()

	select {
	case <-ctx.Done():
		return
	case val, ok := <-pipeline:
		if !ok {
			return
		}

		fmt.Println(string(val))
	}
}

func (s *Stream[S]) Describe(ctx context.Context) {
	query := []byte(protocol.KafkaSerializer{
		QueryAlgo: ksql.Query{
			Query: ksql.DESCRIBE,
			Name:  s.Name,
		},
	}.Query())

	var (
		pipeline = make(chan []byte)
	)

	req, err := http.NewRequest(
		"POST",
		"localhost:8080",
		bytes.NewReader(query))
	if err != nil {
		return
	}

	req.Header.Set(
		"Content-Type",
		"application/json")

	go func() {
		network.Net.PerformRequest(
			req,
			&network.SingeHandler{
				MaxRPS:   100,
				Pipeline: pipeline,
			},
		)
	}()

	select {
	case <-ctx.Done():
		return
	case val, ok := <-pipeline:
		if !ok {
			return
		}

		fmt.Println(string(val))
	}
}

func (s *Stream[S]) Drop(ctx context.Context) {
	query := []byte(protocol.KafkaSerializer{
		QueryAlgo: ksql.Query{
			Query: ksql.DROP,
			Ref:   ksql.STREAM,
			Name:  s.Name,
		},
	}.Query())

	var (
		pipeline = make(chan []byte)
	)

	req, err := http.NewRequest(
		"POST",
		"localhost:8080",
		bytes.NewReader(query))
	if err != nil {
		return
	}

	req.Header.Set(
		"Content-Type",
		"application/json")

	go func() {
		network.Net.PerformRequest(
			req,
			&network.SingeHandler{
				MaxRPS:   100,
				Pipeline: pipeline,
			},
		)
	}()

	select {
	case <-ctx.Done():
		return
	case val, ok := <-pipeline:
		if !ok {
			return
		}

		fmt.Println(string(val))
	}
}

func GetStream[S any](
	ctx context.Context,
	stream string,
	settings StreamSettings) (*Stream[S], error) {

	var (
		s S
	)

	scheme := schema.SerializeProvidedStruct(s)

	protocol.KafkaSerializer{
		QueryAlgo: ksql.Query{
			Query: ksql.DESCRIBE,
			Name:  stream,
		},
	}.Query()

	// TODO make request
	var (
		responseSchema map[string]string
	)

	if responseSchema == nil {
		return nil, constants.ErrStreamDoesNotExist
	}

	remoteSchema := schema.SerializeRemoteSchema(responseSchema)
	matchMap, diffMap := schema.CompareStructs(scheme, remoteSchema)

	if len(matchMap) == 0 {
		fmt.Println(diffMap)
		return nil, errors.New("schemes doesnt match")
	}

	return &Stream[S]{
		Name:         stream,
		sourceStream: nil,
		partitions:   settings.Partitions,
		remoteSchema: &scheme,
		vf:           settings.format,
	}, nil
}

func CreateStream[S any](
	ctx context.Context,
	streamName string,
	settings StreamSettings) (*Stream[S], error) {

	var (
		s S
	)

	rmSchema := schema.SerializeProvidedStruct(s)

	protocol.KafkaSerializer{
		QueryAlgo: ksql.Query{
			Query: ksql.CREATE,
			Ref:   ksql.STREAM,
			Name:  streamName,
		},
		SchemaAlgo: schema.GetTypeFields(
			streamName,
			rmSchema,
		),
		MetadataAlgo: ksql.With{
			Topic:       *settings.SourceTopic,
			ValueFormat: schema.JSON.String(),
		},
	}.Query()

	return &Stream[S]{
		sourceTopic:  settings.SourceTopic,
		sourceStream: &streamName,
		partitions:   settings.Partitions,
		remoteSchema: &rmSchema,
		vf:           settings.format,
	}, nil
}

func CreateStreamAsSelect[S any](
	ctx context.Context,
	streamName string,
	settings StreamSettings,
	query constants.QueryPlan) (*Stream[S], error) {

	var (
		s S
	)

	scheme := schema.SerializeProvidedStruct(s)
	rmScheme := schema.SerializeFields(query.SchemaAlgo)

	matchMap, diffMap := schema.CompareStructs(scheme, rmScheme)

	if len(matchMap) == 0 {
		fmt.Println(diffMap)
		return nil, errors.New("schemes doesnt match")
	}

	protocol.KafkaSerializer{
		QueryAlgo: ksql.Query{
			Query: ksql.CREATE,
			Ref:   ksql.STREAM,
			Name:  streamName,
		},
		SchemaAlgo: query.SchemaAlgo,
		MetadataAlgo: ksql.With{
			Topic:       *settings.SourceTopic,
			ValueFormat: schema.JSON.String(),
		},
		CTE: map[string]protocol.KafkaSerializer{
			"AS": protocol.KafkaSerializer(query),
		},
	}.Query()

	return &Stream[S]{
		sourceTopic:  settings.SourceTopic,
		sourceStream: &streamName,
		partitions:   settings.Partitions,
		remoteSchema: &scheme,
		vf:           settings.format,
	}, nil

}

func (s *Stream[S]) Insert(
	ctx context.Context,
	fields map[string]string) error {

	scheme := *s.remoteSchema

	remoteProjection := schema.GetTypeFieldsAsMap(
		s.Name,
		scheme,
	)

	var (
		searchFields []schema.SearchField
	)

	for key, value := range fields {
		field, ok := remoteProjection[key]
		if !ok {
			continue
		}

		field.Value = value

		searchFields = append(searchFields, field)
	}

	protocol.KafkaSerializer{
		SchemaAlgo: searchFields,
		QueryAlgo: ksql.Query{
			Query: ksql.INSERT,
			Ref:   ksql.STREAM,
			Name:  s.Name,
		},
	}.Query()

	return nil
}

func (s *Stream[S]) InsertAs(
	ctx context.Context,
	query protocol.KafkaSerializer) error {

	scheme := *s.remoteSchema
	rmScheme := schema.SerializeFields(query.SchemaAlgo)

	matchMap, diffMap := schema.CompareStructs(scheme, rmScheme)

	if len(matchMap) == 0 {
		fmt.Println(diffMap)
		return errors.New("schemes doesnt match")
	}

	protocol.KafkaSerializer{
		SchemaAlgo: query.SchemaAlgo,
		QueryAlgo: ksql.Query{
			Query: ksql.INSERT,
			Ref:   ksql.STREAM,
			Name:  s.Name,
		},
		CTE: map[string]protocol.KafkaSerializer{
			"AS": query,
		},
	}.Query()

	return nil
}

func (s *Stream[S]) SelectOnce(
	ctx context.Context) (S, error) {

	var (
		value S
	)

	protocol.KafkaSerializer{
		QueryAlgo: ksql.Query{
			Query: ksql.SELECT,
			Ref:   ksql.STREAM,
			Name:  s.Name,
		},
		SchemaAlgo: schema.GetTypeFields(
			s.Name,
			*s.remoteSchema,
		),
		MetadataAlgo: ksql.With{
			ValueFormat: schema.JSON.String(),
		},
	}.Query()

	return value, nil
}

func (s *Stream[S]) SelectWithEmit(
	ctx context.Context) (<-chan S, error) {

	var (
		value   S
		valuesC = make(chan S)
	)

	protocol.KafkaSerializer{
		QueryAlgo: ksql.Query{
			Query: ksql.SELECT,
			Ref:   ksql.STREAM,
			Name:  s.Name,
		},
		SchemaAlgo: schema.GetTypeFields(
			s.Name,
			*s.remoteSchema,
		),
		MetadataAlgo: ksql.With{
			ValueFormat: schema.JSON.String(),
		},
	}.Query()

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

func (s *Stream[S]) ToTopic(topicName string) (topic constants.Topic[S]) {
	topic.Name = topicName
	return
}

func (s *Stream[S]) ToTable(tableName string) (table constants.Table[S]) {
	table.Name = tableName
	return
}
