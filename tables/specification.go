package tables

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"ksql/kernel/network"
	"ksql/kernel/protocol"
	"ksql/ksql"
	"ksql/proxy"
	"ksql/schema"
	"net/http"
	"reflect"
)

type Table[S any] struct {
	Name         string
	sourceTopic  *string
	sourceStream *string
	partitions   *uint8
	remoteSchema *reflect.Type
	format       schema.ValueFormat
}

func ListTables() {
	query := []byte(
		protocol.KafkaSerializer{
			QueryAlgo: ksql.Query{
				Query: ksql.LIST,
				Ref:   ksql.TABLE,
			}}.
			Query())

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

	if err = network.Net.PerformRequest(
		req,
		&network.SingeHandler{},
	); err != nil {
		return
	}
}

func (s *Table[S]) Describe() {
	query := []byte(protocol.KafkaSerializer{
		QueryAlgo: ksql.Query{
			Query: ksql.DESCRIBE,
			Name:  s.Name,
		},
	}.Query())

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

	if err = network.Net.PerformRequest(
		req,
		&network.SingeHandler{},
	); err != nil {
		return
	}
}

func (s *Table[S]) Drop() {
	query := []byte(protocol.KafkaSerializer{
		QueryAlgo: ksql.Query{
			Query: ksql.DROP,
			Ref:   ksql.TABLE,
			Name:  s.Name,
		},
	}.Query())

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

	if err = network.Net.PerformRequest(
		req,
		&network.SingeHandler{},
	); err != nil {
		return
	}
}

func GetTable[S any](
	ctx context.Context,
	stream string,
	settings TableSettings) (*Table[S], error) {

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
		return nil, ErrTableDoesNotExist
	}

	remoteSchema := schema.SerializeRemoteSchema(responseSchema)
	matchMap, diffMap := schema.CompareStructs(scheme, remoteSchema)

	if len(matchMap) == 0 {
		fmt.Println(diffMap)
		return nil, errors.New("schemes doesnt match")
	}

	return &Table[S]{
		Name:         stream,
		sourceStream: nil,
		partitions:   settings.Partitions,
		remoteSchema: &scheme,
		format:       settings.Format,
	}, nil
}

func CreateTable[S any](
	ctx context.Context,
	tableName string,
	settings TableSettings) (*Table[S], error) {

	var (
		s S
	)

	rmSchema := schema.SerializeProvidedStruct(s)

	protocol.KafkaSerializer{
		QueryAlgo: ksql.Query{
			Query: ksql.CREATE,
			Ref:   ksql.TABLE,
			Name:  tableName,
		},
		SchemaAlgo: schema.GetTypeFields(
			tableName,
			rmSchema,
		),
		MetadataAlgo: ksql.With{
			Topic:       *settings.SourceTopic,
			ValueFormat: schema.JSON.String(),
		},
	}.Query()

	return &Table[S]{
		sourceTopic:  settings.SourceTopic,
		sourceStream: &tableName,
		partitions:   settings.Partitions,
		remoteSchema: &rmSchema,
		format:       settings.Format,
	}, nil
}

func CreateTableAsSelect[S any](
	ctx context.Context,
	tableName string,
	settings TableSettings,
	query proxy.QueryPlan) (*Table[S], error) {

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
			Ref:   ksql.TABLE,
			Name:  tableName,
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

	return &Table[S]{
		sourceTopic:  settings.SourceTopic,
		sourceStream: &tableName,
		partitions:   settings.Partitions,
		remoteSchema: &scheme,
		format:       settings.Format,
	}, nil

}

func (s *Table[S]) SelectOnce(
	ctx context.Context) (S, error) {

	var (
		value S
	)

	protocol.KafkaSerializer{
		QueryAlgo: ksql.Query{
			Query: ksql.SELECT,
			Ref:   ksql.TABLE,
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

func (s *Table[S]) SelectWithEmit(
	ctx context.Context) (<-chan S, error) {

	var (
		value   S
		valuesC = make(chan S)
	)

	protocol.KafkaSerializer{
		QueryAlgo: ksql.Query{
			Query: ksql.SELECT,
			Ref:   ksql.TABLE,
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

func (s *Table[S]) ToTopic(topicName string) proxy.Topic[S] {
	return proxy.CreateTopicFromTable[S](topicName, s)
}

func (s *Table[S]) ToStream(streamName string) proxy.Stream[S] {
	return proxy.CreateStreamFromTable[S](streamName, s)
}
