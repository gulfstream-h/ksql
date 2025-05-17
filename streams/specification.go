package streams

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"ksql/constants"
	"ksql/kernel/network"
	"ksql/kernel/protocol"
	"ksql/kernel/protocol/dao"
	"ksql/kernel/protocol/dto"
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

func ListStreams(ctx context.Context) dto.ShowStreams {
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
		return dto.ShowStreams{}
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
		return dto.ShowStreams{}
	case val, ok := <-pipeline:
		if !ok {
			return dto.ShowStreams{}
		}

		var (
			streams dao.ShowStreams
		)

		if err = jsoniter.Unmarshal(val, &streams); err != nil {
			return dto.ShowStreams{}
		}

		return streams.DTO()
	}
}

func (s *Stream[S]) Describe(ctx context.Context) dto.RelationDescription {
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
		return dto.RelationDescription{}
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
		return dto.RelationDescription{}
	case val, ok := <-pipeline:
		if !ok {
			return dto.RelationDescription{}
		}

		var (
			describe dao.DescribeResponse
		)

		if err = jsoniter.Unmarshal(val, &describe); err != nil {
			return dto.RelationDescription{}
		}

		return describe.DTO()
	}
}

func (s *Stream[S]) Drop(ctx context.Context) error {
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
		return err
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
		return errors.New("context is done")
	case val, ok := <-pipeline:
		if !ok {
			return errors.New("drop response channel is closed")
		}

		var (
			drop dao.DropInfo
		)

		if err = jsoniter.Unmarshal(val, &drop); err != nil {
			return fmt.Errorf("cannot unmarshal drop response: %w", err)
		}

		if drop.CommandStatus.Status != "SUCCESS" {
			return fmt.Errorf("cannot drop stream: %w", drop.CommandStatus.Status)
		}

		return nil
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

	streamInstance := &Stream[S]{
		Name:         stream,
		sourceStream: nil,
		partitions:   settings.Partitions,
		remoteSchema: &scheme,
		vf:           settings.format,
	}
	desc := streamInstance.Describe(ctx)

	var (
		responseSchema = map[string]string{}
	)

	for _, field := range desc.Fields {
		responseSchema[field.Name] = field.Kind
	}

	if responseSchema == nil {
		return nil, constants.ErrStreamDoesNotExist
	}

	remoteSchema := schema.SerializeRemoteSchema(responseSchema)
	matchMap, diffMap := schema.CompareStructs(scheme, remoteSchema)

	if len(matchMap) == 0 {
		fmt.Println(diffMap)
		return nil, errors.New("schemes doesnt match")
	}

	return streamInstance, nil
}

func CreateStream[S any](
	ctx context.Context,
	streamName string,
	settings StreamSettings) (*Stream[S], error) {

	var (
		s S
	)

	rmSchema := schema.SerializeProvidedStruct(s)

	query := []byte(protocol.KafkaSerializer{
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
	}.Query())

	var (
		pipeline = make(chan []byte)
	)

	req, err := http.NewRequest(
		"POST",
		"localhost:8080",
		bytes.NewReader(query))
	if err != nil {
		return nil, err
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
		return nil, errors.New("context is done")
	case val, ok := <-pipeline:
		if !ok {
			return nil, errors.New("drop response channel is closed")
		}

		var (
			create dao.CreateRelationResponse
		)

		if err = jsoniter.Unmarshal(val, &create); err != nil {
			return nil, fmt.Errorf("cannot unmarshal create response: %w", err)
		}

		if len(create) < 1 {
			return nil, fmt.Errorf("unsuccessful response")
		}

		status := create[0]

		if status.CommandStatus.Status != "SUCCESSFUL" {
			return nil, fmt.Errorf("unsuccesful respose. msg: %s", status.CommandStatus.Message)
		}

		return &Stream[S]{
			sourceTopic:  settings.SourceTopic,
			sourceStream: &streamName,
			partitions:   settings.Partitions,
			remoteSchema: &rmSchema,
			vf:           settings.format,
		}, nil
	}
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

	q := []byte(protocol.KafkaSerializer{
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
			"AS": query,
		},
	}.Query())

	var (
		pipeline = make(chan []byte)
	)

	req, err := http.NewRequest(
		"POST",
		"localhost:8080",
		bytes.NewReader(q))
	if err != nil {
		return nil, err
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
		return nil, errors.New("context is done")
	case val, ok := <-pipeline:
		if !ok {
			return nil, errors.New("drop response channel is closed")
		}

		var (
			create dao.CreateRelationResponse
		)

		if err = jsoniter.Unmarshal(val, &create); err != nil {
			return nil, fmt.Errorf("cannot unmarshal create response: %w", err)
		}

		if len(create) < 1 {
			return nil, fmt.Errorf("unsuccessful response")
		}

		status := create[0]

		if status.CommandStatus.Status != "SUCCESSFUL" {
			return nil, fmt.Errorf("unsuccesful respose. msg: %s", status.CommandStatus.Message)
		}

		return &Stream[S]{
			sourceTopic:  settings.SourceTopic,
			sourceStream: &streamName,
			partitions:   settings.Partitions,
			remoteSchema: &scheme,
			vf:           settings.format,
		}, nil
	}
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

	query := []byte(protocol.KafkaSerializer{
		SchemaAlgo: searchFields,
		QueryAlgo: ksql.Query{
			Query: ksql.INSERT,
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
		return err
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
		return errors.New("context is done")
	case val, ok := <-pipeline:
		if !ok {
			return errors.New("insert response channel is closed")
		}

		var (
			insert dao.CreateRelationResponse
		)

		if err = jsoniter.Unmarshal(val, &insert); err != nil {
			return fmt.Errorf("cannot unmarshal insert response: %w", err)
		}

		if len(insert) == 0 {
			return nil
		}

		return errors.New("unpredictable error occurred while inserting")
	}

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

	q := []byte(protocol.KafkaSerializer{
		SchemaAlgo: query.SchemaAlgo,
		QueryAlgo: ksql.Query{
			Query: ksql.INSERT,
			Ref:   ksql.STREAM,
			Name:  s.Name,
		},
		CTE: map[string]protocol.KafkaSerializer{
			"AS": query,
		},
	}.Query())

	var (
		pipeline = make(chan []byte)
	)

	req, err := http.NewRequest(
		"POST",
		"localhost:8080",
		bytes.NewReader(q))
	if err != nil {
		return err
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
		return errors.New("context is done")
	case val, ok := <-pipeline:
		if !ok {
			return errors.New("insert response channel is closed")
		}

		var (
			insert dao.CreateRelationResponse
		)

		if err = jsoniter.Unmarshal(val, &insert); err != nil {
			return fmt.Errorf("cannot unmarshal insert response: %w", err)
		}

		if len(insert) == 0 {
			return nil
		}

		return errors.New("unpredictable error occurred while inserting")
	}
}

func (s *Stream[S]) SelectOnce(
	ctx context.Context) (S, error) {

	var (
		value S
	)

	query := []byte(protocol.KafkaSerializer{
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
	}.Query())

	var (
		pipeline = make(chan []byte)
	)

	req, err := http.NewRequest(
		"POST",
		"localhost:8080",
		bytes.NewReader(query))
	if err != nil {
		return value, err
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
		return value, errors.New("context is done")
	case val, ok := <-pipeline:
		if !ok {
			return value, errors.New("select response channel is closed")
		}

		if err = jsoniter.Unmarshal(val, &value); err != nil {
			return value, fmt.Errorf("cannot unmarshal select response: %w", err)
		}

		return value, nil
	}

}

func (s *Stream[S]) SelectWithEmit(
	ctx context.Context) (<-chan S, error) {

	var (
		value   S
		valuesC = make(chan S)
	)

	query := []byte(protocol.KafkaSerializer{
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
	}.Query())

	var (
		pipeline = make(chan []byte)
	)

	req, err := http.NewRequest(
		"POST",
		"localhost:8080",
		bytes.NewReader(query))
	if err != nil {
		return nil, err
	}

	req.Header.Set(
		"Content-Type",
		"application/json")

	go func() {
		network.Net.PerformRequest(
			req,
			&network.SocketHandler{
				MaxRPS:   100,
				Pipeline: pipeline,
			},
		)
	}()

	select {
	case <-ctx.Done():
		return nil, errors.New("context is done")
	case val, ok := <-pipeline:
		if !ok {
			return nil, errors.New("select response channel is closed")
		}

		if err = jsoniter.Unmarshal(val, &value); err != nil {
			return nil, fmt.Errorf("cannot unmarshal select response: %w", err)
		}

		return valuesC, nil
	}
}

func (s *Stream[S]) ToTopic(topicName string) (topic constants.Topic[S]) {
	topic.Name = topicName
	topic.Partitions = int(*s.partitions)

	return
}

func (s *Stream[S]) ToTable(tableName string) (table constants.Table[S]) {
	table.Name = tableName

	return
}
