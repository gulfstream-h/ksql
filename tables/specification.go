package tables

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

type Table[S any] struct {
	Name         string
	sourceTopic  *string
	sourceStream *string
	partitions   *uint8
	remoteSchema *reflect.Type
	format       schema.ValueFormat
}

func ListTables(ctx context.Context) dto.ShowTables {
	query := []byte(
		protocol.KafkaSerializer{
			QueryAlgo: ksql.Query{
				Query: ksql.LIST,
				Ref:   ksql.TABLE,
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
		return dto.ShowTables{}
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
		return dto.ShowTables{}
	case val, ok := <-pipeline:
		if !ok {
			return dto.ShowTables{}
		}

		var (
			tables dao.ShowTables
		)

		if err = jsoniter.Unmarshal(val, &tables); err != nil {
			return dto.ShowTables{}
		}

		return tables.DTO()
	}
}

func (s *Table[S]) Describe(ctx context.Context) dto.RelationDescription {
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

func (s *Table[S]) Drop(ctx context.Context) error {
	query := []byte(protocol.KafkaSerializer{
		QueryAlgo: ksql.Query{
			Query: ksql.DROP,
			Ref:   ksql.TABLE,
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
			return fmt.Errorf("cannot drop stream: %s", drop.CommandStatus.Status)
		}

		return nil
	}
}

func GetTable[S any](
	ctx context.Context,
	table string,
	settings TableSettings) (*Table[S], error) {

	var (
		s S
	)

	scheme := schema.SerializeProvidedStruct(s)

	tableInstance := &Table[S]{
		Name:         table,
		sourceStream: nil,
		partitions:   settings.Partitions,
		remoteSchema: &scheme,
		format:       settings.Format,
	}
	desc := tableInstance.Describe(ctx)

	var (
		responseSchema = map[string]string{}
	)

	for _, field := range desc.Fields {
		responseSchema[field.Name] = field.Kind
	}

	if responseSchema == nil {
		return nil, constants.ErrTableDoesNotExist
	}

	remoteSchema := schema.SerializeRemoteSchema(responseSchema)
	matchMap, diffMap := schema.CompareStructs(scheme, remoteSchema)

	if len(matchMap) == 0 {
		fmt.Println(diffMap)
		return nil, errors.New("schemes doesnt match")
	}

	return tableInstance, nil
}

func CreateTable[S any](
	ctx context.Context,
	tableName string,
	settings TableSettings) (*Table[S], error) {

	var (
		s S
	)

	rmSchema := schema.SerializeProvidedStruct(s)

	query := []byte(protocol.KafkaSerializer{
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

		return &Table[S]{
			sourceTopic:  settings.SourceTopic,
			sourceStream: &tableName,
			partitions:   settings.Partitions,
			remoteSchema: &rmSchema,
			format:       settings.Format,
		}, nil
	}
}

func CreateTableAsSelect[S any](
	ctx context.Context,
	tableName string,
	settings TableSettings,
	query constants.QueryPlan) (*Table[S], error) {

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
			Ref:   ksql.TABLE,
			Name:  tableName,
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

		return &Table[S]{
			sourceTopic:  settings.SourceTopic,
			sourceStream: &tableName,
			partitions:   settings.Partitions,
			remoteSchema: &scheme,
			format:       settings.Format,
		}, nil
	}
}

func (s *Table[S]) SelectOnce(
	ctx context.Context) (S, error) {

	var (
		value S
	)

	query := []byte(protocol.KafkaSerializer{
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

func (s *Table[S]) SelectWithEmit(
	ctx context.Context) (<-chan S, error) {

	var (
		value   S
		valuesC = make(chan S)
	)

	query := []byte(protocol.KafkaSerializer{
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

func (s *Table[S]) ToTopic(topicName string) (topic constants.Topic[S]) {
	topic.Name = topicName
	topic.Partitions = int(*s.partitions)

	return
}

func (s *Table[S]) ToStream(streamName string) (stream constants.Stream[S]) {
	stream.Name = streamName
	return
}
