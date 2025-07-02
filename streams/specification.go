package streams

import (
	"context"
	"errors"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"ksql/consts"
	"ksql/database"
	errors2 "ksql/errors"
	"ksql/internal/kernel/network"
	dao2 "ksql/internal/kernel/protocol/dao"
	dto2 "ksql/internal/kernel/protocol/dto"
	schema2 "ksql/internal/schema"
	"ksql/internal/schema/report"
	util2 "ksql/internal/util"
	"ksql/kinds"
	"ksql/ksql"
	"ksql/shared"
	"ksql/static"
	"log/slog"
	"strings"

	"net/http"
)

// Stream - is full-functional type,
// providing all ksql-supported operations
// via referred to type functions calls
type Stream[S any] struct {
	Name         string
	partitions   int
	remoteSchema schema2.LintedFields
	format       kinds.ValueFormat
}

// ListStreams - responses with all streams list
// in the current ksqlDB instance
func ListStreams(ctx context.Context) (dto2.ShowStreams, error) {

	query := util2.MustNoError(ksql.List(ksql.STREAM).Expression)

	pipeline, err := network.Net.Perform(
		ctx,
		http.MethodPost,
		query,
		&network.ShortPolling{},
	)
	if err != nil {
		err = fmt.Errorf("cannot perform request: %w", err)
		return dto2.ShowStreams{}, err
	}

	select {
	case <-ctx.Done():
		return dto2.ShowStreams{}, ctx.Err()
	case val, ok := <-pipeline:
		if !ok {
			return dto2.ShowStreams{}, errors2.ErrMalformedResponse
		}

		var (
			streams []dao2.StreamsInfo
		)

		if err = jsoniter.Unmarshal(val, &streams); err != nil {
			err = errors.Join(errors2.ErrUnserializableResponse, err)
			return dto2.ShowStreams{}, err
		}

		if len(streams) == 0 {
			return dto2.ShowStreams{}, errors.New("no streams have been found")
		}

		return streams[0].DTO(), nil
	}
}

// Describe - responses with stream description
func Describe(ctx context.Context, stream string) (dto2.RelationDescription, error) {
	query := util2.MustNoError(ksql.Describe(ksql.STREAM, stream).Expression)

	pipeline, err := network.Net.Perform(
		ctx,
		http.MethodPost,
		query,
		&network.ShortPolling{},
	)
	if err != nil {
		err = fmt.Errorf("cannot perform request: %w", err)
		return dto2.RelationDescription{}, err
	}

	select {
	case <-ctx.Done():
		return dto2.RelationDescription{}, ctx.Err()
	case val, ok := <-pipeline:
		if !ok {
			return dto2.RelationDescription{}, errors2.ErrMalformedResponse
		}

		var (
			describe []dao2.DescribeResponse
		)
		slog.Info("response", "formatted", string(val))

		if strings.Contains(string(val), "Could not find STREAM/TABLE") {
			return dto2.RelationDescription{}, errors2.ErrStreamDoesNotExist
		}

		if err = jsoniter.Unmarshal(val, &describe); err != nil {
			err = errors.Join(errors2.ErrUnserializableResponse, err)
			return dto2.RelationDescription{}, err
		}

		if len(describe) == 0 {
			return dto2.RelationDescription{}, errors2.ErrStreamDoesNotExist
		}

		return describe[0].DTO(), nil
	}
}

// Drop - drops stream from ksqlDB instance
// with parent topic
func Drop(ctx context.Context, stream string) error {

	query := util2.MustNoError(ksql.Drop(ksql.STREAM, stream).Expression)

	pipeline, err := network.Net.Perform(
		ctx,
		http.MethodPost,
		query,
		&network.ShortPolling{},
	)
	if err != nil {
		return fmt.Errorf("cannot perform request: %w", err)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case val, ok := <-pipeline:
		if !ok {
			return errors2.ErrMalformedResponse
		}

		var (
			drop []dao2.DropInfo
		)

		slog.Debug("received from pipiline", slog.String("val", string(val)))

		if err = jsoniter.Unmarshal(val, &drop); err != nil {
			return fmt.Errorf("cannot unmarshal drop response: %w", err)
		}

		if len(drop) == 0 {
			return errors.New("cannot drop stream")
		}

		if drop[0].CommandStatus.Status != consts.SUCCESS {
			return fmt.Errorf("cannot drop stream: %s", drop[0].CommandStatus.Status)
		}

		return nil
	}
}

// GetStream - gets table from ksqlDB instance
// by receiving http description from settings
// or from cache if reflection mode is enabled
// if user-provided struct doesn't match
// with ksql Description - function returns detailed error
func GetStream[S any](
	ctx context.Context,
	stream string) (*Stream[S], error) {

	var (
		s S
	)

	scheme, err := schema2.NativeStructRepresentation(stream, s)
	if err != nil {
		return nil, err
	}

	streamInstance := &Stream[S]{
		Name:         stream,
		remoteSchema: scheme,
	}
	desc, err := Describe(ctx, stream)
	if err != nil {
		if errors.Is(err, errors2.ErrStreamDoesNotExist) || len(desc.Fields) == 0 {
			return nil, err
		}
		return nil, fmt.Errorf("cannot get stream description: %w", err)
	}

	var (
		responseSchema = make(map[string]string)
	)

	for _, field := range desc.Fields {
		responseSchema[field.Name] = field.Kind
	}

	remoteSchema := schema2.RemoteFieldsRepresentation(stream, responseSchema)
	if err = remoteSchema.CompareWithFields(scheme.Array()); err != nil {
		return nil, fmt.Errorf("reflection check failed: %w", err)
	}

	return streamInstance, nil
}

// CreateStream - creates stream in ksqlDB instance
func CreateStream[S any](
	ctx context.Context,
	streamName string,
	settings shared.StreamSettings,
) (*Stream[S], error) {

	var (
		s S
	)

	err := settings.Validate()
	if err != nil {
		return nil, fmt.Errorf("validate settings: %w", err)
	}

	rmSchema, err := schema2.NativeStructRepresentation(streamName, s)
	if err != nil {
		return nil, err
	}

	if len(rmSchema.Map()) == 0 {
		return nil, fmt.Errorf("cannot create stream with empty schema")
	}

	metadata := ksql.Metadata{
		Topic:       settings.SourceTopic,
		Partitions:  settings.Partitions,
		ValueFormat: kinds.JSON.String(),
	}

	query, _ := ksql.Create(ksql.STREAM, streamName).
		SchemaFields(rmSchema.Array()...).
		With(metadata).
		Expression()

	pipeline, err := network.Net.Perform(
		ctx,
		http.MethodPost,
		query,
		&network.ShortPolling{},
	)
	if err != nil {
		return nil, fmt.Errorf("cannot perform request: %w", err)
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case val, ok := <-pipeline:
		if !ok {
			return nil, errors2.ErrMalformedResponse
		}

		var (
			create []dao2.CreateRelationResponse
		)

		slog.Debug(
			"received from create stream",
			slog.String("value", string(val)),
		)

		if err = jsoniter.Unmarshal(val, &create); err != nil {
			return nil, fmt.Errorf("cannot unmarshal create response: %w", err)
		}

		if len(create) < 1 {
			return nil, fmt.Errorf("unsuccessful response")
		}

		status := create[0]

		if status.CommandStatus.Status != consts.SUCCESS {
			return nil, fmt.Errorf("unsuccesful respose. msg: %s", status.CommandStatus.Message)
		}

		static.StreamsProjections.Set(streamName, settings, rmSchema)

		return &Stream[S]{
			Name:         streamName,
			partitions:   settings.Partitions,
			remoteSchema: rmSchema,
			format:       settings.Format,
		}, nil
	}
}

// CreateStreamAsSelect - creates table in ksqlDB instance
// with user built query
func CreateStreamAsSelect[S any](
	ctx context.Context,
	streamName string,
	settings shared.StreamSettings,
	selectBuilder ksql.SelectBuilder) (*Stream[S], error) {

	var (
		s S
	)
	if selectBuilder == nil {
		return nil, errors.New("select builder cannot be nil")
	}

	fields := selectBuilder.Returns()

	if len(fields.Map()) == 0 {
		return nil, errors.New("select builder must return at least one field")
	}

	if static.ReflectionFlag {
		err := report.ReflectionReportNative(s, fields)
		if err != nil {
			return nil, fmt.Errorf("reflection report native: %w", err)
		}

		for relName, rel := range selectBuilder.RelationReport() {
			err = report.ReflectionReportRemote(relName, rel.Map())
			if err != nil {
				return nil, fmt.Errorf("reflection report remote: %w", err)
			}
		}
	}

	query, err := ksql.Create(ksql.STREAM, streamName).
		AsSelect(selectBuilder).
		With(ksql.Metadata{
			Topic:       settings.SourceTopic,
			ValueFormat: kinds.JSON.String(),
		}).
		Expression()

	if err != nil {
		return nil, fmt.Errorf("build create query: %w", err)
	}

	pipeline, err := network.Net.Perform(
		ctx,
		http.MethodPost,
		query,
		&network.ShortPolling{},
	)
	if err != nil {
		err = fmt.Errorf("cannot perform request: %w", err)
		return nil, fmt.Errorf("cannot perform request: %w", err)
	}

	select {
	case <-ctx.Done():
		return nil, errors.New("context is done")
	case val, ok := <-pipeline:
		if !ok {
			return nil, errors.New("drop response channel is closed")
		}

		var (
			create []dao2.CreateRelationResponse
		)

		if err := jsoniter.Unmarshal(val, &create); err != nil {
			slog.Debug("raw received", slog.String("raw", string(val)))
			return nil, fmt.Errorf("cannot unmarshal create response: %w", err)
		}

		if len(create) < 1 {
			return nil, fmt.Errorf("unsuccessful response")
		}

		status := create[0]

		if status.CommandStatus.Status != consts.SUCCESS {
			return nil, fmt.Errorf("unsuccesful respose. msg: %s", status.CommandStatus.Message)
		}

		static.StreamsProjections.Set(streamName, settings, fields)

		return &Stream[S]{
			partitions:   settings.Partitions,
			Name:         streamName,
			remoteSchema: fields,
			format:       settings.Format,
		}, nil
	}
}

func (s *Stream[S]) Insert(
	ctx context.Context,
	val S,
) error {
	query, err := ksql.
		Insert(ksql.STREAM, s.Name).
		InsertStruct(val).
		Expression()

	if err != nil {
		return fmt.Errorf("construct query: %w", err)
	}

	pipeline, err := network.Net.Perform(
		ctx,
		http.MethodPost,
		query,
		&network.ShortPolling{},
	)
	if err != nil {
		return fmt.Errorf("cannot perform request: %w", err)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case val, ok := <-pipeline:
		if !ok {
			return errors2.ErrMalformedResponse
		}

		var (
			insert []dao2.CreateRelationResponse
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

// InsertRow - provides insertion to stream functionality
// written fields are defined by user
func (s *Stream[S]) InsertRow(
	ctx context.Context,
	fields ksql.Row,
) error {

	if static.ReflectionFlag {
		scheme := s.remoteSchema
		relationCachedFields := scheme.Map()
		for key, value := range fields {
			field, ok := relationCachedFields[key]
			if !ok {
				return fmt.Errorf("field %s is not represented in remote schema", field.Name)
			}

			val := util2.Serialize(value)
			field.Value = &val

		}
	}

	query, err := ksql.Insert(ksql.STREAM, s.Name).Rows(fields).Expression()
	if err != nil {
		return fmt.Errorf("build insert query: %w", err)
	}

	pipeline, err := network.Net.Perform(
		ctx,
		http.MethodPost,
		query,
		&network.ShortPolling{},
	)
	if err != nil {
		return fmt.Errorf("cannot perform request: %w", err)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case val, ok := <-pipeline:
		if !ok {
			return errors2.ErrMalformedResponse
		}

		var (
			insert []dao2.CreateRelationResponse
		)

		if err = jsoniter.Unmarshal(val, &insert); err != nil {
			slog.Debug(
				"unmarshal failed",
				slog.String("raw", string(val)),
			)
			return fmt.Errorf("cannot unmarshal insert response: %w", err)
		}

		if len(insert) == 0 {
			return nil
		}

		return errors.New("unpredictable error occurred while inserting")
	}

}

// InsertAs - provides insertion to stream.
// written fields are pre-fetched from select query, which
// is built by user
func (s *Stream[S]) InsertAsSelect(
	ctx context.Context,
	selectBuilder ksql.SelectBuilder,
) error {

	var (
		stream S
	)
	if selectBuilder == nil {
		return errors.New("select builder cannot be nil")
	}

	if static.ReflectionFlag {
		fields := selectBuilder.Returns()

		err := report.ReflectionReportNative(stream, fields)
		if err != nil {
			return fmt.Errorf("reflection report native: %w", err)
		}

		for relName, rel := range selectBuilder.RelationReport() {
			err = report.ReflectionReportRemote(relName, rel.Map())
			if err != nil {
				return fmt.Errorf("reflection report remote: %w", err)
			}
		}
	}

	query, err := ksql.Insert(ksql.STREAM, s.Name).AsSelect(selectBuilder).Expression()
	if err != nil {
		return fmt.Errorf("build insert query: %w", err)
	}

	pipeline, err := network.Net.Perform(
		ctx,
		http.MethodPost,
		query,
		&network.ShortPolling{},
	)
	if err != nil {
		return fmt.Errorf("cannot perform request: %w", err)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case val, ok := <-pipeline:
		if !ok {
			return errors.New("insert response channel is closed")
		}

		var (
			insert []dao2.CreateRelationResponse
		)

		slog.Info("response", "formatted", string(val))

		if err = jsoniter.Unmarshal(val, &insert); err != nil {
			return fmt.Errorf("cannot unmarshal insert response: %w", err)
		}

		if len(insert) == 1 && insert[0].CommandStatus.Status == consts.SUCCESS {
			return nil
		}

		return errors.New("unpredictable error occurred while inserting")
	}
}

// SelectOnce - performs select query
// and return only one http answer
// After channel closes
func (s *Stream[S]) SelectOnce(
	ctx context.Context) (S, error) {

	var (
		value S
	)

	var (
		fields []ksql.Field
	)

	for _, field := range s.remoteSchema.Array() {
		fields = append(fields, ksql.F(field.Name))
	}

	query, err := ksql.
		Select(fields...).
		From(ksql.Schema(s.Name, ksql.STREAM)).
		Expression()

	if err != nil {
		return value, fmt.Errorf("build select query: %w", err)
	}

	valuesC, err := database.Select[S](ctx, query)
	if err != nil {
		return value, err
	}

	value = <-valuesC

	return value, nil
}

// SelectWithEmit - performs
// select with emit request
// answer is received for every new record
// and propagated to channel
func (s *Stream[S]) SelectWithEmit(ctx context.Context) (
	<-chan S, context.CancelFunc, error,
) {

	ctx, cancel := context.WithCancel(ctx)

	var (
		fields []ksql.Field
	)

	for _, field := range s.remoteSchema.Array() {
		fields = append(fields, ksql.F(field.Name))
	}

	query, err := ksql.Select(fields...).
		From(ksql.Schema(s.Name, ksql.STREAM)).
		EmitChanges().
		Expression()

	if err != nil {
		return nil, cancel, fmt.Errorf("build select query: %w", err)
	}

	valuesC, err := database.Select[S](ctx, query)
	if err != nil {
		return nil, cancel, err
	}

	return valuesC, cancel, nil
}
