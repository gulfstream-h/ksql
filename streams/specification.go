package streams

import (
	"context"
	"errors"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"ksql/consts"
	"ksql/kernel/network"
	"ksql/kernel/protocol/dao"
	"ksql/kernel/protocol/dto"
	"ksql/kinds"
	"ksql/ksql"
	"ksql/schema"
	"ksql/schema/netparse"
	"ksql/schema/report"
	"ksql/shared"
	"ksql/static"
	"log/slog"
	"strings"

	"ksql/util"
	"net/http"
)

// Stream - is full-functional type,
// providing all ksql-supported operations
// via referred to type functions calls
type Stream[S any] struct {
	Name         string
	partitions   uint8
	remoteSchema schema.LintedFields
	format       kinds.ValueFormat
}

// ListStreams - responses with all streams list
// in the current ksqlDB instance. Also it reloads
// map of available projections
func ListStreams(ctx context.Context) (dto.ShowStreams, error) {

	query := util.MustNoError(ksql.List(ksql.STREAM).Expression)

	pipeline, err := network.Net.Perform(
		ctx,
		http.MethodPost,
		query,
		&network.ShortPolling{},
	)
	if err != nil {
		err = fmt.Errorf("cannot perform request: %w", err)
		return dto.ShowStreams{}, err
	}

	select {
	case <-ctx.Done():
		return dto.ShowStreams{}, ctx.Err()
	case val, ok := <-pipeline:
		if !ok {
			return dto.ShowStreams{}, static.ErrMalformedResponse
		}

		var (
			streams []dao.StreamsInfo
		)

		if err = jsoniter.Unmarshal(val, &streams); err != nil {
			err = errors.Join(static.ErrUnserializableResponse, err)
			return dto.ShowStreams{}, err
		}

		if len(streams) == 0 {
			return dto.ShowStreams{}, errors.New("no streams have been found")
		}

		return streams[0].DTO(), nil
	}
}

// Describe - responses with stream description.
// Can be used for table schema and query by which
// it was created
func Describe(ctx context.Context, stream string) (dto.RelationDescription, error) {
	query := util.MustNoError(ksql.Describe(ksql.STREAM, stream).Expression)

	pipeline, err := network.Net.Perform(
		ctx,
		http.MethodPost,
		query,
		&network.ShortPolling{},
	)
	if err != nil {
		err = fmt.Errorf("cannot perform request: %w", err)
		return dto.RelationDescription{}, err
	}

	select {
	case <-ctx.Done():
		return dto.RelationDescription{}, ctx.Err()
	case val, ok := <-pipeline:
		if !ok {
			return dto.RelationDescription{}, static.ErrMalformedResponse
		}

		var (
			describe []dao.DescribeResponse
		)

		if err = jsoniter.Unmarshal(val, &describe); err != nil {
			err = errors.Join(static.ErrUnserializableResponse, err)
			return dto.RelationDescription{}, err
		}

		if len(describe) == 0 {
			return dto.RelationDescription{}, errors.New("stream not found")
		}

		return describe[0].DTO(), nil
	}
}

// Drop - drops stream from ksqlDB instance
// with parent topic. Also deletes projection from list
func Drop(ctx context.Context, stream string) error {

	query := util.MustNoError(ksql.Drop(ksql.STREAM, stream).Expression)

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
			return static.ErrMalformedResponse
		}

		var (
			drop []dao.DropInfo
		)

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
// current command return difference between
// struct tags and remote schema
func GetStream[S any](
	ctx context.Context,
	stream string) (*Stream[S], error) {

	var (
		s S
	)

	scheme, err := schema.NativeStructRepresentation(s)
	if err != nil {
		return nil, err
	}

	streamInstance := &Stream[S]{
		Name:         stream,
		remoteSchema: scheme,
	}
	desc, err := Describe(ctx, stream)
	if err != nil {
		if errors.Is(err, static.ErrStreamDoesNotExist) || len(desc.Fields) == 0 {
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

	remoteSchema := schema.RemoteFieldsRepresentation(stream, responseSchema)
	if err = remoteSchema.CompareWithFields(scheme.Array()); err != nil {
		return nil, fmt.Errorf("reflection check failed: %w", err)
	}

	return streamInstance, nil
}

// CreateStream - creates stream in ksqlDB instance
// after creating, user should call
// select or select with emit to get data from it
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

	rmSchema, err := schema.NativeStructRepresentation(s)
	if err != nil {
		return nil, err
	}

	if len(rmSchema.Map()) == 0 {
		return nil, fmt.Errorf("cannot create stream with empty schema")
	}

	metadata := ksql.Metadata{
		Topic:       settings.SourceTopic,
		Partitions:  int(settings.Partitions),
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
			return nil, static.ErrMalformedResponse
		}

		var (
			create []dao.CreateRelationResponse
		)

		slog.Debug(
			"received from create stream",
			slog.String("value", string(val)),
		)

		if err := jsoniter.Unmarshal(val, &create); err != nil {
			return nil, fmt.Errorf("cannot unmarshal create response: %w", err)
		}

		if len(create) < 1 {
			return nil, fmt.Errorf("unsuccessful response")
		}

		status := create[0]

		if status.CommandStatus.Status != consts.SUCCESS {
			return nil, fmt.Errorf("unsuccesful respose. msg: %s", status.CommandStatus.Message)
		}

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
// after creating, user should call
// select or select with emit to get data from it
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
			create []dao.CreateRelationResponse
		)

		if err := jsoniter.Unmarshal(val, &create); err != nil {
			return nil, fmt.Errorf("cannot unmarshal create response: %w", err)
		}

		if len(create) < 1 {
			return nil, fmt.Errorf("unsuccessful response")
		}

		status := create[0]

		if status.CommandStatus.Status != consts.SUCCESS {
			return nil, fmt.Errorf("unsuccesful respose. msg: %s", status.CommandStatus.Message)
		}

		return &Stream[S]{
			partitions:   settings.Partitions,
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
			return static.ErrMalformedResponse
		}

		var (
			insert []dao.CreateRelationResponse
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

			val := util.Serialize(value)
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
			return static.ErrMalformedResponse
		}

		var (
			insert []dao.CreateRelationResponse
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

// InsertAsSelect - provides insertion to stream functionality
// written fields are pre-fetched from select query, that
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
			insert dao.CreateRelationResponse
		)

		if err = jsoniter.Unmarshal(val, &insert); err != nil {
			return fmt.Errorf("cannot unmarshal insert response: %w", err)
		}

		return errors.New("unpredictable error occurred while inserting")
	}
}

// SelectOnce - performs select query
// and return only one http answer
// channel is closed almost immediately
func (s *Stream[S]) SelectOnce(
	ctx context.Context) (S, error) {

	var (
		value S
	)

	query, err := ksql.
		SelectAsStruct(s.Name, s.remoteSchema).
		From(s.Name, ksql.STREAM).
		Expression()

	if err != nil {
		return value, fmt.Errorf("build select query: %w", err)
	}

	pipeline, err := network.Net.PerformSelect(
		ctx,
		http.MethodPost,
		query,
		&network.LongPolling{},
	)
	if err != nil {
		return value, fmt.Errorf("cannot perform request: %w", err)
	}

	var (
		iter    = 0
		headers dao.Header
	)

	for {
		select {
		case <-ctx.Done():
			return value, ctx.Err()
		case val, ok := <-pipeline:
			if !ok {
				return value, static.ErrMalformedResponse
			}

			if strings.Contains(string(val), "Query Completed") {
				return value, nil
			}

			if iter == 0 {
				str := val[1 : len(val)-1]

				if err = jsoniter.Unmarshal(str, &headers); err != nil {
					return value, fmt.Errorf("cannot unmarshal headers: %w", err)
				}

				iter++
				continue
			}

			var (
				row dao.Row
			)

			if err = jsoniter.Unmarshal(val[:len(val)-1], &row); err != nil {
				return value, fmt.Errorf("cannot unmarshal row: %w", err)
			}

			value, err = netparse.ParseNetResponse[S](headers, row)
			if err != nil {
				slog.Error(
					"parse net response",
					slog.String("error", err.Error()),
					slog.Any("headers", headers),
					slog.Any("row", row),
				)
				return value, err
			}
			return value, nil

		}
	}
}

// SelectWithEmit - performs
// select with emit request
// answer is received for every new record
// and propagated to channel
func (s *Stream[S]) SelectWithEmit(
	ctx context.Context) (<-chan S, error) {

	var (
		value   S
		valuesC = make(chan S)
	)

	query, err := ksql.SelectAsStruct(s.Name, value).
		From(s.Name, ksql.STREAM).
		EmitChanges().
		Expression()

	if err != nil {
		return nil, fmt.Errorf("build select query: %w", err)
	}

	pipeline, err := network.Net.PerformSelect(
		ctx,
		http.MethodPost,
		query,
		&network.LongPolling{},
	)
	if err != nil {
		return nil, fmt.Errorf("cannot perform request: %w", err)
	}

	go func() {

		var (
			iter    = 0
			headers dao.Header
		)

		for {
			select {
			case <-ctx.Done():
				close(valuesC)
				return
			case val, ok := <-pipeline:
				if !ok {
					close(valuesC)
					return
				}

				if iter == 0 {
					str := val[1 : len(val)-1]

					if err = jsoniter.Unmarshal(str, &headers); err != nil {
						slog.Error(
							"select with emit read loop",
							slog.String("error", err.Error()),
							slog.String("headers", string(str)),
						)
						return
					}

					iter++
					continue
				}

				var (
					row dao.Row
				)

				if err = jsoniter.Unmarshal(val[:len(val)-1], &row); err != nil {
					slog.Error("select with emit read loop",
						slog.String("error", err.Error()),
						slog.String("val", string(val[:len(val)-1])),
					)
					continue
				}

				if len(row.FinalMessage) != 0 {
					if row.FinalMessage == "Query Completed" {
						continue
					}
				}
				value, err = netparse.ParseNetResponse[S](headers, row)
				if err != nil {
					slog.Error(
						"parse net response",
						slog.String("error", err.Error()),
						slog.Any("headers", headers),
						slog.Any("row", row),
					)
					return
				}

				valuesC <- value
			}
		}
	}()

	return valuesC, nil
}
