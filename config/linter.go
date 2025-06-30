package config

import (
	"context"
	"fmt"
	"ksql/kinds"
	"ksql/schema"
	"ksql/shared"
	"ksql/static"
	"ksql/streams"
	"ksql/tables"
	"log/slog"
)

type (
	// _NoReflectionMode - applying no-cache mode of application.
	// There is no query fields check-ups
	_NoReflectionMode struct{}
	// _ReflectionMode - applying relation cache mode.
	// All existing relations are additionally parsed to internal formats
	// so query builder matches user-listed fields with cached storage.
	// if field is not presented in cache or user-provided type mismatch with
	// cashed value - reflection check will return an error before executing query
	_ReflectionMode struct{}
)

func (mode _NoReflectionMode) InitLinter(context.Context) error {
	return nil
}

// InitLinter - caches streams & tables. Eventually, it changes global reflection variable
func (mode _ReflectionMode) InitLinter(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	static.ReflectionFlag = true
	slog.Debug("reflection mode is enabled")

	streamList, err := streams.ListStreams(ctx)
	if err != nil {
		return fmt.Errorf("cannot list streams: %w", err)
	}

	slog.Debug("streams listed!", "list", streamList)

	for _, stream := range streamList.Streams {
		description, err := streams.Describe(ctx, stream.Name)
		if err != nil {
			slog.Error("cannot describe stream", "error", err.Error())
			return fmt.Errorf("cannot describe stream: %w", err)
		}

		var (
			responseSchema = make(map[string]string)
		)

		for _, field := range description.Fields {
			responseSchema[field.Name] = field.Kind
		}

		static.StreamsProjections.Set(stream.Name, shared.StreamSettings{
			SourceTopic: stream.Topic,
			Format:      kinds.JSON,
		}, schema.RemoteFieldsRepresentation(stream.Name, responseSchema))
	}

	tableList, err := tables.ListTables(ctx)
	if err != nil {
		return fmt.Errorf("cannot list tables: %w", err)
	}

	slog.Debug("tables listed!", "list", tableList)

	for _, table := range tableList.Tables {
		description, err := streams.Describe(ctx, table.Name)
		if err != nil {
			slog.Error("cannot describe table", "error", err.Error())
			return fmt.Errorf("cannot describe table: %w", err)
		}

		var (
			responseSchema = make(map[string]string)
		)

		for _, field := range description.Fields {
			responseSchema[field.Name] = field.Kind
		}

		static.StreamsProjections.Set(table.Name, shared.StreamSettings{
			SourceTopic: table.Topic,
			Format:      kinds.JSON,
		}, schema.RemoteFieldsRepresentation(table.Name, responseSchema))
	}

	return nil
}

var (
	_ shared.Linter = new(_NoReflectionMode)
	_ shared.Linter = new(_ReflectionMode)
)
