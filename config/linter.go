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
	_ReflectionMode   struct{}
	_NoReflectionMode struct{}
)

func (mode _ReflectionMode) InitLinter(context.Context) error {
	return nil
}

func (mode _NoReflectionMode) InitLinter(ctx context.Context) error {
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
			continue
		}

		var (
			responseSchema = make(map[string]string)
		)

		for _, field := range description.Fields {
			responseSchema[field.Name] = field.Kind
		}

		static.StreamsProjections.Store(stream.Name, shared.StreamSettings{
			Name:        stream.Name,
			SourceTopic: &stream.Topic,
			Schema:      schema.SerializeRemoteSchema(responseSchema),
			Format:      kinds.JSON,
		})
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
			continue
		}

		var (
			responseSchema = make(map[string]string)
		)

		for _, field := range description.Fields {
			responseSchema[field.Name] = field.Kind
		}

		static.StreamsProjections.Store(table.Name, shared.StreamSettings{
			Name:        table.Name,
			SourceTopic: &table.Topic,
			Schema:      schema.SerializeRemoteSchema(responseSchema),
			Format:      kinds.JSON,
		})
	}

	return nil
}

var (
	_ shared.Linter = new(_ReflectionMode)
	_ shared.Linter = new(_NoReflectionMode)
)
