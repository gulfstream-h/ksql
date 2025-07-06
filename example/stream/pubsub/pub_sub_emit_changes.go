package main

import (
	"context"
	"fmt"
	"github.com/gulfstream-h/ksql/config"
	"github.com/gulfstream-h/ksql/ksql"
	"github.com/gulfstream-h/ksql/shared"
	"github.com/gulfstream-h/ksql/streams"
	"log"
	"log/slog"
	"math/rand"
	"time"
)

type (
	StreamEvent struct {
		ID          string            `ksql:"id"`
		ExternalID  string            `ksql:"external_id"`
		Description string            `ksql:"description"`
		Event       map[string]string `ksql:"event"`
	}
)

func Init(ctx context.Context) error {
	const (
		host = `http://localhost:8088`
	)

	slog.SetLogLoggerLevel(slog.LevelDebug)

	err := config.
		New(host, 600, true).
		Configure(ctx)
	if err != nil {
		return fmt.Errorf("init config: %w", err)
	}

	return nil
}

func List(ctx context.Context) {

	streamsList, err := streams.ListStreams(ctx)
	if err != nil {
		slog.Error("cannot list streams", "error", err.Error())
		return
	}

	for _, s := range streamsList.Streams {
		if s.Name == "EXAMPLE_STREAM" {
			err = streams.Drop(ctx, s.Name)
			if err != nil {
				slog.Error(
					"drop stream",
					slog.String("stream_name", s.Name),
				)
				return
			}

			slog.Info(
				"successfully dropped",
				slog.String("stream_name", s.Name),
			)
		}
	}

	slog.Info("successfully executed!", "streams", streamsList)
}

func StreamFromTopic(
	ctx context.Context,
	streamName string,
	sourceName string,
) error {
	stream, err := streams.CreateStream[StreamEvent](
		ctx,
		streamName,
		shared.StreamSettings{
			Name:        streamName,
			SourceTopic: sourceName,
			Partitions:  1,
		},
	)

	if err != nil {
		return fmt.Errorf("create stream: %w", err)
	}

	readChan, _, err := stream.SelectWithEmit(ctx)
	if err != nil {
		return fmt.Errorf("select with emit: %w", err)
	}

	go listenLoop(ctx, readChan)
	go produceStructLoop(ctx, stream)
	go produceRowLoop(ctx, stream)

	<-ctx.Done()

	err = streams.Drop(ctx, streamName)
	if err != nil {
		return fmt.Errorf("drop stream: %w", err)
	}

	return nil
}

func produceStructLoop(
	ctx context.Context,
	stream *streams.Stream[StreamEvent],
) {
	counter := 0
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-ctx.Done():
			slog.Info("context is done... exiting")
			return
		case <-ticker.C:
			event := StreamEvent{
				ID:          fmt.Sprintf("event_id_%d", counter),
				ExternalID:  fmt.Sprintf("external_id_%d", rand.Int31()),
				Description: "description",
				Event:       map[string]string{"name": "john"},
			}

			err := stream.Insert(ctx, event)
			if err != nil {
				slog.Error(
					"insert",
					slog.String("error", err.Error()),
					slog.Any("event", event),
				)
				continue
			}

			slog.Info("struct inserted")
		}
	}
}

func produceRowLoop(
	ctx context.Context,
	stream *streams.Stream[StreamEvent],
) {
	counter := 10_000
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-ctx.Done():
			slog.Info("context is done... exiting")
			return
		case <-ticker.C:

			event := ksql.Row{
				"id":          fmt.Sprintf("event_id_%d", counter),
				"external_id": fmt.Sprintf("external_id_%d", rand.Int31()),
				"description": "some desc",
				"event": map[string]string{
					"a": "a_field",
					"b": "b_field",
				},
			}

			err := stream.InsertRow(ctx, event)

			slog.Info("row inserted")

			if err != nil {
				slog.Error(
					"insert",
					slog.String("error", err.Error()),
					slog.Any("event", event),
				)
				continue
			}

			slog.Info("struct inserted")
		}
	}
}

func listenLoop(
	ctx context.Context,
	dataChan <-chan StreamEvent,
) {

	for {
		select {
		case <-ctx.Done():
			slog.Info("context is done... exiting")
			return
		case evt, ok := <-dataChan:
			if !ok {
				slog.Info("data chan closed... exiting")
				return
			}

			slog.Info(
				"received event",
				slog.String("ID", evt.ID),
				slog.String("ExternalID", evt.ExternalID),
				slog.String("Description", evt.Description),
				slog.Any("event", evt.Event),
			)

		}
	}
}

func main() {
	const (
		streamName  = `example_stream`
		sourceTopic = `example_topic`
	)

	ctx := context.Background()
	err := Init(ctx)
	if err != nil {
		log.Printf("init: %s\n", err.Error())
		return
	}

	List(ctx)

	ctx, cancel := context.WithTimeout(ctx, time.Second*120)
	defer cancel()

	err = StreamFromTopic(ctx, streamName, sourceTopic)
	if err != nil {
		log.Printf("stream from topic: %s\n", err.Error())
		return
	}

}
