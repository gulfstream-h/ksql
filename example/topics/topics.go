package main

import (
	"context"
	"github.com/gulfstream-h/ksql/config"
	"github.com/gulfstream-h/ksql/topics"
	"log/slog"
)

const (
	ksqlURL = "http://localhost:8088"
)

func main() {
	ctx := context.Background()
	cfg := config.New(ksqlURL, 15, false)

	if err := cfg.Configure(ctx); err != nil {
		slog.Error("cannot configure ksql", "error", err.Error())
	}

	topicList, err := topics.ListTopics(context.Background())
	if err != nil {
		slog.Error("cannot list topics", "error", err.Error())
		return
	}

	slog.Info("successfully executed", "topics", topicList)
}
