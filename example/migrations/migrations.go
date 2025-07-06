package main

import (
	"context"
	"github.com/gulfstream-h/ksql/config"
	"github.com/gulfstream-h/ksql/migrations"
	"log/slog"
)

const (
	ksqlURL       = "http://localhost:8088"
	migrationPath = "./migrations"
)

func main() {
	path, err := migrations.GenPath()(migrationPath)
	if err != nil {
		slog.Error("cannot get migration path", "error", err.Error())
		return
	}

	migration := migrations.New(ksqlURL, path)
	if err := migration.AutoMigrate(context.Background()); err != nil {
		slog.Error("cannot automigrate", "error", err.Error())
		return
	}
}
func init() {
	cfg := config.New(ksqlURL, 15, false)
	if err := cfg.Configure(context.Background()); err != nil {
		slog.Error("cannot configure ksql", "error", err.Error())
	}
}
