package main

import (
	"context"
	"ksql/config"
	"ksql/migrations"
	"log/slog"
)

const (
	ksqlURL       = "http://localhost:8088"
	migrationPath = "../../ksqlmig/"
)

func main() {
	migration := migrations.New(ksqlURL, migrationPath)
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
