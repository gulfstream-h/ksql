package main

import (
	"context"
	"github.com/gulfstream-h/ksql/config"
	"github.com/gulfstream-h/ksql/database"
	"github.com/gulfstream-h/ksql/kinds"
	"github.com/gulfstream-h/ksql/ksql"
	"github.com/gulfstream-h/ksql/shared"
	"github.com/gulfstream-h/ksql/tables"
	"log/slog"
)

const (
	table           = "experimantaltable"
	additionalTable = "additionaltable"
	ksqlURL         = "http://localhost:8088"
)

func main() {
	//if _, err := createExperimentalTable(context.Background()); err != nil {
	//	slog.Error("cannot create table", "error", err.Error())
	//	return
	//}
	//SelectData()
	//SelectAvg()
	//Join()
	//createAdditionalTable(context.Background())
	CTE()

}

func init() {
	slog.SetLogLoggerLevel(slog.LevelDebug)
	cfg := config.New(ksqlURL, 15, false)
	if err := cfg.Configure(context.Background()); err != nil {
		slog.Error("cannot configure ksql", "error", err.Error())
	}
}

type ExpTable struct {
	ID    int    `ksql:"ID, primary"`
	Label string `ksql:"LABEL"`
	INFO  string `ksql:"INFO"`
	SUBS  int    `ksql:"SUBS"`
}

type AdditionalTable struct {
	ID    int    `ksql:"ID, primary"`
	INFO  string `ksql:"INFO"`
	Likes int    `ksql:"LIKES"`
}

func createExperimentalTable(ctx context.Context) (*tables.Table[ExpTable], error) {
	topic := "experimental-topic"
	partitions := 1

	return tables.CreateTable[ExpTable](ctx, table, shared.TableSettings{
		SourceTopic: topic,
		Partitions:  partitions,
		ValueFormat: kinds.JSON,
	})
}

func createAdditionalTable(ctx context.Context) (*tables.Table[AdditionalTable], error) {
	topic := "additional-topic"
	partitions := 1

	return tables.CreateTable[AdditionalTable](ctx, additionalTable, shared.TableSettings{
		SourceTopic: topic,
		Partitions:  partitions,
		ValueFormat: kinds.JSON,
	})
}

func SelectData() {
	query, err := ksql.
		Select(ksql.F("ID"), ksql.F("LABEL"), ksql.F("INFO"), ksql.F("SUBS")).
		From(ksql.Schema("QUERYABLE_experimantaltable", ksql.TABLE)).
		Where(ksql.F("SUBS").Equal(10)).EmitChanges().
		Expression()
	if err != nil {
		slog.Error("err", "error", err.Error())
		return
	}

	slog.Info("query", "text", query)
	resp, err := database.Select[ExpTable](context.Background(), query)
	if err != nil {
		slog.Error("err", "error", err.Error())
		return
	}

	for v := range resp {
		slog.Info("msg", "struct", v)
	}
}

func SelectAvg() {
	query, err := ksql.
		Select(ksql.F("INFO"), ksql.Avg(ksql.F("SUBS"))).
		From(ksql.Schema("QUERYABLE_experimantaltable", ksql.TABLE)).
		GroupBy(ksql.F("INFO")).EmitChanges().
		Expression()
	if err != nil {
		slog.Error("err", "error", err.Error())
		return
	}

	slog.Info("query", "text", query)
	resp, err := database.Select[ExpTable](context.Background(), query)
	if err != nil {
		slog.Error("err", "error", err.Error())
		return
	}

	for v := range resp {
		slog.Info("msg", "struct", v)
	}
}

func Join() {
	query, err := ksql.
		Select(ksql.F("QUERYABLE_EXPERIMANTALTABLE.INFO"), ksql.F("LIKES")).
		From(ksql.Schema("QUERYABLE_experimantaltable", ksql.TABLE)).EmitChanges().
		Join(ksql.Schema("ADDITIONALTABLE", ksql.TABLE), ksql.F("QUERYABLE_experimantaltable.ID").Equal(ksql.F("ADDITIONALTABLE.ID"))).
		Expression()
	if err != nil {
		slog.Error("err", "error", err.Error())
		return
	}

	slog.Info("query", "text", query)
	resp, err := database.Select[ExpTable](context.Background(), query)
	if err != nil {
		slog.Error("err", "error", err.Error())
		return
	}

	for v := range resp {
		slog.Info("msg", "struct", v)
	}
}

func CTE() {
	sb := ksql.Select(ksql.F("INFO"), ksql.F("LIKES")).From(ksql.Schema("QUERYABLE_experimentaltable", ksql.TABLE)).As("userinfo")

	query, err := ksql.
		Select(ksql.F("INFO"), ksql.F("userinfo.LIKES")).
		From(ksql.Schema("QUERYABLE_experimantaltable", ksql.TABLE)).WithCTE(sb).
		Expression()
	if err != nil {
		slog.Error("err", "error", err.Error())
		return
	}

	slog.Info("query", "text", query)
	resp, err := database.Select[ExpTable](context.Background(), query)
	if err != nil {
		slog.Error("err", "error", err.Error())
		return
	}

	for v := range resp {
		slog.Info("msg", "struct", v)
	}
}
