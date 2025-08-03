package main

import (
	"context"
	"fmt"
	"github.com/gulfstream-h/ksql/config"
	"github.com/gulfstream-h/ksql/kinds"
	"github.com/gulfstream-h/ksql/ksql"
	"github.com/gulfstream-h/ksql/shared"
	"github.com/gulfstream-h/ksql/tables"
	"log/slog"
)

const (
	ksqlURL = "http://localhost:8088"
)

// Every of the functions below can be uncommented
// and executed independently (except for create)
// in testing and debug purposes

func main() {
	//ctx := context.Background()
	//List(ctx)
	//Create(ctx)
	//Describe(ctx)
	//Drop(ctx)
	//Select(ctx)
	//SelectWithEmit(ctx)
	//CreateAsSelect(ctx)
}

func init() {
	slog.SetLogLoggerLevel(slog.LevelDebug)
	cfg := config.New(ksqlURL, 15, false)
	if err := cfg.Configure(context.Background()); err != nil {
		slog.Error("cannot configure ksql", "error", err.Error())
	}
}

func List(ctx context.Context) {
	tableList, err := tables.ListTables(ctx)
	if err != nil {
		slog.Error("cannot list topics", "error", err.Error())
		return
	}

	slog.Info("successfully executed!", "tables", tableList)
}

type ExampleTable struct {
	ID   int    `ksql:"ID, primary"`
	Name string `ksql:"NAME"`
}

const (
	tableName = "PCE_TABLE"
)

func Create(ctx context.Context) {
	sourceTopic := "process-topics"
	partitions := 1 // if topic doesnt exists, partitions are required

	exampleTable, err := tables.CreateTable[ExampleTable](
		ctx, tableName, shared.TableSettings{
			Name:        tableName,
			SourceTopic: sourceTopic,
			Partitions:  partitions,
			ValueFormat: kinds.JSON,
		})

	if err != nil {
		slog.Error("cannot create table", "error", err.Error())
		return
	}

	slog.Info("table created!", "name", exampleTable.Name)
}

func Describe(ctx context.Context) {
	description, err := tables.Describe(ctx, tableName)
	if err != nil {
		slog.Error("cannot describe table", "error", err.Error())
		return
	}

	slog.Info("successfully executed", "description", description)
}

func Drop(ctx context.Context) {
	queryableTableName := fmt.Sprintf("QUERYABLE_%s", tableName)

	if err := tables.Drop(ctx, queryableTableName); err != nil {
		slog.Error("cannot drop queryable table", "error", err.Error())
		return
	}

	if err := tables.Drop(ctx, tableName); err != nil {
		slog.Error("cannot drop table", "error", err.Error())
		return
	}

	slog.Info("table dropped!", "name", tableName)
}

func Select(ctx context.Context) {
	exampleTable, err := tables.GetTable[ExampleTable](ctx, tableName)
	if err != nil {
		slog.Error("cannot get table", "error", err.Error())
		return
	}

	rows, err := exampleTable.SelectOnce(ctx)
	if err != nil {
		slog.Error("cannot select from table", "error", err.Error())
		return
	}

	slog.Info("successfully selected rows", "rows", rows)
}

func SelectWithEmit(ctx context.Context) {
	exampleTable, err := tables.GetTable[ExampleTable](ctx, tableName)
	if err != nil {
		slog.Error("cannot get table", "error", err.Error())
		return
	}

	notesStream, cancel, err := exampleTable.SelectWithEmit(ctx)
	if err != nil {
		slog.Error("error during emit", "error", err.Error())
		return
	}

	for note := range notesStream {
		slog.Info("received note", "note", note)
		cancel()
	}
}

func CreateAsSelect(ctx context.Context) {
	sql := ksql.Select(ksql.F("ID"), ksql.F("TOKEN")).From(ksql.Schema("EXAMPLETABLE", ksql.TABLE))
	sourceTopic := "examples-topics"
	_, err := tables.CreateTableAsSelect[ExampleTable](ctx, "dublicate", shared.TableSettings{
		SourceTopic: sourceTopic,
		ValueFormat: kinds.JSON,
	}, sql)
	if err != nil {
		slog.Error("cannot create table as select", "error", err.Error())
		return
	}

	slog.Info("table created!")
}
