# ksql
KSQL is an open-source Go package for interacting with the Confluent KSQL service, enabling stream processing of data from Kafka topics using SQL-like queries. 
This library was created by technology researchers due to the absence of an official Go client.

Beyond a simple client, it includes a migration tool, a KSQL query builder, and an ORM-like interface for working seamlessly with KSQL-DB.
A key feature of the library is schema reflection — it analyzes the structure of every query you build to ensure correctness and consistency

## Installation 
```go
go get github.com/gulfstream-h/ksql
```

## Examples 
All examples of library usage are located in [ksql-examples](https://github.com/gulfstream-h/ksql-examples) repository.

## Configuration 
To initialize a library instance, you need to provide the URL of the KSQL server,
the timeout for requests, and whether to enable reflection for automatic schema generation.
```go
url := "your_ksql_server_url"
timeoutInSeconds := 15
withReflection := false


cfg := config.New(url, int64(timeoutInSeconds), withReflection)
if err := cfg.Configure(ctx); err != nil {
   return
}

```


## Capabilities:
### Operating Modes:

#### Raw-Query Mode
Allows sending raw SQL queries as strings to KSQL-DB.
The user-provided query is serialized and sent via HTTP to the server specified in the configuration. 
This mode gives the library client full control over query construction, error handling, and response deserialization. 
To prevent syntax errors and improve usability, this mode supports a query builder feature.

For push queries, you should use:
```go
query := "DESCRIBE BALANCE_STREAM;"


response, err := database.Execute(context.TODO(), query)
if err != nil{
   return
}


slog.Info("ksql response", "description", response)
```
For pull queries:
```go
type Transaction struct {
   ID int64 `ksql:"ID"`
   Amount float64 `ksql:"AMOUNT"`
   ClientHash string `ksql:"CLIENT_HASH"`
   IsFrozen bool `ksql:"IS_FROZEN"`
}


query := "SELECT ID, AMOUNT, CLIENT_HASH, IS_FROZEN  FROM ANTIFRAUD_STREAM"


transactionQueque, err := database.Select[Transaction](context.TODO(), query)
if err != nil{
   return
}


lastTransaction := <- transactionQueque


slog.Info("ksql response", "description", lastTransaction)
```

#### Struct ORM Mode
Adds an additional abstraction layer between the developer and the database. Queries for topics, streams, and tables are constructed by calling corresponding functions from the library's packages.

**List** – a method for listing all existing topics/streams/tables along with their metadata.


```go
topicList, err := topics.ListTopics(context.Background())
if err != nil {
   slog.Error("cannot list topics", "error", err.Error())
   return
}


slog.Info("successfully executed", "topics", topicList)


streamsList, err := streams.ListStreams(ctx)
if err != nil {
   slog.Error("cannot list streams", "error", err.Error())
   return
}


slog.Info("successfully executed!", "streams", streamsList)


tableList, err := tables.ListTables(ctx)
if err != nil {
   slog.Error("cannot list topics", "error", err.Error())
   return
}


slog.Info("successfully executed!", "tables", tableList)
```

**Drop** – a method for deleting topics/streams/tables from the KSQL server.
```go
if err := streams.Drop(ctx, streamName); err != nil {
   slog.Error("cannot drop stream", "error", err.Error())
   return
}


slog.Info("stream dropped!", "name", streamName)

if err := tables.Drop(ctx, tableName); err != nil {
   slog.Error("cannot drop table", "error", err.Error())
   return
}


slog.Info("table dropped!", "name", tableName)
```

**Describe** – a method for retrieving metadata about topics/streams/tables.
```go
description, err := streams.Describe(ctx, streamName)
if err != nil {
   slog.Error("cannot describe stream", "error", err.Error())
   return
}


slog.Info("successfully executed", "description", description)


description, err := tables.Describe(ctx, tableName)
if err != nil {
   slog.Error("cannot describe table", "error", err.Error())
   return
}


slog.Info("successfully executed", "description", description)
```


**CreateAsSelect** – a method for creating a relation from a select query. 
Method requires the use of the query builder feature. 
The projection created by this method is fully compatible with all methods described in this documentation.

```go
sql := ksql.Select(ksql.F("ID"), ksql.F("TOKEN")).From(ksql.Schema(streamName, ksql.STREAM))
sourceTopic := "examples-topics"
_, err := streams.CreateStreamAsSelect[ExampleStream](ctx, "dublicateStream", shared.StreamSettings{
   SourceTopic: sourceTopic,
   Format:      kinds.JSON,
}, sql)
if err != nil {
   slog.Error("cannot create stream as select", "error", err.Error())
   return
}


slog.Info("stream created!")


sql := ksql.Select(ksql.F("ID"), ksql.F("TOKEN")).From(ksql.Schema("EXAMPLETABLE", ksql.TABLE))
sourceTopic := "examples-topics"
_, err := tables.CreateTableAsSelect[ExampleTable](ctx, "dublicate", shared.TableSettings{
   SourceTopic: sourceTopic,
   Format:      kinds.JSON,
}, sql)
if err != nil {
   slog.Error("cannot create table as select", "error", err.Error())
   return
}


slog.Info("table created!")
```


For more complex queries **ksql** library provides a convenient ORM format description using Go structures with the `ksql` tag.

```go
type ExampleStream struct {
   ID    int    `ksql:"ID"`
   Token []byte `ksql:"TOKEN"`
}


type ExampleTable struct {
   ID   int    `ksql:"ID, primary"`
   Name string `ksql:"NAME"`
}
```

This structure is passed to the methods listed below as a generic and is parsed using the `reflect` package to extract data types, field names, and additional tags for use in the following queries:

**Create** – a generic method that creates a stream or table based on the fields defined in the provided structure.
```go
sourceTopic := "examples-topics"
partitions := 1 // if topic doesnt exists, partitions are required


exampleTable, err := streams.CreateStream[ExampleStream](
   ctx, streamName, shared.StreamSettings{
      SourceTopic: sourceTopic,
      Partitions:  partitions,
      Format:      kinds.JSON,
   })


if err != nil {
   slog.Error("cannot create stream", "error", err.Error())
   return
}


slog.Info("stream created!", "name", exampleTable.Name)

sourceTopic := "process-topics"
partitions := 1 // if topic doesnt exists, partitions are required


exampleTable, err := tables.CreateTable[ExampleTable](
   ctx, tableName, shared.TableSettings{
      SourceTopic: sourceTopic,
      Partitions:  partitions,
      Format:      kinds.JSON,
   })


if err != nil {
   slog.Error("cannot create table", "error", err.Error())
   return
}


slog.Info("table created!", "name", exampleTable.Name)
```


**Get** - is a generic method, checking the user-provided declarative structure for compliance with the naming and field types in KSQL-DB.
With full compliance, it returns an instance of `Stream[Generic]`/`Table[Generic]` with receiver methods `Select` and `SelectWithEmit`.
```go

exampleStream, err := streams.GetStream[ExampleStream](ctx, streamName)
if err != nil {
   slog.Error("cannot get stream", "error", err.Error())
   return
}


exampleTable, err := tables.GetTable[ExampleTable](ctx, tableName)
if err != nil {
   slog.Error("cannot get table", "error", err.Error())
   return
}
```

**Select** is a method for getting the last data record from a table/stream. In case of a successful operation, 
the fields returned from ksql-db are parsed into the corresponding fields of the structure based on tags and types. 
Ultimately, the user receives an instance of the structure with values completely cleared of metadata.

```go
exampleStream, err := streams.GetStream[ExampleStream](ctx, streamName)
if err != nil {
   slog.Error("cannot get stream", "error", err.Error())
   return
}


rows, err := exampleStream.SelectOnce(ctx)
if err != nil {
   slog.Error("cannot select from stream", "error", err.Error())
   return
}


slog.Info("successfully selected rows", "rows", rows)

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
```



**Select With Emit** is a method that starts listening to a relational relation in real-time until stopped by the user or an unexpected error occurs. 
As a Select it deserializes the received fields in the response into the user's custom structure. 
It returns a channel that receives instances of the generic type cleared of metadata.

```go
exampleStream, err := streams.GetStream[ExampleStream](ctx, streamName)
if err != nil {
   slog.Error("cannot get stream", "error", err.Error())
   return
}


notesStream, cancel, err := exampleStream.SelectWithEmit(ctx)
if err != nil {
   slog.Error("error during emit", "error", err.Error())
   return
}


for note := range notesStream {
   slog.Info("received note", "note", note)
   cancel()
}

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
```

**Insert** is a method for inserting data into a stream. It is not worked with tables

```go
exampleStream, err := streams.GetStream[ExampleStream](ctx, streamName)
if err != nil {
   slog.Error("cannot get stream", "error", err.Error())
   return
}


data := []byte("SECRET_BASE64_DATA")
token := []byte(base64.StdEncoding.EncodeToString(data))


if err = exampleStream.InsertRow(ctx, ksql.Row{
   "ID":    1,
   "TOKEN": token,
}); err != nil {
   slog.Error("cannot insert data to stream", "error", err.Error())
   return
}


slog.Info("successfully inserted")
```


**InsertAsSelect** - adds records that are the result of a nested query written by the user. Query should be built by ksql builder feature
```go
sql := ksql.Select(ksql.F("ID"), ksql.F("TOKEN")).From(ksql.Schema(streamName, ksql.STREAM))


stream, err := streams.GetStream[ExampleStream](ctx, "EXAMPLESTREAM")
if err != nil {
   slog.Error("cannot get stream", "error", err.Error())
   return
}


err = stream.InsertAsSelect(ctx, sql)
if err != nil {
   slog.Error("cannot insert as select to stream", "error", err.Error())
   return
}


slog.Info("inserted as select")
```

## Features 
### KSQL Query Builder
A query builder inspired by `goqu.Builder`. 
It uses a flexible expression-building format that ensures correct query semantics. 
None of the builder's methods have a predefined call order and can be invoked according to the business logic of a given code segment.

#### Builder methods
**SELECT** 
```go
// Select fields as enumeration as items in ksql query.
selectBuilderWithFields := ksql.
   Select(
      ksql.F("schema1.col1"),
      ksql.F("schema1.col2"),
      ksql.F("schema1.col3"),
   ).
   From(ksql.Schema("schema1", ksql.STREAM))


// Select fields as event of current ksql query.
type SchemaEvent struct {
   Col1 string `ksql:"col1"`
   Col2 string `ksql:"col2"`
   Col3 string `ksql:"col3"`
}
selectBuilderStructed := ksql.
   SelectAsStruct("schema1", SchemaEvent{}).
   From(ksql.Schema("schema1", ksql.STREAM))
```

**INSERT**
```go
// Insert data into ksql table.
event := SchemaEvent{
   Col1: "value1",
   Col2: "value2",
   Col3: "value3",
}


// Insert data as a dictionary.
insertDictBuilder := ksql.Insert(ksql.STREAM, "schema1").
   Rows(
      ksql.Row{
         "col1": "value1",
         "col2": "value2",
         "col3": "value3",
      },
   )


// Insert data as a struct.
insertEventBuilder := ksql.Insert(ksql.STREAM, "schema1").
   InsertStruct(event)


// Insert data as a result of select statement from other schema


innerQuery := ksql.Select(
   ksql.F("col1"),
   ksql.F("col2"),
   ksql.F("col3"),
).
   From(ksql.Schema("another schema", ksql.STREAM))


insertAsSelectBuilder := ksql.Insert(ksql.STREAM, "schema1").
   AsSelect(innerQuery)
```
**CREATE**
```go
// Create queries


innerQuery := ksql.Select(
   ksql.F("col1"),
   ksql.F("col2"),
   ksql.F("col3"),
).
   From(ksql.Schema("another_schema", ksql.STREAM)).
   Where(ksql.F("col1").Equal("value1"))


// Create a new ksql stream from another stream.
createStreamBuilder := ksql.Create(ksql.STREAM, "schema1").
   AsSelect(innerQuery)


// Create a new stream from a provided struct as schema representation.
createStreamFromStructBuilder := ksql.Create(ksql.STREAM, "schema1").
   SchemaFromStruct(SchemaEvent{})
```

**DROP**

```go
// Drop queries


// Drop a ksql stream
dropStreamBuilder := ksql.Drop(ksql.STREAM, "schema1")


// Drop a ksql table
dropTableBuilder := ksql.Drop(ksql.TABLE, "schema1")
```

**LIST**
```go
// List queries


// list all ksql streams
listStreamsBuilder := ksql.List(ksql.STREAM)


// list all ksql tables
listTablesBuilder := ksql.List(ksql.TABLE)


// list all ksql topics
listTopicsBuilder := ksql.List(ksql.TOPIC)
```

**DESCRIBE**

```go
// Describe queries


// Describe a ksql stream
describeStreamBuilder := ksql.Describe(ksql.STREAM, "schema1")


// Describe a ksql table
describeTableBuilder := ksql.Describe(ksql.TABLE, "schema1")
```

#### Builder operators:
**WHERE**
```go
// Single where condition
queryBuilder := ksql.Select(ksql.F("col1")).From(ksql.Schema("schema1", ksql.STREAM)).
   Where(ksql.F("col1").Equal("value1"))


// Multiple where conditions


// Using AND operator
queryBuilderAnd := ksql.Select(ksql.F("col1")).From(ksql.Schema("schema1", ksql.STREAM)).
   Where(
      ksql.F("col1").Equal("value1"),
      ksql.F("col2").Equal("value2"),
   )


// Using OR operator
queryBuilderOr := ksql.Select(ksql.F("col1")).From(ksql.Schema("schema1", ksql.STREAM)).
   Where(
      ksql.Or(
         ksql.F("col1").Equal("value1"),
         ksql.F("col3").Equal("value3"),
      ),
   )
```

**JOIN**
```go
// JOIN
queryBuilderJoin := ksql.Select(
   ksql.F("a.col1"),
   ksql.F("b.col2"),
).From(
   ksql.Schema("a", ksql.STREAM),
).Join(
   ksql.Schema("b", ksql.STREAM),
   ksql.F("a.col1").Equal("b.col1"),
)


// JOIN with multiple conditions
queryBuilderJoinMultiple := ksql.Select(
   ksql.F("a.col1"),
   ksql.F("b.col2"),
).From(
   ksql.Schema("a", ksql.STREAM),
).Join(
   ksql.Schema("b", ksql.STREAM),
   ksql.And(
      ksql.F("a.col1").Equal("b.col1"),
      ksql.F("a.col2").Equal("b.col2"),
   ),
)


// LeftJoin/RightJoin
queryBuilderLeftJoin := ksql.Select(
   ksql.F("a.col1"),
   ksql.F("b.col2"),
).From(
   ksql.Schema("a", ksql.STREAM),
).LeftJoin(
   ksql.Schema("b", ksql.STREAM),
   ksql.F("a.col1").Equal("b.col1"),
)

```

**WINDOWED**
Supports all types of windowed functions:
- Tumbling
- Session
- Hopping

```go
// Windowed functions
queryBuilderWindowed := ksql.Select(
   ksql.F("col1"),
   ksql.F("col2"),
).From(
   ksql.Schema("schema1", ksql.STREAM),
).Windowed(
   ksql.NewHoppingWindow(
      ksql.TimeUnit{
         Val:  1,
         Unit: ksql.Seconds,
      },
      ksql.TimeUnit{
         Val:  5,
         Unit: ksql.Seconds,
      },
   ),
).
   GroupBy(ksql.F("col1"))
```

**HAVING, GROUP BY, ORDER BY**
```go
// Having, GroupBy, OrderBy
queryBuilderHaving := ksql.Select(
   ksql.F("col1"),
   ksql.F("col2"),
).From(
   ksql.Schema("schema1", ksql.STREAM),
).GroupBy(
   ksql.F("col1"),
).Having(
   ksql.F("col2").Greater(100),
).OrderBy(
   ksql.F("col1").Asc(),
   ksql.F("col2").Desc(),
)
```

**WITH EMIT**
```go
// With Emit
queryBuilderWithEmit := ksql.Select(
   ksql.F("col1"),
   ksql.F("col2"),
).From(
   ksql.Schema("schema1", ksql.STREAM),
  
   // Or Emit final
).EmitChanges()
```


**Aliases**
With special method `As()`, query entities can get an alias, if allowed by ksql semantics rules.
**CTEs**
KSQL currently supports only one common table expression (CTE) per query. Developers should be aware of this limitation and validate such queries accordingly, as the library itself does not restrict this capability due to the rapid evolution of KSQL technology.
```go
// CTE
queryBuilderCTE := ksql.
    Select(ksql.F("col1"), ksql.F("col2")).
    From(ksql.Schema("schema1", ksql.STREAM)).
    WithCTE(
        ksql.Select(ksql.F("col1"), ksql.F("col2")).
        From(ksql.Schema("schema2", ksql.STREAM)).As("cte1"),
    )
```


#### Query Building Rules
`KSQL DB` has its own specific set of rules that restrict users in their actions related to relational relations.
Builder also knows about these rules, proactively returning an error when trying to build a query that violates the regulations.
**Rules list:**
- `GROUP BY` requires `WINDOWED` clause on stream 
- No `HAVING` without `GROUP BY` 
- Aggregated functions should be used with `GROUP BY` clauses 
- `WINDOWED` expression are not allowed on table references 
- `EMIT FINAL` can be used only on tables 
- `EMIT FINAL` and `EMIT CHANGES` cannot be used together 
- Cannot create stream from table 
- Cannot create table from non-aggregated stream 
- Cannot crete a table from query with `WINDOWED` operator

## Reflection

The library (when operating in ORM Mode and using the KSQL builder) is capable of analyzing queries and schemas retrieved from KSQL-DB for structural consistency.

Reflection checks the following:

* Existence of the requested stream/table in the database
* Presence of the requested field in the relational entity
* Type consistency between the query and the database schema

Alias usage is supported within this feature.

**Metadata Handling:**

* Upon initialization, KSQL gathers information about all relations registered in KSQL-DB.
* The library stores all created/deleted deserialized structures using its internal tooling in an in-memory cache.
* **\[In Progress]**: It is being developed to also collect data based on migrations created via the library.

Reflection is an optional feature and can be enabled or disabled through the configuration.

## Migrations 

Migrations are used to separate the database architecture from the business logic of the application.
For full functionality, it is necessary to install the CLI tool for Linux/macOS:

```go
go install github.com/gulfstream-h/ksql
// TODO: should we drop installation (we've already has an installation topic)
```

To start work with migration tool, it needs to create sql migration in your file system with `ksql` command:
```bash
ksql create transaction
```
After calling the command file with name {{UNIX_TIMESTAMP}}_transaction.sql will be created in the current directory.
File contains:
```sql
-- +seeker Upsq
--write your up-migration here--
-- +seeker Down
--write your down-migration here--
```

Now we replace string `--write your up-migration here–` to our migration
```sql
CREATE
STREAM balance_operations (operation_id STRING,user_id STRING,operation_type STRING,amount DOUBLE,currency STRING)
WITH (kafka_topic='balance_operations',value_format='JSON');
```

And we can write down migration by replacing string `--write your down-migration here–` to :
```sql
DROP STREAM balance_operations;
```

To apply the migration, we need to run one of this commands:

for up migration:
```bash
ksql up 1751478066_transactions.sql --db_url=http://localhost:8088
```


for down migrations:
```bash
ksql down 1751478066_transactions.sql --db_url=http://localhost:8088
```

The migration will be applied to the remote database server, and the timestamp of the last applied migration will be recorded in a system stream. 
Subsequent `up` migrations can only be applied if they have a newer timestamp, while `down` is only allowed for the most recently applied migration.

There is also a helper function called `automigrate`, which skips already applied migrations and applies only new ones when the service starts.

```go

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

```


You can specify the database URL in the CLI either using the `--db_url` flag or by setting the `db_url` field in a `.env` file.
Although the library allows creating tables through the package's code, we recommend using the migration feature for version control and easier transfer of table definitions across different infrastructures.


## Roadmap:
To See features in progress or planned in future releases, see our issues [board](https://github.com/gulfstream-h/ksql/issues?q=is%3Aissue%20state%3Aopen%20%20type%3AFeature%20has%3Amilestone)
Al features has special type `Feature` and orchestrated by `milestone` label.

## Contributing
See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on contributing to the project.




