# ksql

**ksql** is a lightweight library aimed at solving the absence of a native Kafka SQL library in Go.

It enables interaction with the [`ksqldb-server`](https://docs.confluent.io/ksqldb/) via HTTP APIs,  
offering a rapid and type-safe way to manage and query Kafka streams and tables.

---

## ✅ Supported Operations

### Metadata
- `ListTopics`
- `ListStreams`
- `ListTables`

### Descriptions
- `DescribeStream`
- `DescribeTable`

### Drop
- `DropStream`
- `DropTable`

### Create
- `CreateStream`
- `CreateStreamAsSelect`
- `CreateTable`
- `CreateTableAsSelect`

### Queries
- `Select`
- `Select` with `Emit Changes`

### Inserts *(only for streams)*
- `Insert`
- `InsertAs`

### Transformation
- `ToTopic`
- `ToTable`
- `ToStream`

---

## ⚙️ Code-Centric Stream/Table Management

The project helps manage streams and tables via in-code function calls,  
each corresponding to a specific relational entity supported by Confluent.

Complex queries can be constructed via a builder,  
delegating verbosity, formality, and syntax concerns to internal abstractions.

---

## 🧠 Schema Awareness & Internal Linting

Code-based query representation includes internal linting.  
Each stream and table is retrieved from the Kafka server and transformed  
from a textual query description into a robust code prototype —  
containing:

- Schema fields with their types
- Parent topic
- Value format
- And more...

This ensures:

- All requested fields exist in the relation schema
- Field types are valid for the given operation
- The operation is supported by KSQL

The library acts as middleware to prevent unnecessary waiting for unprocessable responses,  
catching such issues at **compile time or runtime**.

---

## 🧩 Struct Mapping

For convenient development, `ksql` supports using generic Go structs.

User-defined structures with `ksql:"tag"` are:

- **Compared to remote Kafka schemas**
- **Used as return types** for `Select` operations (with automatic unmarshaling)
- **Used in `Insert` statements** to reduce repetitive declarations of field names and values

```bash
go get github.com/golstream/ksql
```

## Running KSQL in Docker
```yaml
services:
  kafka:
    image: bitnami/kafka:3.6.1-debian-11-r4
    ports:
      - "9092:9092"
      - "9094:9094"
    environment:
      - KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS=1000000
      - KAFKA_CFG_NODE_ID=0
      - KAFKA_CFG_PROCESS_ROLES=controller,broker
      - KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:9094
      - KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092,EXTERNAL://localhost:9094
      - KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT
      - KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=0@kafka:9093
      - KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER
      - KAFKA_CFG_INTER_BROKER_LISTENER_NAME=PLAINTEXT
    volumes:
      - kafka_data:/bitnami2

  ksqldb-server:
    image: confluentinc/ksqldb-server:latest
    ports:
      - "8088:8088"
    environment:
      KSQL_CONFIG_DIR: "/etc/ksql"
      KSQL_BOOTSTRAP_SERVERS: "kafka:9092"
      KSQL_LISTENERS: "http://0.0.0.0:8088"
      KSQL_KSQL_SERVICE_ID: "ksql-cluster"
      KSQL_KSQL_STREAMS_AUTO_OFFSET_RESET: "earliest"
    depends_on:
      - kafka

  ksqldb-cli:
    image: confluentinc/ksqldb-cli:latest
    entrypoint: /bin/sh
    tty: true
    depends_on:
      - ksqldb-server
volumes:
  kafka_data:
    driver: local
```
