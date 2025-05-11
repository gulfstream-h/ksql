package dto

type Topic struct {
	Name     string `json:"name"`
	Replicas []int  `json:"replicaInfo"`
}

// curl -X POST \\n  -H "Content-Type: application/vnd.ksql.v1+json" \\n  -d '{"ksql": "SHOW TOPICS;"}' \\n  "http://localhost:8088/ksql"
type ShowTopics struct {
	KafkaType string  `json:"type"`
	Command   string  `json:"statementText"`
	Topics    []Topic `json:"topics"`
	Warnings  []any   `json:"warnings"`
}

type Stream struct {
	KafkaType   string `json:"type"`
	Name        string `json:"name"`
	Topic       string `json:"topic"`
	KeyFormat   string `json:"keyFormat"`
	ValueFormat string `json:"valueFormat"`
	IsWindowed  bool   `json:"isWindowed"`
}

// curl -X POST \\n  -H "Content-Type: application/vnd.ksql.v1+json" \\n  -d '{"ksql": "SHOW STREAMS;"}' \\n  "http://localhost:8088/ksql"
type ShowStreams struct {
	KafkaType string   `json:"type"`
	Command   string   `json:"statementText"`
	Streams   []string `json:"streams"`
	Warnings  []any    `json:"warnings"`
}

type Table struct {
	Type        string `json:"type"`
	Name        string `json:"name"`
	Topic       string `json:"topic"`
	KeyFormat   string `json:"keyFormat"`
	ValueFormat string `json:"valueFormat"`
	IsWindowed  bool   `json:"isWindowed"`
}

// curl -X POST \\n  -H "Content-Type: application/vnd.ksql.v1+json" \\n  -d '{"ksql": "SHOW TABLES;"}' \\n  "http://localhost:8088/ksql"
type ShowTables struct {
	Type          string  `json:"@type"`
	StatementText string  `json:"statementText"`
	Tables        []Table `json:"tables"`
	Warnings      []any   `json:"warnings"`
}
