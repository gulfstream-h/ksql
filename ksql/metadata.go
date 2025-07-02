package ksql

import (
	"fmt"
	"strings"
)

// Metadata - represents metadata for a Kafka topic or stream
// for creation using the WITH clause in KSQL queries
type Metadata struct {
	Topic           string
	ValueFormat     string
	Partitions      int
	Replicas        int
	Timestamp       string
	TimestampFormat string
	KeyFormat       string
}

// Expression - generates a KSQL expression string for the metadata
func (m *Metadata) Expression() string {
	var (
		str   strings.Builder
		parts []string
	)

	if m.Topic != "" {
		parts = append(parts, fmt.Sprintf("KAFKA_TOPIC = '%s'", m.Topic))
	}
	if m.ValueFormat != "" {
		parts = append(parts, fmt.Sprintf("VALUE_FORMAT = '%s'", m.ValueFormat))
	}
	if m.KeyFormat != "" {
		parts = append(parts, fmt.Sprintf("KEY_FORMAT = '%s'", m.KeyFormat))
	}
	if m.Partitions != 0 {
		parts = append(parts, fmt.Sprintf("PARTITIONS = %d", m.Partitions))
	}
	if m.Replicas != 0 {
		parts = append(parts, fmt.Sprintf("REPLICAS = %d", m.Replicas))
	}
	if m.Timestamp != "" {
		parts = append(parts, fmt.Sprintf("TIMESTAMP = '%s'", m.Timestamp))
	}
	if m.TimestampFormat != "" {
		parts = append(parts, fmt.Sprintf("TIMESTAMP_FORMAT = '%s'", m.TimestampFormat))
	}

	if len(parts) != 0 {
		str.WriteString("WITH (")
		str.WriteString(strings.Join(parts, ","))
		str.WriteString(")")
	}

	return str.String()
}
