package dao

import (
	"github.com/gulfstream-h/ksql/internal/kernel/protocol/dto"
)

type Topic struct {
	Name     string `json:"name"`
	Replicas []int  `json:"replicaInfo"`
}

type ShowTopics struct {
	KafkaType string  `json:"type"`
	Command   string  `json:"statementText"`
	Topics    []Topic `json:"topics"`
	Warnings  []any   `json:"warnings"`
}

type Stream struct {
	Type        string `json:"type"`
	Name        string `json:"name"`
	Topic       string `json:"topic"`
	KeyFormat   string `json:"keyFormat"`
	ValueFormat string `json:"valueFormat"`
	IsWindowed  bool   `json:"isWindowed"`
}

type StreamsInfo struct {
	Type          string   `json:"@type"`
	StatementText string   `json:"statementText"`
	Streams       []Stream `json:"streams"`
	Warnings      []any    `json:"warnings"`
}

type Table struct {
	Type        string `json:"type"`
	Name        string `json:"name"`
	Topic       string `json:"topic"`
	KeyFormat   string `json:"keyFormat"`
	ValueFormat string `json:"valueFormat"`
	IsWindowed  bool   `json:"isWindowed"`
}

type ShowTables struct {
	Type          string  `json:"@type"`
	StatementText string  `json:"statementText"`
	Tables        []Table `json:"tables"`
	Warnings      []any   `json:"warnings"`
}

func (st ShowTopics) DTO() dto.ShowTopics {
	topicInfos := make([]dto.TopicInfo, len(st.Topics))
	for i, topic := range st.Topics {
		topicInfos[i] = dto.TopicInfo{Name: topic.Name}
	}
	return dto.ShowTopics{Topics: topicInfos}
}

func (ss StreamsInfo) DTO() dto.ShowStreams {
	streamsInfos := make([]dto.RelationInfo, len(ss.Streams))
	for i, stream := range ss.Streams {
		streamsInfos[i] = dto.RelationInfo{
			Name:        stream.Name,
			Topic:       stream.Topic,
			KeyFormat:   stream.KeyFormat,
			ValueFormat: stream.ValueFormat,
			Windowed:    stream.IsWindowed,
		}
	}
	return dto.ShowStreams{Streams: streamsInfos}
}

func (ss ShowTables) DTO() dto.ShowTables {
	tableInfos := make([]dto.RelationInfo, len(ss.Tables))
	for i, table := range ss.Tables {
		tableInfos[i] = dto.RelationInfo{
			Name:        table.Name,
			Topic:       table.Topic,
			KeyFormat:   table.KeyFormat,
			ValueFormat: table.ValueFormat,
			Windowed:    table.IsWindowed,
		}
	}
	return dto.ShowTables{Tables: tableInfos}
}
