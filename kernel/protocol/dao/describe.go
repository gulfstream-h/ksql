package dao

import (
	"ksql/kernel/protocol/dto"
)

type FieldSchema struct {
	Type         string       `json:"type"`
	Fields       []any        `json:"fields"`
	MemberSchema *FieldSchema `json:"memberSchema"`
}

type Field struct {
	Name   string      `json:"name"`
	Schema FieldSchema `json:"schema"`
	Type   string      `json:"type"`
}

type SourceDescription struct {
	Name                 string      `json:"name"`
	WindowType           interface{} `json:"windowType"`
	ReadQueries          []any       `json:"readQueries"`
	WriteQueries         []any       `json:"writeQueries"`
	Fields               []Field     `json:"fields"`
	Type                 string      `json:"type"`
	Timestamp            string      `json:"timestamp"`
	Statistics           string      `json:"statistics"`
	ErrorStats           string      `json:"errorStats"`
	Extended             bool        `json:"extended"`
	KeyFormat            string      `json:"keyFormat"`
	ValueFormat          string      `json:"valueFormat"`
	Topic                string      `json:"topic"`
	Partitions           int         `json:"partitions"`
	Replication          int         `json:"replication"`
	Statement            string      `json:"statement"`
	QueryOffsetSummaries []any       `json:"queryOffsetSummaries"`
	SourceConstraints    []any       `json:"sourceConstraints"`
	ClusterStatistics    []any       `json:"clusterStatistics"`
	ClusterErrorStats    []any       `json:"clusterErrorStats"`
}

//curl -X POST \
//-H "Content-Type: application/vnd.ksql.v1+json" \
//-d '{
//  "ksql": "DESCRIBE MY_STREAM;"
//}' \
//http://localhost:8088/ksql

type DescribeResponse struct {
	Type              string            `json:"@type"`
	StatementText     string            `json:"statementText"`
	SourceDescription SourceDescription `json:"sourceDescription"`
	Warnings          []any             `json:"warnings"`
}

func (dr DescribeResponse) DTO() dto.RelationDescription {
	fields := make([]dto.Field, len(dr.SourceDescription.Fields))

	for i, field := range dr.SourceDescription.Fields {
		kind := field.Schema.Type

		if kind == "ARRAY" && field.Schema.MemberSchema != nil {
			kind = "ARRAY<" + field.Schema.MemberSchema.Type + ">"
		}

		if kind == "MAP" && field.Schema.MemberSchema != nil {
			// in ksql maps only strings keys are allowed
			kind = "MAP<STRING, " + field.Schema.MemberSchema.Type + ">"
		}

		fields[i] = dto.Field{
			Name: field.Name,
			Kind: kind,
		}
	}

	return dto.RelationDescription{
		Name:             dr.SourceDescription.Name,
		Fields:           fields,
		Kind:             dr.SourceDescription.Type,
		KeyFormat:        dr.SourceDescription.KeyFormat,
		ValueFormat:      dr.SourceDescription.ValueFormat,
		Topic:            dr.SourceDescription.Topic,
		Partitions:       dr.SourceDescription.Partitions,
		Replication:      dr.SourceDescription.Replication,
		CreatedByCommand: dr.SourceDescription.Statement,
	}

}
