package dao

type FieldSchema struct {
	Type         string      `json:"type"`
	Fields       []any       `json:"fields"`
	MemberSchema interface{} `json:"memberSchema"`
}

type Field struct {
	Name   string      `json:"name"`
	Schema FieldSchema `json:"schema"`
	Type   string      `json:"type,omitempty"`
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

type DescribeResponse struct {
	Type              string            `json:"@type"`
	StatementText     string            `json:"statementText"`
	SourceDescription SourceDescription `json:"sourceDescription"`
	Warnings          []any             `json:"warnings"`
}
