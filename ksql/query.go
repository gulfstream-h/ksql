package ksql

type Query struct {
	Query QueryType
	Ref   Reference
	Name  string
	CTE   map[string]Query
}

type (
	QueryType int
	Reference int
)

const (
	LIST = QueryType(iota)
	DESCRIBE
	DROP
	CREATE
	SELECT
	INSERT
)

const (
	STREAM = Reference(iota)
	TABLE
	TOPIC
)
