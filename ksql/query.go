package ksql

type (
	Query     int
	Reference int
)

const (
	LIST = Query(iota)
	DESCRIBE
	CREATE
	SELECT
	INSERT
)

const (
	STREAM = Reference(iota)
	TABLE
	TOPIC
)
