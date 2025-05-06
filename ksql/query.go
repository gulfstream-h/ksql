package ksql

type (
	Query     int
	Reference int
)

const (
	LIST = Query(iota)
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
