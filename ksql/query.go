package ksql

type (
	Query int
)

const (
	LIST = Query(iota)
	DESCRIBE
	CREATE
	SELECT
	INSERT
)
