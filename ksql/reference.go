package ksql

type (
	Reference int
)

const (
	STREAM = Reference(iota)
	TABLE
	TOPIC
)
