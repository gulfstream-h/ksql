package ksql

type (
	// Reference - represents a reference type for streams, tables, or topics
	Reference int
)

const (
	STREAM = Reference(iota)
	TABLE
	TOPIC
)
