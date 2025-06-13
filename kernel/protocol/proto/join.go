package proto

import (
	"ksql/ksql"
	"ksql/schema"
)

type JoinEx struct {
	Field   string
	RefName string
	Ref     ksql.Reference
}

type Join struct {
	Kind        Joins
	SelectField schema.SearchField
	JoinField   schema.SearchField
}

type (
	Joins int
)

const (
	Left = Joins(iota)
	Inner
	Right
)
