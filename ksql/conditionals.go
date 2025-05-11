package ksql

import (
	"fmt"
	"ksql/schema"
)

type Cond struct {
	WhereClause  []WhereEx
	HavingClause []HavingEx
}

type WhereEx struct {
	FieldName string
	schema    schema.SearchField
	comp      string
	equal     string
}

func (ex WhereEx) Equal(values ...string) WhereEx {
	if len(values) == 0 {
		return ex
	}

	if len(values) == 1 {
		ex.comp = "="
		ex.equal = values[0]
		return ex
	}

	ex.comp = "in"

	var (
		fields string
	)

	fields += "("

	for _, v := range values {
		fields += fmt.Sprintf("%s,", v)
	}

	fields += ")"

	return ex
}

type HavingEx struct {
	FieldName string
	schema    schema.SearchField
	comp      string
	equal     string
}

func (ex HavingEx) Equal(values ...string) HavingEx {
	if len(values) == 0 {
		return ex
	}

	if len(values) == 1 {
		ex.comp = "="
		ex.equal = values[0]
		return ex
	}

	ex.comp = "in"

	var (
		fields string
	)

	fields += "("

	for _, v := range values {
		fields += fmt.Sprintf("%s,", v)
	}

	fields += ")"

	return ex
}
