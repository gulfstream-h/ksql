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
	schema.SearchField
	comp  string
	equal string
}

func (ex *WhereEx) Equal(values ...string) {
	if len(values) == 0 {
		return
	}

	if len(values) == 1 {
		ex.comp = "="
		ex.equal = values[0]
		return
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
}

type HavingEx struct {
	schema.SearchField
	comp  string
	equal string
}

func (ex *HavingEx) Equal(values ...string) {
	if len(values) == 0 {
		return
	}

	if len(values) == 1 {
		ex.comp = "="
		ex.equal = values[0]
		return
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
}
