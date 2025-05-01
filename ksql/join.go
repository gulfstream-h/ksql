package ksql

import (
	"fmt"
	"ksql/schema"
)

// will be used in future commits
const (
	InnerJoin = Joins[schema.SearchField](iota)
	LeftJoin
	RightJoin
)

type (
	Joins[J schema.Joinable] int
)

func (j Joins[J]) Join(F1, F2 J) string {
	switch j {
	case Joins[J](InnerJoin):
		return innerJoin(F1, F2)
	case Joins[J](LeftJoin):
		return leftJoin(F1, F2)
	case Joins[J](RightJoin):
		return rightJoin(F1, F2)
	default:
		return ""
	}
}

func innerJoin[J schema.Joinable](fieldOne, fieldTwo J) (command string) {
	if fieldOne.CheckJoinCapability() && fieldTwo.CheckJoinCapability() {
		command = fmt.Sprintf("JOIN %s ON %s.%s = %s.%s",
			fieldTwo.Referer, fieldOne.Referer, fieldOne.FieldName, fieldTwo.Referer, fieldTwo.FieldName)
	}

	return
}

func leftJoin[J schema.Joinable](fieldOne, fieldTwo J) (command string) {
	if fieldOne.CheckJoinCapability() && fieldTwo.CheckJoinCapability() {
		command = fmt.Sprintf("LEFT JOIN %s ON %s.%s = %s.%s",
			fieldTwo.Referer, fieldOne.Referer, fieldOne.FieldName, fieldTwo.Referer, fieldTwo.FieldName)
	}

	return
}

func rightJoin[J schema.Joinable](fieldOne, fieldTwo J) (command string) {
	if fieldOne.CheckJoinCapability() && fieldTwo.CheckJoinCapability() {
		command = fmt.Sprintf("RIGHT JOIN %s ON %s.%s = %s.%s",
			fieldTwo.Referer, fieldOne.Referer, fieldOne.FieldName, fieldTwo.Referer, fieldTwo.FieldName)
	}

	return
}
