package join

import (
	"fmt"
	"ksql/schema"
)

type (
	Joins int
)

// will be used in future commits
const (
	Inner = Joins(iota)
	Left
	Right
)

func InnerJoin[J schema.Joinable](fieldOne, fieldTwo J) (command string) {
	if fieldOne.CheckJoinCapability() && fieldTwo.CheckJoinCapability() {
		command = fmt.Sprintf("JOIN %s ON %s.%s = %s.%s",
			fieldTwo.Referer, fieldOne.Referer, fieldOne.FieldName, fieldTwo.Referer, fieldTwo.FieldName)
	}

	return
}

func LeftJoin[J schema.Joinable](fieldOne, fieldTwo J) (command string) {
	if fieldOne.CheckJoinCapability() && fieldTwo.CheckJoinCapability() {
		command = fmt.Sprintf("LEFT JOIN %s ON %s.%s = %s.%s",
			fieldTwo.Referer, fieldOne.Referer, fieldOne.FieldName, fieldTwo.Referer, fieldTwo.FieldName)
	}

	return
}

func RightJoin[J schema.Joinable](fieldOne, fieldTwo J) (command string) {
	if fieldOne.CheckJoinCapability() && fieldTwo.CheckJoinCapability() {
		command = fmt.Sprintf("RIGHT JOIN %s ON %s.%s = %s.%s",
			fieldTwo.Referer, fieldOne.Referer, fieldOne.FieldName, fieldTwo.Referer, fieldTwo.FieldName)
	}

	return
}
