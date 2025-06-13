package ksql

import (
	"fmt"
	"ksql/util"
	"strconv"
)

type (
	Expression interface {
		Expression() (string, bool)
	}

	Comparable interface {
		Equal(val any) Expression
		NotEqual(val any) Expression
	}

	Ordered interface {
		Greater(val any) Expression
		Less(val any) Expression
		GreaterEq(val any) Expression
		LessEq(val any) Expression
	}

	Nullable interface {
		IsNull() Expression
		IsNotNull() Expression
	}

	ComparableArray interface {
		In(val ...any) Expression
		NotIn(val ...any) Expression
	}

	Op int
)

const (
	equal = Op(iota)
	notEqual
	more
	less
	moreEqual
	lessEqual
	isNull
	isNotNull
	isTrue
	isFalse
	in
	notIn
)

type booleanExp struct {
	left      Field
	right     any
	operation Op
}

func NewBooleanExp(left Field, right any, op Op) Expression {
	return &booleanExp{left: left, right: right, operation: op}
}

func (b *booleanExp) Expression() (string, bool) {
	var (
		operation   string
		ordered     bool
		iterable    bool
		rightString string
	)

	expression, ok := b.left.Expression()
	if !ok {
		return "", false
	}

	switch b.operation {
	case isNull:
		return fmt.Sprintf("%s IS NULL", expression), true
	case isNotNull:
		return fmt.Sprintf("%s IS NOT NULL", expression), true
	case isTrue:
		return fmt.Sprintf("%s IS TRUE", expression), true
	case isFalse:
		return fmt.Sprintf("%s IS FALSE", expression), true

	case equal:
		operation = "="
	case notEqual:
		operation = "!="
	case more:
		operation = ">"
		ordered = true
	case less:
		operation = "<"
		ordered = true
	case moreEqual:
		operation = ">="
		ordered = true
	case lessEqual:
		operation = "<="
		ordered = true
	case in:
		operation = "IN"
		iterable = true
	case notIn:
		operation = "NOT IN"
		iterable = true
	default:
		return "", false
	}

	if ordered && !util.IsOrdered(b.right) {
		return "", false
	}

	if iterable {
		if !util.IsIterable(b.right) {
			return "", false
		}
		rightString = util.FormatSlice(b.right)
		return fmt.Sprintf("%s %s %s", expression, operation, rightString), true
	}

	switch v := b.right.(type) {
	case int:
		rightString = strconv.Itoa(v)
	case int8, int16, int32, int64:
		rightString = fmt.Sprintf("%d", v)
	case uint, uint8, uint16, uint32, uint64:
		rightString = fmt.Sprintf("%d", v)
	case float32, float64:
		rightString = fmt.Sprintf("%f", v)
	case string:
		rightString = fmt.Sprintf("'%s'", v)
	default:
		rightString = util.Serialize(b.right)
		if len(rightString) == 0 {
			return "", false
		}
	}

	return fmt.Sprintf("%s %s %s", expression, operation, rightString), true
}

func (b *booleanExp) Left() Field {
	return b.left
}

func (b *booleanExp) Right() any {
	return b.right
}
