package ksql

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

type (
	BooleanExpression interface {
		Expression() string
		Left() Field
		Right() any
	}

	Comparable interface {
		Equal(val any) BooleanExpression
		NotEqual(val any) BooleanExpression
	}

	Ordered interface {
		Greater(val any) BooleanExpression
		Less(val any) BooleanExpression
		GreaterEq(val any) BooleanExpression
		LessEq(val any) BooleanExpression
	}

	Nullable interface {
		IsNull() BooleanExpression
		IsNotNull() BooleanExpression
	}

	ComparableArray interface {
		In(val ...any) BooleanExpression
		NotIn(val ...any) BooleanExpression
	}

	Op int
)

const (
	equal = iota
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

func NewBooleanExp(left Field, right any, op Op) BooleanExpression {
	return &booleanExp{left: left, right: right, operation: op}
}

func (b *booleanExp) Expression() string {
	var (
		operation   string
		ordered     bool
		iterable    bool
		rightString string
	)

	switch b.operation {
	case isNull:
		return fmt.Sprintf("%s IS NULL", b.left.Expression())
	case isNotNull:
		return fmt.Sprintf("%s IS NOT NULL", b.left.Expression())
	case isTrue:
		return fmt.Sprintf("%s IS TRUE", b.left.Expression())
	case isFalse:
		return fmt.Sprintf("%s IS FALSE", b.left.Expression())

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
		return ""
	}

	if ordered && !isOrdered(b.right) {
		return ""
	}

	if iterable {
		if !isIterable(b.right) {
			return ""
		}

		rightString = formatSlice(b.right.([]any))
		return fmt.Sprintf("%s %s %s", b.left.Expression(), operation, rightString)
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
		rightString = serialize(b.right)
		if len(rightString) == 0 {
			return ""
		}
	}

	return fmt.Sprintf("%s %s %s", b.left.Expression(), operation, rightString)
}

func formatSlice[T any](slice []T) string {
	var parts []string
	for _, v := range slice {
		switch x := any(v).(type) {
		case string:
			parts = append(parts, fmt.Sprintf("'%s'", x))
		case int, int64, float64:
			parts = append(parts, fmt.Sprintf("%v", x))
		default:
			return ""
		}
	}
	return "(" + strings.Join(parts, ", ") + ")"
}

func serialize(val any) string {
	switch v := val.(type) {
	case []byte:
		return string(v)
	case string:
		return v
	case fmt.Stringer:
		return v.String()
	case float32, float64:
		return fmt.Sprintf("%v", v)
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", v)
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v)
	default:
		return ""
	}
}

func isIterable(val any) bool {
	t := reflect.TypeOf(val)
	if t == nil {
		return false
	}
	kind := t.Kind()
	return kind == reflect.Slice || kind == reflect.Array || kind == reflect.String
}

func isOrdered(val any) bool {
	switch val.(type) {
	case int, uint, int8, uint8, int16, uint16, int32, uint32, int64, uint64:
		return true
	case float32, float64:
		return true
	case string:
		return true
	case []byte:
		return true
	default:
		return false
	}
}

func (b *booleanExp) Left() Field {
	return b.left
}

func (b *booleanExp) Right() any {
	return b.right
}
