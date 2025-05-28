package ksql

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
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

	if ordered && !isOrdered(b.right) {
		return "", false
	}

	if iterable {
		if !isIterable(b.right) {
			return "", false
		}
		rightString = formatSlice(b.right)
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
		rightString = serialize(b.right)
		if len(rightString) == 0 {
			return "", false
		}
	}

	return fmt.Sprintf("%s %s %s", expression, operation, rightString), true
}

func formatSlice(slice ...any) string {
	var parts []string
	for _, v := range slice {
		switch x := v.(type) {
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
