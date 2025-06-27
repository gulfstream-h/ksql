package ksql

import (
	"fmt"
	"ksql/util"
	"strconv"
	"strings"
)

type (
	Conditional interface {
		Left() []Field
		Right() []any
		Expression() (string, error)
	}

	Expression interface {
		Expression() (string, error)
	}

	Comparable interface {
		Equal(val any) Conditional
		NotEqual(val any) Conditional
	}

	Ordered interface {
		Greater(val any) Conditional
		Less(val any) Conditional
		GreaterEq(val any) Conditional
		LessEq(val any) Conditional
		Asc() OrderedExpression
		Desc() OrderedExpression
	}

	Nullable interface {
		IsNull() Conditional
		IsNotNull() Conditional
	}

	ComparableArray interface {
		In(val ...any) Conditional
		NotIn(val ...any) Conditional
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

func NewBooleanExp(left Field, right any, op Op) Conditional {
	return &booleanExp{left: left, right: right, operation: op}
}

func (b *booleanExp) Expression() (string, error) {
	var (
		operation   string
		ordered     bool
		iterable    bool
		rightString string
	)

	expression, err := b.left.Expression()
	if err != nil {
		return "", fmt.Errorf("left field expression: %w", err)
	}

	switch b.operation {
	case isNull:
		return fmt.Sprintf("%s IS NULL", expression), nil
	case isNotNull:
		return fmt.Sprintf("%s IS NOT NULL", expression), nil
	case isTrue:
		return fmt.Sprintf("%s IS TRUE", expression), nil
	case isFalse:
		return fmt.Sprintf("%s IS FALSE", expression), nil

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
		return "", fmt.Errorf("unsupported operation: %d", b.operation)
	}

	if ordered && !util.IsOrdered(b.right) {
		return "", fmt.Errorf("operation requeres right expression to be ordered: %v", b.right)
	}

	if iterable {
		if !util.IsIterable(b.right) {
			return "", fmt.Errorf("operation requires right expression to be iterable: %v", b.right)
		}
		rightString, err = util.FormatSlice(b.right)
		if err != nil {
			return "", fmt.Errorf("right expression: %w", err)
		}
		return fmt.Sprintf("%s %s %s", expression, operation, rightString), nil
	}

	switch v := b.right.(type) {
	case int:
		rightString = strconv.Itoa(v)
	case int8, int16, int32, int64:
		rightString = fmt.Sprintf("%d", v)
	case uint, uint8, uint16, uint32, uint64:
		rightString = fmt.Sprintf("%d", v)
	case float32, float64:
		rightString = fmt.Sprintf("%v", v)
	case string:
		rightString = fmt.Sprintf("'%s'", v)
	case bool:
		rightString = strings.ToUpper(strconv.FormatBool(v))
	case Field:
		rightString, err = v.Expression()
		if err != nil {
			return "", fmt.Errorf("invalid right field expression: %w", err)
		}
	default:
		if util.IsNil(v) {
			switch b.operation {
			case equal:
				return fmt.Sprintf("%s IS NULL", expression), nil
			case notEqual:
				return fmt.Sprintf("%s IS NOT NULL", expression), nil
			default:
				return "", fmt.Errorf("unsupported operation with nil value: %d", b.operation)
			}
		}
		rightString = util.Serialize(b.right)
		if len(rightString) == 0 {
			return "", fmt.Errorf("unsupported type in right expression: %T", b.right)
		}
	}

	return fmt.Sprintf("%s %s %s", expression, operation, rightString), nil
}

func (b *booleanExp) Left() []Field {
	return []Field{b.left}
}

func (b *booleanExp) Right() []any {
	return []any{b.right}
}
