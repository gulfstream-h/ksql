package ksql

import (
	"fmt"
	"github.com/gulfstream-h/ksql/internal/util"
	"strconv"
	"strings"
)

type (
	// Conditional - common interface for all conditional expressions
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
	// Op - represents an operation type for boolean expressions
	Op int
)

const (
	// equal checks if a value is equal to another value
	equal = Op(iota)
	// notEqual checks if a value is not equal to another value
	notEqual
	// more checks if a value is greater than another value
	more
	// less checks if a value is greater than another value
	less
	// moreEqual checks if a value is greater than or equal to another value
	moreEqual
	// lessEqual checks if a value is less than or equal to another value
	lessEqual
	// isNull checks if a value is null
	isNull
	// isNotNull checks if a value is not null
	isNotNull
	// isTrue checks if a value is true
	isTrue
	// isFalse checks if a value is false
	isFalse
	// in is used to check if a value is in a set of values
	in
	// notIn is used to check if a value is not in a set of values
	notIn
)

// booleanExp - represents a boolean expression with a left field, right value, and operation
type booleanExp struct {
	left      Field
	right     any
	operation Op
}

// NewBooleanExp - creates a new boolean expression with the given left field, right value, and operation
func NewBooleanExp(left Field, right any, op Op) Conditional {
	return &booleanExp{left: left, right: right, operation: op}
}

// Expression - accumulates all applied settings and build string query
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

// Left - returns left field of the boolean expression
func (b *booleanExp) Left() []Field {
	return []Field{b.left}
}

// Right - returns right value of the boolean expression
func (b *booleanExp) Right() []any {
	return []any{b.right}
}
