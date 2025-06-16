package kinds

import (
	"errors"
	"reflect"
)

type (
	Ktype int
)

const (
	Bool Ktype = iota + 1
	Int
	Float
	String
)

func ToKsql(kind reflect.Kind) (Ktype, error) {
	switch kind {
	case reflect.Invalid:
		return 0, errUnsupportedType
	case reflect.Bool:
		return Bool, nil
	case
		reflect.Int,
		reflect.Int8,
		reflect.Int16,
		reflect.Int32,
		reflect.Int64,
		reflect.Uint,
		reflect.Uint8,
		reflect.Uint16,
		reflect.Uint32,
		reflect.Uint64,
		reflect.Uintptr:

		return Int, nil
	case
		reflect.Float32,
		reflect.Float64:

		return Float, nil
	case reflect.String:
		return String, nil
	case reflect.Struct:
		// TODO: make easy implicit
		fallthrough
	case reflect.Array, reflect.Slice:
		// TODO: make easy implicit
		fallthrough
	case
		reflect.Chan,
		reflect.Func,
		reflect.Interface,
		reflect.Map,
		reflect.UnsafePointer,
		reflect.Pointer,
		reflect.Complex64,
		reflect.Complex128:

		return 0, errUnsupportedType
	}

	return 0, errUnsupportedType
}

func (k Ktype) GetKafkaRepresentation() string {
	switch k {
	case Int:
		return "INT"
	case Bool:
		return "BOOL"
	case Float:
		return "FLOAT"
	case String:
		return "VARCHAR"
	default:
		return ""
	}
}

func (k Ktype) Example() any {
	switch k {
	case Bool:
		return true
	case Int:
		return -1
	case Float:
		return 2.71
	case String:
		return ""
	}

	return nil
}

var (
	errUnsupportedType = errors.New("type isn't supported at now")
)
