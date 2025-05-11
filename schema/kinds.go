package schema

import (
	"errors"
	"reflect"
)

type (
	KsqlKind int
)

const (
	Bool KsqlKind = iota + 1
	Int
	Float
	String
)

type ResourceKind int

const (
	STREAM = ResourceKind(iota)
	TABLE
)

func castType(kind reflect.Kind) (KsqlKind, error) {
	switch kind {
	case reflect.Invalid:
		return 0, errors.New("invalid format isn't supported in ksql yet")
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

		return 0, errors.New("type isn't supported now")
	}

	return 0, errors.New("unpredictable reflect kind")
}

func getKindExample(kind KsqlKind) any {
	switch kind {
	case Bool:
		return true
	case Int:
		return 0
	case Float:
		return 2.71
	case String:
		return ""
	}

	return nil
}
