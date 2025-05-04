package schema

import (
	"errors"
	"reflect"
	"strings"
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

type Joinable interface {
	SearchField
}

func (k KsqlKind) CheckJoinCapability() (capable bool) {
	switch k {
	case Bool, Int, Float, String:
		capable = true
	}
	return
}

func (k KsqlKind) Marshal() (string, error) {
	switch k {
	case Bool:
		return "BOOL", nil
	case Int:
		return "INT", nil
	case Float:
		return "FLOAT", nil
	case String:
		return "STRING", nil
	}

	return "", errors.New("unpredictable type")
}

func Unmarshal(pairs []string) map[string]KsqlKind {

	var (
		fields map[string]KsqlKind
	)

	for _, pair := range pairs {
		values := strings.Split(pair, " ")
		key, value := values[0], values[1]
		switch value {
		case "BOOL":
			fields[key] = Bool
		case "INT":
			fields[key] = Int
		case "FLOAT":
			fields[key] = Float
		case "STRING":
			fields[key] = String
		default:
			continue
		}
	}

	return fields
}

func castType(kind reflect.Kind) (KsqlKind, error) {
	switch kind {
	case reflect.Invalid:
		return 0, errors.New("invalid format isn't supported in ksql yet")
	case reflect.Bool:
		return Bool, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return Int, nil
	case reflect.Float32, reflect.Float64:
		return Float, nil
	case reflect.Complex64:
		return 0, errors.New("complex isn't supported now")
	case reflect.Complex128:
		return 0, errors.New("complex isn't supported now")
	case reflect.Array:
		// TODO: make easy implicit
		return 0, errors.New("array isn't supported now")
	case reflect.Chan:
		return 0, errors.New("chat isn't supported now")
	case reflect.Func:
		return 0, errors.New("func isn't supported now")
	case reflect.Interface:
		return 0, errors.New("interface isn't supported now")
	case reflect.Map:
		return 0, errors.New("map isn't supported now")
	case reflect.Pointer:
		// TODO: make easy implicit
		return 0, errors.New("pointer isn't supported now")
	case reflect.Slice:
		// TODO: make easy implicit
		return 0, errors.New("slice isn't supported now")
	case reflect.String:
		return String, nil
	case reflect.Struct:
		// TODO: make easy implicit
		return 0, errors.New("struct isn't supported now")
	case reflect.UnsafePointer:
		return 0, errors.New("unsafe pointer isn't supported now")
	}

	return 0, errors.New("unpredictable reflect kind")
}

type (
	ValueFormat int
)

const (
	String = ValueFormat(iota)
	Json
	Integer
	Long
	Double
	Float
	Bytes
	None
)

var (
	ErrUnknownValueFormat = errors.New("unknown value format have been provided")
)

func (v ValueFormat) GetName() (string, error) {
	switch v {
	case String:
		return "string", nil
	case Json:
		return "json", nil
	case Integer:
		return "integer", nil
	case Long:
		return "long", nil
	case Double:
		return "double", nil
	case Float:
		return "float", nil
	case Bytes:
		return "bytes", nil
	case None:
		return "none", nil
	}
	return "", ErrUnknownValueFormat
}
