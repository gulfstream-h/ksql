package kinds

import (
	"errors"
	"log/slog"
	"reflect"
)

type (
	Ktype int
)

const (
	Bool Ktype = iota + 1
	Int
	Double
	String
	BigInt
	Bytes

	ArrInt
	ArrBool
	ArrDouble
	ArrString
	ArrBigInt
	ArrBytes

	MapInt
	MapBool
	MapDouble
	MapString
	MapBigInt
	MapBytes
)

// ToKsql - translate golang struct
// into internal type
func ToKsql(typ reflect.Type) (Ktype, error) {
	switch typ.Kind() {
	case reflect.Invalid:
		return 0, errUnsupportedType
	case reflect.Bool:
		return Bool, nil
	case reflect.Uint8:
		return Bytes, nil
	case
		reflect.Int,
		reflect.Int8,
		reflect.Int16,
		reflect.Int32:

		return Int, nil
	case reflect.Int64:
		return BigInt, nil
	case
		reflect.Float32,
		reflect.Float64:
		return Double, nil
	case reflect.String:
		return String, nil
	case reflect.Map:
		keyTyp := typ.Key()
		if keyTyp.Kind() != reflect.String {
			slog.Debug("unsupported map key type", "type", keyTyp)
			return 0, errUnsupportedType
		}

		valTyp, err := ToKsql(typ.Elem())
		if err != nil {
			return 0, err
		}

		switch valTyp {
		case Int:
			return MapInt, nil
		case Bool:
			return MapBool, nil
		case Double:
			return MapDouble, nil
		case String:
			return MapString, nil
		case BigInt:
			return MapBigInt, nil
		case Bytes:
			return MapBytes, nil
		default:
			slog.Debug("unsupported map value type", "type", valTyp)
			return 0, errUnsupportedType
		}

	case reflect.Slice:
		elemTyp, err := ToKsql(typ.Elem())
		if err != nil {
			return 0, err
		}
		switch elemTyp {
		case Int:
			return ArrInt, nil
		case Bool:
			return ArrBool, nil
		case Double:
			return ArrDouble, nil
		case String:
			return ArrString, nil
		case BigInt:
			return ArrBigInt, nil
		case Bytes:
			return Bytes, nil
		case ArrBytes:
			return Bytes, nil
		default:
			slog.Debug("unsupported slice type", "type", elemTyp)
		}

		fallthrough
	case reflect.Array:
		fallthrough
	case reflect.Struct:
		// TODO: make easy implicit
		fallthrough
	case
		reflect.Uint,
		reflect.Uint16,
		reflect.Uint32,
		reflect.Uint64,
		reflect.Uintptr:
		fallthrough
	case
		reflect.Chan,
		reflect.Func,
		reflect.Interface,
		reflect.UnsafePointer,
		reflect.Pointer,
		reflect.Complex64,
		reflect.Complex128:

		return 0, errUnsupportedType
	}

	return 0, errUnsupportedType
}

// GetKafkaRepresentation - translates
// internal type representation
// into ksql acceptable format
func (k Ktype) GetKafkaRepresentation() string {
	switch k {
	case Int:
		return "INT"
	case Bool:
		return "BOOL"
	case Double:
		return "DOUBLE"
	case String:
		return "VARCHAR"
	case BigInt:
		return "BIGINT"
	case Bytes:
		return "BYTES"
	case ArrInt:
		return "ARRAY<INT>"
	case ArrBool:
		return "ARRAY<BOOL>"
	case ArrDouble:
		return "ARRAY<DOUBLE>"
	case ArrString:
		return "ARRAY<VARCHAR>"
	case ArrBigInt:
		return "ARRAY<BIGINT>"
	case ArrBytes:
		return "ARRAY<BYTES>"
	case MapInt:
		return "MAP<VARCHAR, INT>"
	case MapBool:
		return "MAP<VARCHAR, BOOL>"
	case MapDouble:
		return "MAP<VARCHAR, DOUBLE>"
	case MapString:
		return "MAP<VARCHAR, VARCHAR>"
	case MapBigInt:
		return "MAP<VARCHAR, BIGINT>"
	case MapBytes:
		return "MAP<VARCHAR, BYTES>"
	}

	return ""
}

// CastResponseTypes - translates ksql describe response
// string schema into internal representation.
func CastResponseTypes(typification string) (Ktype, bool) {
	switch typification {
	case "INT", "INTEGER":
		return Int, true
	case "DOUBLE":
		return Double, true
	case "VARCHAR", "STRING":
		return String, true
	case "BOOL":
		return Bool, true
	case "BYTES":
		return Bytes, true
	case "BIGINT":
		return BigInt, true
	case "ARRAY<INT>":
		return ArrInt, true
	case "ARRAY<DOUBLE>":
		return ArrDouble, true
	case "ARRAY<VARCHAR>", "ARRAY<STRING>":
		return ArrString, true
	case "ARRAY<BOOL>":
		return ArrBool, true
	case "ARRAY<BYTES>":
		return ArrBytes, true
	case "ARRAY<BIGINT>":
		return ArrBigInt, true
	case "MAP<VARCHAR, INT>", "MAP<STRING, INT>":
		return MapInt, true
	case "MAP<VARCHAR, DOUBLE>", "MAP<STRING, DOUBLE>":
		return MapDouble, true
	case "MAP<VARCHAR, VARCHAR>", "MAP<VARCHAR, STRING>",
		"MAP<STRING, VARCHAR>", "MAP<STRING, STRING>":
		return MapString, true
	case "MAP<VARCHAR, BOOL>, MAP<STRING, BOOL>":
		return MapBool, true
	case "MAP<VARCHAR, BYTES>, MAP<STRING, BYTES>":
		return MapBytes, true
	case "MAP<VARCHAR, BIGINT>, MAP<STRING, BIGINT>":
		return MapBigInt, true
	default:
		return 0, false
	}
}

var (
	errUnsupportedType = errors.New("type isn't supported at now")
)
