package kinds

import (
	"errors"
	"log/slog"
	"math"
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

func (k Ktype) GetKafkaRepresentation() string {
	switch k {
	case Int:
		return "INT"
	case Bool:
		return "BOOL"
	case Double:
		return "FLOAT"
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

func (k Ktype) Example() any {
	switch k {
	case Bool:
		return true
	case Int:
		return -1
	case Double:
		return 2.71
	case String:
		return ""
	case BigInt:
		return math.MaxInt64
	case Bytes:
		return []byte("aGVsbG8gd29ybGQ=")
	case ArrInt:
		return []int{-1, 0, 1}
	case ArrBool:
		return []bool{true, false}
	case ArrDouble:
		return []float64{2.71, 3.14}
	case ArrString:
		return []string{"example1", "example2"}
	case ArrBigInt:
		return []int64{math.MaxInt64, math.MinInt64}
	case ArrBytes:
		return [][]byte{[]byte("example1"), []byte("example2")}
	case MapInt:
		return map[string]int{"key1": -1, "key2": 0, "key3": 1}
	case MapBool:
		return map[string]bool{"key1": true, "key2": false}
	case MapDouble:
		return map[string]float64{"key1": 2.71, "key2": 3.14}
	case MapString:
		return map[string]string{"key1": "example1", "key2": "example2"}
	case MapBigInt:
		return map[string]int64{"key1": math.MaxInt64, "key2": math.MinInt64}
	case MapBytes:
		return map[string][]byte{"key1": []byte("example1"), "key2": []byte("example2")}
	}

	return nil
}

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
