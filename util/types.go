package util

import (
	"fmt"
	"reflect"
	"strings"
)

func FormatSlice(slice ...any) string {
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

func Serialize(val any) string {
	switch v := val.(type) {
	case []byte:
		return string(v)
	case string:
		return "'" + v + "'"
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

func IsIterable(val any) bool {
	t := reflect.TypeOf(val)
	if t == nil {
		return false
	}
	kind := t.Kind()
	return kind == reflect.Slice || kind == reflect.Array || kind == reflect.String
}

func IsOrdered(val any) bool {
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
