package util

import (
	"fmt"
	"reflect"
	"strings"
)

func FormatSlice(slice any) (string, bool) {
	val := reflect.ValueOf(slice)
	if val.Kind() != reflect.Slice && val.Kind() != reflect.Array {
		return "", false
	}

	var parts []string
	for i := 0; i < val.Len(); i++ {
		v := val.Index(i).Interface()
		switch x := v.(type) {
		case string:
			parts = append(parts, fmt.Sprintf("'%s'", x))
		case int, int64, float64:
			parts = append(parts, fmt.Sprintf("%v", x))
		case bool:
			if x {
				parts = append(parts, "TRUE")
			} else {
				parts = append(parts, "FALSE")
			}
		default:
			if IsNil(x) {
				parts = append(parts, "NULL")
				continue
			}
			return "", false
		}
	}
	return "(" + strings.Join(parts, ", ") + ")", true
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
	v := reflect.ValueOf(val)
	if !v.IsValid() {
		return false
	}
	kind := v.Kind()
	if kind == reflect.Slice || kind == reflect.Array || kind == reflect.String {
		return v.Len() > 0
	}
	return false
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

func IsNil(v any) bool {
	if v == nil {
		return true
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Ptr, reflect.Interface:
		return rv.IsNil()
	default:
		return false
	}
}
