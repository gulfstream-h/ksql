package netparse

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
)

// ParseHeadersAndValues - defines fields
// and values name and group em in coinciding pairs
func ParseHeadersAndValues(
	headers string,
	values []any,
) (map[string]any, error) {

	var (
		parts []string
	)

	re := regexp.MustCompile("`[^`]+`\\s+\\w+(?:<[^>]+>)?")

	matches := re.FindAllString(headers, -1)

	for _, m := range matches {
		parts = append(parts, m)
	}

	result := make(map[string]any)

	re = regexp.MustCompile("`([^`]*)`")

	if len(parts) != len(values) {
		return nil, fmt.Errorf("headers and values count mismatch")
	}

	for i, part := range parts {
		match := re.FindStringSubmatch(part)
		if len(match) < 2 {
			return nil, fmt.Errorf("invalid header format: %s", part)
		}

		if strings.Contains(part, "BYTES") {
			castedValue, ok := values[i].(string)
			if !ok {
				return nil, fmt.Errorf("expected string alias for BYTES type, got %T", values[i])
			}

			result[match[1]] = []byte(castedValue)
			continue
		}

		result[match[1]] = values[i]
	}

	return result, nil
}

// NormalizeValue - defines the real type of
// unmarshalled ksql response interface field
// and generates reflect value, that can be set
// to destination generic struct
func NormalizeValue(
	v interface{},
	targetType reflect.Type,
) (reflect.Value, bool) {

	switch targetType.Kind() {
	case reflect.Slice:
		rawSlice, ok := v.([]interface{})
		if !ok {
			if bytesVal, ok := v.([]byte); ok {
				return reflect.ValueOf(bytesVal), true
			}

			return reflect.Value{}, false
		}
		elemType := targetType.Elem()
		result := reflect.MakeSlice(targetType, len(rawSlice), len(rawSlice))
		for i, item := range rawSlice {
			itemVal := reflect.ValueOf(item)
			if itemVal.Type().ConvertibleTo(elemType) {
				result.Index(i).Set(itemVal.Convert(elemType))
			} else if elemType.Kind() == reflect.String && itemVal.Kind() == reflect.Interface {
				strVal, ok := item.(string)
				if !ok {
					return reflect.Value{}, false
				}
				result.Index(i).Set(reflect.ValueOf(strVal))
			} else {
				return reflect.Value{}, false
			}
		}
		return result, true

	case reflect.Map:
		rawMap, ok := v.(map[string]interface{})
		if !ok {
			return reflect.Value{}, false
		}
		keyType := targetType.Key()
		elemType := targetType.Elem()
		if keyType.Kind() != reflect.String {
			return reflect.Value{}, false
		}
		result := reflect.MakeMapWithSize(targetType, len(rawMap))
		for k, val := range rawMap {
			valVal := reflect.ValueOf(val)
			if valVal.Type().ConvertibleTo(elemType) {
				result.SetMapIndex(reflect.ValueOf(k), valVal.Convert(elemType))
			} else if elemType.Kind() == reflect.String {
				strVal, ok := val.(string)
				if !ok {
					return reflect.Value{}, false
				}
				result.SetMapIndex(reflect.ValueOf(k), reflect.ValueOf(strVal))
			} else {
				return reflect.Value{}, false
			}
		}
		return result, true

	default:
		valVal := reflect.ValueOf(v)
		if valVal.Type().ConvertibleTo(targetType) {
			return valVal.Convert(targetType), true
		}
		return reflect.Value{}, false
	}
}
