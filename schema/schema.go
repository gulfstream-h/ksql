package schema

import (
	"fmt"
	"ksql/kinds"
	"ksql/static"
	"ksql/util"
	"log/slog"
	"reflect"
	"regexp"
	"strings"
)

type Ident struct {
	RelationLabel string
	KType         kinds.Ktype
}

// SerializeFieldsToStruct - creates struct from composition
// of fields. Can be used for new schemas creation
func SerializeFieldsToStruct(
	fieldsList []SearchField) reflect.Type {

	var (
		fields = make([]reflect.StructField, 0, len(fieldsList))
	)

	for _, field := range fieldsList {
		fields = append(fields, reflect.StructField{
			Name: field.Name,
			Type: reflect.TypeOf(field.Kind.Example()),
			Tag: reflect.StructTag(fmt.Sprintf("%s:%s",
				static.KSQL, field.Name)),
		})
	}

	return reflect.StructOf(fields)
}

// ParseStructToFieldsDictionary - returns map of SearchField
// from in-code serialized struct or from runtime generated structure
// current fields describes all required info for DDL
// it can be useful for faster search & comparison in structs
func ParseStructToFieldsDictionary(
	structName string,
	runtimeStruct reflect.Type,
) map[string]SearchField {

	var (
		fields = make(map[string]SearchField)
	)

	for i := 0; i < runtimeStruct.NumField(); i++ {
		field := runtimeStruct.Field(i)

		ksqlKind, err := kinds.ToKsql(field.Type)
		if err != nil {
			continue
		}

		tag, found := strings.CutPrefix(string(field.Tag), "ksql:")
		if !found {
			return nil
		}

		fields[tag] = SearchField{
			Name:     tag,
			Relation: structName,
			Kind:     ksqlKind,
		}
	}

	return fields

}

// ParseStructToFields - returns array of SearchField
// from user-provided struct
// current fields describes all required info for DDL
func ParseStructToFields(name string, runtimeStruct any) []SearchField {
	var (
		fields []SearchField
	)

	if _, ok := runtimeStruct.([]any); ok {
		slog.Debug("runtimeStruct cannot be slice")
		return nil
	}

	if _, ok := runtimeStruct.(reflect.Type); ok {
		slog.Debug("runtimeStruct is already reflect type")
		return nil
	}

	structType := reflect.TypeOf(runtimeStruct)
	val := reflect.ValueOf(runtimeStruct)

	for i := 0; i < structType.NumField(); i++ {
		fieldType := structType.Field(i)
		fieldVal := val.Field(i)

		ksqlKind, err := kinds.ToKsql(fieldType.Type)
		if err != nil {
			continue
		}

		taggedName := fieldType.Tag.Get("ksql")
		serializedVal := util.Serialize(fieldVal.Interface())

		var tag string

		if fieldType.Tag != "" {
			tag, _ = strings.CutPrefix(string(fieldType.Tag), "ksql:")
		}

		fields = append(fields, SearchField{
			Name:     taggedName,
			Relation: name,
			Kind:     ksqlKind,
			Value:    &serializedVal,
			Tag:      tag,
		})
	}

	return fields
}

// ParseReflectStructToFields - returns array of SearchField
// from in-code serialized struct or from runtime generated structure
// current fields describes all required info for DDL
func ParseReflectStructToFields(
	structName string,
	runtimeStruct reflect.Type,
) []SearchField {

	var (
		fields []SearchField
	)

	for i := 0; i < runtimeStruct.NumField(); i++ {
		field := runtimeStruct.Field(i)

		ksqlKind, err := kinds.ToKsql(field.Type)
		if err != nil {
			continue
		}

		fields = append(fields, SearchField{
			Name:     field.Name,
			Relation: structName,
			Kind:     ksqlKind,
			Tag:      field.Name,
		})
	}

	return fields
}

// SerializeProvidedStruct - casts user defined(generic) struct
// to reflect structure, so it can be compared to runtime ones
func SerializeProvidedStruct(
	schema any) reflect.Type {

	var (
		values = make(map[string]Ident)
	)

	typ := reflect.TypeOf(schema)

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		ident := Ident{}

		ksqlKind, err := kinds.ToKsql(field.Type)
		if err != nil {
			continue
		}

		tags := strings.Split(field.Tag.Get(static.KSQL), ",")

		if len(tags) == 0 {
			continue
		}

		if len(tags) == 2 {
			ident.RelationLabel = tags[1]
		}

		ident.KType = ksqlKind

		values[strings.ToUpper(tags[0])] = ident

	}

	return createProjection(values)
}

// SerializeRemoteSchema - casts http ksql description
// of stream/table to reflect struct
func SerializeRemoteSchema(
	fields map[string]string) reflect.Type {

	var (
		schemaFields = make(
			map[string]Ident,
		)
	)

	for k, v := range fields {
		switch v {
		case "INT", "INTEGER":
			schemaFields[k] = Ident{KType: kinds.Int}
		case "DOUBLE":
			schemaFields[k] = Ident{KType: kinds.Double}
		case "VARCHAR", "STRING":
			schemaFields[k] = Ident{KType: kinds.String}
		case "BOOL":
			schemaFields[k] = Ident{KType: kinds.Bool}
		case "BYTES":
			schemaFields[k] = Ident{KType: kinds.Bytes}
		case "BIGINT":
			schemaFields[k] = Ident{KType: kinds.BigInt}
		case "ARRAY<INT>":
			schemaFields[k] = Ident{KType: kinds.ArrInt}
		case "ARRAY<DOUBLE>":
			schemaFields[k] = Ident{KType: kinds.ArrDouble}
		case "ARRAY<VARCHAR>", "ARRAY<STRING>":
			schemaFields[k] = Ident{KType: kinds.ArrString}
		case "ARRAY<BOOL>":
			schemaFields[k] = Ident{KType: kinds.ArrBool}
		case "ARRAY<BYTES>":
			schemaFields[k] = Ident{KType: kinds.ArrBytes}
		case "ARRAY<BIGINT>":
			schemaFields[k] = Ident{KType: kinds.ArrBigInt}
		case "MAP<VARCHAR, INT>", "MAP<STRING, INT>":
			schemaFields[k] = Ident{KType: kinds.MapInt}
		case "MAP<VARCHAR, DOUBLE>", "MAP<STRING, DOUBLE>":
			schemaFields[k] = Ident{KType: kinds.MapDouble}
		case "MAP<VARCHAR, VARCHAR>", "MAP<VARCHAR, STRING>",
			"MAP<STRING, VARCHAR>", "MAP<STRING, STRING>":
			schemaFields[k] = Ident{KType: kinds.MapString}
		case "MAP<VARCHAR, BOOL>, MAP<STRING, BOOL>":
			schemaFields[k] = Ident{KType: kinds.MapBool}
		case "MAP<VARCHAR, BYTES>, MAP<STRING, BYTES>":
			schemaFields[k] = Ident{KType: kinds.MapBytes}
		case "MAP<VARCHAR, BIGINT>, MAP<STRING, BIGINT>":
			schemaFields[k] = Ident{KType: kinds.MapBigInt}
		default:
			slog.Warn("Unsupported type in schema", "field", k, "type", v)
			return nil
		}
	}

	return createProjection(schemaFields)
}

// createProjection - defines reflect structure from map[string]kinds.Ktype declaration
// current structure is comparable. Can be invoked, parsed and cloned
func createProjection(
	fieldsList map[string]Ident) reflect.Type {

	var (
		fields = make([]reflect.StructField, 0, len(fieldsList))
	)

	for name, kind := range fieldsList {
		var tag reflect.StructTag = reflect.StructTag(fmt.Sprintf("%s:%s", static.KSQL, name))

		if kind.RelationLabel != "" {
			tag = reflect.StructTag(fmt.Sprintf("%s:%s", static.KSQL, name+","+kind.RelationLabel))
		}

		fields = append(fields, reflect.StructField{
			Name: name,
			Type: reflect.TypeOf(kind.KType.Example()),
			Tag:  tag,
		})
	}

	return reflect.StructOf(fields)
}

func ParseHeadersAndValues(headers string, values []any) (map[string]any, error) {
	var (
		parts []string
	)

	re := regexp.MustCompile("`[^`]+`\\s+\\w+(?:<[^>]+>)?")

	matches := re.FindAllString(headers, -1)

	for _, m := range matches {
		fmt.Println("Found match:", m)
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

func NormalizeValue(v interface{}, targetType reflect.Type) (reflect.Value, bool) {
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
