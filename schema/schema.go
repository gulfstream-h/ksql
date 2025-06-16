package schema

import (
	"fmt"
	"github.com/fatih/structs"
	"ksql/kinds"
	"ksql/static"
	"ksql/util"
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

		ksqlKind, err := kinds.ToKsql(field.Type.Kind())
		if err != nil {
			continue
		}

		tag := field.Tag.Get(static.KSQL)

		fields[tag] = SearchField{
			Name:     tag,
			Relation: structName,
			Kind:     ksqlKind,
		}
	}

	return fields

}

// ParseStructToFields - returns array of SearchField
// from in-code serialized struct or from runtime generated structure
// current fields describes all required info for DDL
func ParseStructToFields(
	structName string,
	runtimeStruct any,
) []SearchField {

	var (
		fields []SearchField
	)

	structType := reflect.TypeOf(runtimeStruct)
	val := reflect.ValueOf(runtimeStruct)

	for i := 0; i < structType.NumField(); i++ {
		fieldType := structType.Field(i)
		fieldVal := val.Field(i)

		ksqlKind, err := kinds.ToKsql(fieldType.Type.Kind())
		if err != nil {
			continue
		}


		taggedName := fieldType.Tag.Get("ksql")
		serializedVal := util.Serialize(fieldVal.Interface())

		var tag string

		if field.Tag != "" {
			tag, _ = strings.CutPrefix(string(field.Tag), "ksql:")
		}

		fields = append(fields, SearchField{
			Name:     taggedName,
			Relation: structName,
			Kind:     ksqlKind,
			Value:    &serializedVal,
			Tag:      tag,
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

	fields := structs.Fields(schema)

	for _, field := range fields {
		ident := Ident{}
		fmt.Println(field.Name())
		tag := strings.Split(field.Tag(static.KSQL), ",")

		if len(tag) == 2 {
			ident.RelationLabel = tag[1]
		}

		kind := field.Kind()

		ksqlKind, err := kinds.ToKsql(kind)
		if err != nil {
			continue
		}

		ident.KType = ksqlKind

		values[strings.ToUpper(tag[0])] = ident
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
		case "FLOAT":
			schemaFields[k] = Ident{KType: kinds.Float}
		case "VARCHAR", "STRING":
			schemaFields[k] = Ident{KType: kinds.String}
		case "BOOL":
			schemaFields[k] = Ident{KType: kinds.Bool}
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
		var tag reflect.StructTag

		if kind.RelationLabel != "" {
			tag = reflect.StructTag(fmt.Sprintf("%s:%s", static.KSQL, kind.RelationLabel))
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
	parts := strings.Split(headers, ",")

	result := make(map[string]any)

	re := regexp.MustCompile("`([^`]*)`")

	if len(parts) != len(values) {
		return nil, fmt.Errorf("headers and values count mismatch")
	}

	for i, part := range parts {
		match := re.FindStringSubmatch(part)
		if len(match) < 2 {
			return nil, fmt.Errorf("invalid header format: %s", part)
		}

		result[match[1]] = values[i]
	}

	return result, nil
}
