package schema

import (
	"fmt"
	"github.com/fatih/structs"
	"ksql/kinds"
	"ksql/static"
	"reflect"
	"strings"
)

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
	runtimeStruct reflect.Type,
) []SearchField {

	var (
		fields []SearchField
	)

	for i := 0; i < runtimeStruct.NumField(); i++ {
		field := runtimeStruct.Field(i)

		ksqlKind, err := kinds.ToKsql(field.Type.Kind())
		if err != nil {
			continue
		}

		fields = append(fields, SearchField{
			Name:     field.Name,
			Relation: structName,
			Kind:     ksqlKind,
		})
	}

	return fields
}

// SerializeProvidedStruct - casts user defined(generic) struct
// to reflect structure, so it can be compared to runtime ones
func SerializeProvidedStruct(
	schema any) reflect.Type {

	var (
		values = make(map[string]kinds.Ktype)
	)

	fields := structs.Fields(schema)

	for _, field := range fields {
		fmt.Println(field.Name())
		tag := field.Tag(static.KSQL)
		kind := field.Kind()

		ksqlKind, err := kinds.ToKsql(kind)
		if err != nil {
			continue
		}

		values[strings.ToUpper(tag)] = ksqlKind
	}

	return createProjection(values)
}

// SerializeRemoteSchema - casts http ksql description
// of stream/table to reflect struct
func SerializeRemoteSchema(
	fields map[string]string) reflect.Type {

	var (
		schemaFields = make(
			map[string]kinds.Ktype,
		)
	)

	for k, v := range fields {
		switch v {
		case "INT", "INTEGER":
			schemaFields[k] = kinds.Int
		case "FLOAT":
			schemaFields[k] = kinds.Float
		case "VARCHAR", "STRING":
			schemaFields[k] = kinds.String
		case "BOOL":
			schemaFields[k] = kinds.Bool
		}
	}

	return createProjection(schemaFields)
}

// createProjection - defines reflect structure from map[string]kinds.Ktype declaration
// current structure is comparable. Can be invoked, parsed and cloned
func createProjection(
	fieldsList map[string]kinds.Ktype) reflect.Type {

	var (
		fields = make([]reflect.StructField, 0, len(fieldsList))
	)

	fmt.Println(fieldsList)

	for name, kind := range fieldsList {
		fields = append(fields, reflect.StructField{
			Name: name,
			Type: reflect.TypeOf(kind.Example()),
			Tag:  reflect.StructTag(fmt.Sprintf("%s:%s", static.KSQL, name)),
		})
	}

	return reflect.StructOf(fields)
}
