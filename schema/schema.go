package schema

import (
	"fmt"
	"github.com/fatih/structs"
	"reflect"
)

type (
	Schema interface{}
)

const (
	ksqlTag = "ksql"
)

func SerializeFields(
	fieldsList []SearchField) reflect.Type {

	var (
		fields = make([]reflect.StructField, 0, len(fieldsList))
	)

	for _, field := range fieldsList {
		fields = append(fields, reflect.StructField{
			Name: field.FieldName,
			Type: reflect.TypeOf(field.KsqlKind.example()),
			Tag:  reflect.StructTag(fmt.Sprintf("%s:%s", ksqlTag, field.FieldName)),
		})
	}

	return reflect.StructOf(fields)
}

func SerializeProvidedStruct[T Schema](
	schema T) reflect.Type {

	var (
		values map[string]KsqlKind
	)

	fields := structs.Fields(schema)

	for _, field := range fields {
		tag := field.Tag(ksqlTag)
		kind := field.Kind()

		ksqlKind, err := Ksql(kind)
		if err != nil {
			continue
		}

		values[tag] = ksqlKind
	}

	return createProjection(values)
}

func SerializeRemoteSchema(
	fields map[string]string) reflect.Type {

	var (
		schemaFields = make(map[string]KsqlKind)
	)

	for k, v := range fields {
		switch v {
		case "INT":
			schemaFields[k] = Int
		case "FLOAT":
			schemaFields[k] = Float
		case "VARCHAR":
			schemaFields[k] = String
		case "BOOL":
			schemaFields[k] = Bool
		}
	}

	return createProjection(schemaFields)
}

func createProjection(
	fieldsList map[string]KsqlKind) reflect.Type {

	var (
		fields = make([]reflect.StructField, 0, len(fieldsList))
	)

	for name, kind := range fieldsList {
		fields = append(fields, reflect.StructField{
			Name: name,
			Type: reflect.TypeOf(kind.example()),
			Tag:  reflect.StructTag(fmt.Sprintf("%s:%s", ksqlTag, name)),
		})
	}

	return reflect.StructOf(fields)
}
