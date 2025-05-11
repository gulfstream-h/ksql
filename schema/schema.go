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

func createProjection(
	fieldsList map[string]KsqlKind) reflect.Type {

	var (
		fields = make([]reflect.StructField, 0, len(fieldsList))
	)

	for name, kind := range fieldsList {
		fields = append(fields, reflect.StructField{
			Name: name,
			Type: reflect.TypeOf(getKindExample(kind)),
			Tag:  reflect.StructTag(fmt.Sprintf("%s:%s", ksqlTag, name)),
		})
	}

	return reflect.StructOf(fields)
}

func SerializeProvidedStruct[T Schema](
	schema T) map[string]KsqlKind {

	var (
		values map[string]KsqlKind
	)

	fields := structs.Fields(schema)

	for _, field := range fields {
		tag := field.Tag(ksqlTag)
		kind := field.Kind()

		ksqlKind, err := castType(kind)
		if err != nil {
			continue
		}

		values[tag] = ksqlKind
	}

	return values
}

func SerializeRemoteSchema(
	fields map[string]string) map[string]KsqlKind {

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

	return schemaFields
}
