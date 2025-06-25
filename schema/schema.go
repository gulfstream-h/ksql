package schema

import (
	"fmt"
	"ksql/consts"
	"ksql/kinds"
	"ksql/reflector"
	"ksql/util"
	"log/slog"
)

func RemoteFieldsRepresentation(
	relationName string,
	remoteFields map[string]string,
) structFields {

	var (
		schemaFields = make(structFields)
	)

	for name, typification := range remoteFields {
		kind, ok := kinds.CastResponseTypes(typification)
		if !ok {
			slog.Debug("cannot cast field",
				"name", name, "type", typification)
			continue
		}
		schemaFields[name] = SearchField{
			Name:     name,
			Relation: relationName,
			Kind:     kind,
		}
	}

	return schemaFields
}

func NativeStructRepresentation(structure any) (structFields, error) {
	typ, err := reflector.GetType(structure)
	if err != nil {
		return nil, fmt.Errorf("cannot get reflect.Type of provided struct: %w", err)
	}

	val, err := reflector.GetValue(structure)
	if err != nil {
		return nil, fmt.Errorf("cannot get reflect.Value of provided struct: %w", err)
	}

	structName := typ.Name()

	var (
		fields = make(structFields)
	)

	for i := 0; i < typ.NumField(); i++ {
		fieldTyp := typ.Field(i)
		fieldVal := val.Field(i)

		ksqlKind, err := kinds.ToKsql(fieldTyp.Type)
		if err != nil {
			continue
		}

		tag := fieldTyp.Tag.Get(consts.KSQL)
		if tag == "" {
			continue
		}

		literalValue := util.Serialize(fieldVal.Interface())

		fields[tag] = SearchField{
			Name:     tag,
			Relation: structName,
			Kind:     ksqlKind,
			Value:    &literalValue,
		}
	}

	return fields, nil
}
