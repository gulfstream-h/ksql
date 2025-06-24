package schema

import (
	"errors"
	"fmt"
	"ksql/kinds"
	"ksql/reflector"
	"ksql/shared"
	"ksql/static"
	"log/slog"
)

func RemoteFieldsRepresentation(
	relationName string,
	remoteFields map[string]string,
) LintedFields {

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

func NativeStructRepresentation(structure any) (LintedFields, error) {
	typ, err := reflector.GetType(structure)
	if err != nil {
		return nil, fmt.Errorf("cannot get reflect.Type of provided struct: %w", err)
	}

	structName := typ.Name()

	var (
		fields = make(structFields)
	)

	for i := 0; i < typ.NumField(); i++ {
		fieldTyp := typ.Field(i)

		ksqlKind, err := kinds.ToKsql(fieldTyp.Type)
		if err != nil {
			continue
		}

		tag := fieldTyp.Tag.Get(static.KSQL)
		if tag == "" {
			continue
		}

		fields[tag] = SearchField{
			Name:     tag,
			Relation: structName,
			Kind:     ksqlKind,
		}
	}

	return fields, nil
}

// FindRelationFields returns the fields of a relation (stream or table) based on its name.
// It can be used for other DDL check-ups
func FindRelationFields(relationName string) (LintedFields, error) {
	streamSettings, exists := static.StreamsProjections.Load(relationName)
	if exists {
		settings, ok := streamSettings.(shared.StreamSettings)
		if !ok {
			return nil, errors.New("invalid map values have been inserted")
		}
		return settings.Schema, nil
	}

	tableSettings, exists := static.TablesProjections.Load(relationName)
	if exists {
		settings, ok := tableSettings.(shared.TableSettings)
		if !ok {
			return nil, errors.New("invalid map values have been inserted")
		}
		return settings.Schema, nil
	}

	return nil, errors.New("cannot find relation fields")
}
