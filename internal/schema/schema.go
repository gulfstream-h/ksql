package schema

import (
	"errors"
	"fmt"
	"ksql/consts"
	"ksql/internal/reflector"
	"ksql/internal/util"
	"ksql/kinds"
	"log/slog"
	"strings"
)

// RemoteFieldsRepresentation - function that parse
// ksql describe fields into implicit internal representation
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

// NativeStructRepresentation - function, that parse
// client provided structure into implicit internal representation
func NativeStructRepresentation(
	relationName string,
	structure any,
) (LintedFields, error) {
	typ, err := reflector.GetType(structure)
	if err != nil {
		return nil, fmt.Errorf("cannot get reflect.Type of provided struct: %w", err)
	}

	val, err := reflector.GetValue(structure)
	if err != nil {
		return nil, fmt.Errorf("cannot get reflect.Value of provided struct: %w", err)
	}

	var (
		fields     = make(structFields)
		hasPrimary = false
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

		tag, isPrimaryField := isPrimary(tag)
		if isPrimaryField {
			if hasPrimary {
				return nil, errors.New("event must contain only one primary key")
			}
			hasPrimary = true
		}

		literalValue := util.Serialize(fieldVal.Interface())

		fields[tag] = SearchField{
			Name:      tag,
			Relation:  relationName,
			Kind:      ksqlKind,
			Value:     &literalValue,
			IsPrimary: isPrimaryField,
		}
	}

	return fields, nil
}

// isPrimary - return field with primary key
// if field tag contains primary keyword
func isPrimary(tag string) (string, bool) {
	return strings.CutSuffix(tag, ", primary")
}
