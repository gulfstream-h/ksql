package reflection_report

import (
	"fmt"
	"ksql/schema"
	"ksql/static"
)

func ReflectionReportRemote(
	remote string,
	parsed map[string]schema.SearchField,
) error {
	remoteRelation, err := static.FindRelationFields(remote)
	if err != nil {
		return fmt.Errorf("cannot find remote relation %s: %w", remote, err)
	}

	for _, field := range parsed {
		remoteField, ok := remoteRelation[field.Name]
		if !ok {
			return fmt.Errorf("field %s not found in remote schema", field.Name)
		}

		if field.Kind != remoteField.Kind && remoteField.Kind != 0 {
			return fmt.Errorf("field %s kind mismatch: expected %s, got %s",
				field.Name, field.Kind.GetKafkaRepresentation(), remoteField.Kind.GetKafkaRepresentation())
		}

		if field.Relation != remoteField.Relation {
			return fmt.Errorf("field %s relation mismatch: expected %s, got %s",
				field.Name, field.Relation, remoteField.Relation)
		}
	}

	return nil
}

func ReflectionReportNative(
	structure any,
	parsed schema.LintedFields,
) error {
	fields, err := schema.NativeStructRepresentation(structure)
	if err != nil {
		return fmt.Errorf("cannot get native struct representation: %w", err)
	}

	for _, field := range parsed.Map() {
		nativeField, ok := fields.Get(field.Name)
		if !ok {
			return fmt.Errorf("field %s not found in native struct", field.Name)
		}

		if field.Kind != nativeField.Kind {
			return fmt.Errorf("field %s kind mismatch: expected %s, got %s",
				field.Name, field.Kind.GetKafkaRepresentation(), nativeField.Kind.GetKafkaRepresentation())
		}
	}

	return nil
}
