package report

import (
	"fmt"
	"github.com/gulfstream-h/ksql/internal/schema"
	"github.com/gulfstream-h/ksql/static"
)

// ReflectionReportRemote - compares in-cache
// describe structure with collected fields
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

		if field.Kind != remoteField.Kind && field.Kind != 0 {
			return fmt.Errorf("field %s kind mismatch: expected %s, got %s",
				field.Name, remoteField.Kind.GetKafkaRepresentation(), field.Kind.GetKafkaRepresentation())
		}

		if field.Relation != remoteField.Relation {
			return fmt.Errorf("field %s relation mismatch: expected %s, got %s",
				field.Name, remoteField.Relation, field.Relation)
		}
	}

	return nil
}

// ReflectionReportNative - compares custom
// structure with builder collected fields
// and returns error on fields mismatch
func ReflectionReportNative(
	structure any,
	parsed schema.LintedFields,
) error {
	fields, err := schema.NativeStructRepresentation("", structure)
	if err != nil {
		return fmt.Errorf("cannot get native struct representation: %w", err)
	}

	for _, field := range parsed.Map() {
		nativeField, ok := fields.Get(field.Name)
		if !ok {
			return fmt.Errorf("field %s not found in native struct", field.Name)
		}

		if field.Kind != nativeField.Kind && field.Kind != 0 {
			return fmt.Errorf("field %s kind mismatch: expected %s, got %s",
				field.Name, nativeField.Kind.GetKafkaRepresentation(), field.Kind.GetKafkaRepresentation())
		}
	}

	return nil
}
