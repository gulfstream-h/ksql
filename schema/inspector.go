package schema

import (
	"fmt"
	"ksql/kinds"
)

type (
	// SearchField - is the most important
	// field in DDL implementation. Remote kafka
	// schemas are parsed to search field and can be
	// easily deserialized from internal field to
	// insert, create, select representation
	SearchField struct {
		Name      string      // field name
		Relation  string      // stream/table name
		Kind      kinds.Ktype // internal type, describing primitive types
		Value     *string     // value to be inserted (valid only for streams)
		Tag       string
		IsPrimary bool
	}

	structFields map[string]SearchField

	LintedFields interface {
		Map() map[string]SearchField
		Array() []SearchField
		CompareWithFields(compFields []SearchField) error
		Get(name string) (SearchField, bool)
		Set(field SearchField)
	}
)

func NewLintedFields() LintedFields {
	return make(structFields)
}

func (sf structFields) CompareWithFields(compFields []SearchField) error {
	for _, field := range compFields {
		matchField, ok := sf[field.Name]
		if !ok {
			return fmt.Errorf("match for field %s not found", field.Name)
		}

		if matchField.Kind != field.Kind {
			return fmt.Errorf("kinds for field %s doesnt match", field.Name)
		}
	}

	return nil
}

func (sf structFields) Get(name string) (SearchField, bool) {
	field, ok := sf[name]
	if !ok {
		return SearchField{}, false
	}
	return field, true
}

func (sf structFields) Set(field SearchField) {
	if sf == nil {
		sf = make(structFields)
	}
	sf[field.Name] = field
}

func (sf structFields) Map() map[string]SearchField {
	return sf
}

func (sf structFields) Array() []SearchField {
	fieldsList := make([]SearchField, 0, len(sf))
	for _, value := range sf {
		fieldsList = append(fieldsList, value)
	}
	return fieldsList
}
