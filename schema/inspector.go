package schema

import (
	"errors"
	"ksql/kinds"
	"ksql/static"
	"reflect"
)

// SearchField - is the most important
// field in DDL implementation. Remote kafka
// schemas are parsed to search field and can be
// easily deserialized from internal field to
// insert, create, select representation
type SearchField struct {
	Name     string     // field name
	Relation string     // stream/table name
	Kind     kinds.Ksql // internal type, describing primitive types
	Value    *string    // value to be inserted (valid only for streams)
}

// CompareStructs - checks if two structs matches
// if some fields are missing in one of the structs
// it returns a map of common fields and a map of
// different fields
func CompareStructs(
	firstStruct reflect.Type,
	secondStruct reflect.Type) (
	map[string]bool, map[string]struct{}) {

	var (
		commonMap map[string]bool
		diffMap   map[string]struct{}
	)

	for i := 0; i < firstStruct.NumField(); i++ {
		fs := firstStruct.Field(i)
		commonMap[fs.Name] = false
	}

	for i := 0; i < secondStruct.NumField(); i++ {
		ss := secondStruct.Field(i)
		if _, exists := commonMap[ss.Name]; exists {
			commonMap[ss.Name] = true
			continue
		}

		diffMap[ss.Name] = struct{}{}
	}

	return commonMap, diffMap
}

// FindRelationFields returns the fields of a relation (stream or table) based on its name.
// It can be used for other DDL check-ups
func FindRelationFields(relationName string) (map[string]SearchField, error) {
	streamSettings, exists := static.StreamsProjections[relationName]
	if exists {
		return DeserializeStructToFieldsDictionary(relationName, streamSettings.Schema), nil
	}

	tableSettings, exists := static.TablesProjections[relationName]
	if exists {
		return DeserializeStructToFieldsDictionary(relationName, tableSettings.Schema), nil
	}

	return nil, errors.New("cannot find relation fields")
}
