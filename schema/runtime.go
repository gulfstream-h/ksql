package schema

import (
	"fmt"
	"reflect"
)

const (
	ksql = "ksql"
)

type SearchField struct {
	FieldName string
	Value     string
	Object    reflect.Type
	Referer   string
	Aggregate *string
	KsqlKind
}

type CompatibilityReport struct {
	CompatibilityByName bool
	CompatibilityByTag  bool
	CompatibilityByType bool
}

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

func CompareFields(
	firstField SearchField,
	secondField SearchField,
) (*CompatibilityReport, error) {
	ff, ffFound := firstField.Object.FieldByName(firstField.FieldName)
	sf, sfFound := secondField.Object.FieldByName(secondField.FieldName)

	if !ffFound {
		return nil, fmt.Errorf("field %s not found", firstField.FieldName)
	}

	if !sfFound {
		return nil, fmt.Errorf("field %s not found", secondField.FieldName)
	}

	var (
		report CompatibilityReport
	)

	if ff.Name == sf.Name {
		report.CompatibilityByName = true
	}

	if ff.Tag == sf.Tag {
		report.CompatibilityByTag = true
	}

	if ff.Type == sf.Type {
		report.CompatibilityByType = true
	}

	return &report, nil
}

func CreateRemoteSchema(fieldsList map[string]KsqlKind) reflect.Type {
	var (
		fields = make([]reflect.StructField, 0, len(fieldsList))
	)

	for name, kind := range fieldsList {
		fields = append(fields, reflect.StructField{
			Name: name,
			Type: reflect.TypeOf(getKindExample(kind)),
			Tag:  reflect.StructTag(fmt.Sprintf("%s:%s", ksql, name)),
		})
	}

	return reflect.StructOf(fields)
}

func getKindExample(kind KsqlKind) any {
	switch kind {
	case Bool:
		return true
	case Int:
		return 0
	case Float:
		return 2.71
	case String:
		return ""
	}

	return nil
}
