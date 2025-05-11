package schema

import (
	"ksql/proxy"
	"reflect"
)

type SearchField struct {
	FieldName string
	Value     string
	Referer   string
	KsqlKind
}

func GetTypeFields(
	name string,
	remoteStruct reflect.Type) []SearchField {

	var (
		fields []SearchField
	)

	for i := 0; i < remoteStruct.NumField(); i++ {
		fs := remoteStruct.Field(i)

		ksqlKind, err := Ksql(fs.Type.Kind())
		if err != nil {
			continue
		}

		fields = append(fields, SearchField{
			FieldName: fs.Tag.Get(ksqlTag),
			Referer:   name,
			KsqlKind:  ksqlKind,
		})
	}

	return fields
}

func GetSchemeFields(
	name string,
	kind ResourceKind) []SearchField {

	switch kind {
	case STREAM:
		stream, err := proxy.FindStreamSettings(name)
		if err != nil {
			return nil
		}

		return GetTypeFields(stream.Name, stream.Schema)
	case TABLE:
		table, err := proxy.FindTableSettings(name)
		if err != nil {
			return nil
		}

		GetTypeFields(table.Name, table.Schema)
	}

	return nil
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

type (
	CompatibilityReport struct {
		CompatibilityByTag  bool
		CompatibilityByType bool
	}
)

func CompareFields(
	ff SearchField,
	sf SearchField,
) (*CompatibilityReport, error) {

	var (
		report CompatibilityReport
	)

	if ff.FieldName == sf.FieldName {
		report.CompatibilityByTag = true
	}

	if ff.KsqlKind == sf.KsqlKind {
		report.CompatibilityByType = true
	}

	return &report, nil
}

func GetTypeFieldsAsMap(
	name string,
	remoteStruct reflect.Type) map[string]SearchField {

	var (
		fields map[string]SearchField
	)

	for i := 0; i < remoteStruct.NumField(); i++ {
		fs := remoteStruct.Field(i)

		ksqlKind, err := Ksql(fs.Type.Kind())
		if err != nil {
			continue
		}

		fields[fs.Name] = SearchField{
			FieldName: fs.Tag.Get(ksqlTag),
			Referer:   name,
			KsqlKind:  ksqlKind,
		}
	}

	return fields
}
