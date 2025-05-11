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

func GetSchemeFields(
	name string,
	kind ResourceKind) []SearchField {

	var (
		fields []SearchField
	)

	switch kind {
	case STREAM:
		stream, err := proxy.FindStreamSettings(name)
		if err != nil {
			return nil
		}

		for i := 0; i < stream.Schema.NumField(); i++ {
			fs := stream.Schema.Field(i)

			ksqlKind, err := castType(fs.Type.Kind())
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

	case TABLE:
		table, err := proxy.FindTableSettings(name)
		if err != nil {
			return nil
		}

		for i := 0; i < table.Schema.NumField(); i++ {
			fs := table.Schema.Field(i)

			ksqlKind, err := castType(fs.Type.Kind())
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
