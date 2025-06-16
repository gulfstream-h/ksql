package ddl

import (
	"ksql/kinds"
	"ksql/ksql"
	"ksql/schema"
	"strings"
)

type (
	SchemaRestAnalysis struct{}
)

func parseCreate(partialQuery string) []schema.SearchField {

	_, partialQuery, found := strings.Cut(partialQuery, "(")
	if !found {
		return nil
	}

	partialQuery, _, found = strings.Cut(partialQuery, ")")
	if !found {
		return nil
	}

	var (
		rawFields = strings.Split(partialQuery, ",")
	)

	var (
		schemaFields = make([]schema.SearchField, 0, len(rawFields))
	)

	for _, rawField := range rawFields {
		words := strings.Split(rawField, " ")
		if len(words) < 2 {
			continue
		}

		field := schema.SearchField{
			Name: words[0],
		}

		switch strings.TrimSpace(words[1]) {
		case "VARCHAR", "STRING":
			field.Kind = kinds.String
		case "INT", "BIGINT":
			field.Kind = kinds.Int
		case "FLOAT":
			field.Kind = kinds.Float
		case "BOOL":
			field.Kind = kinds.Bool
		}

		schemaFields = append(schemaFields, field)
	}

	return schemaFields
}

func parseInsert(partialQuery string) []schema.SearchField {
	_, fields, found := strings.Cut(partialQuery, "(")
	if !found {
		return nil
	}

	fields, values, found := strings.Cut(fields, ") VALUES (")
	if !found {
		return nil
	}

	values, found = strings.CutSuffix(values, ")")
	if !found {
		return nil
	}

	fieldsSplitted := strings.Split(fields, ",")
	valuesSplitted := strings.Split(values, ",")

	if len(fieldsSplitted) != len(valuesSplitted) {
		return nil
	}

	var (
		searchFields = make([]schema.SearchField, len(fieldsSplitted))
	)

	for i := 0; i < len(fieldsSplitted); i++ {
		searchFields[i] = schema.SearchField{
			Name:  fieldsSplitted[i],
			Value: &valuesSplitted[i],
		}
	}

	return searchFields
}

func parseSelect(partialQuery string) []schema.SearchField {
	q, found := strings.CutPrefix(partialQuery, "SELECT")
	if !found {
		return nil
	}

	q, found = strings.CutPrefix(q, "FROM")
	if !found {
		return nil
	}

	rawFields := strings.Split(q, ",")

	var (
		fields = make([]schema.SearchField, 0, len(rawFields))
	)

	for _, rawField := range rawFields {
		sf := schema.SearchField{}
		alias, field, found := strings.Cut(rawField, ".")
		if !found {
			sf.Name = field
		} else {
			sf.Relation = alias
			sf.Name = field
		}

		fields = append(fields, sf)
	}

	return fields
}

func (s SchemaRestAnalysis) Deserialize(
	partialQuery string,
	kind ksql.QueryType) []schema.SearchField {

	switch kind {
	case ksql.CREATE:
		return parseCreate(partialQuery)
	case ksql.INSERT:
		return parseInsert(partialQuery)
	case ksql.SELECT:
		return parseSelect(partialQuery)
	default:
		return nil
	}
}
