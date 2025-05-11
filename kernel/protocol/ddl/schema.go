package ddl

import (
	"ksql/schema"
)

type (
	SchemaRestAnalysis struct{}
)

func (s SchemaRestAnalysis) Deserialize(schema string) []schema.SearchField {
	return nil
}
