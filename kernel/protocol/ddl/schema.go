package ddl

import (
	"ksql/schema"
)

type (
	SchemaRestAnalysis struct{}
)

func (s SchemaRestAnalysis) Deserialize(partialQuery string) []schema.SearchField {
	return nil
}
