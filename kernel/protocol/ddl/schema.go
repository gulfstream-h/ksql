package ddl

import (
	"ksql/ksql"
)

type (
	SchemaRestAnalysis struct{}
)

func (s SchemaRestAnalysis) Deserialize(schema string) ksql.FullSchema {
	return ksql.FullSchema{}
}
