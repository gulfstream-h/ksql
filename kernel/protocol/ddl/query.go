package ddl

import (
	"ksql/ksql"
)

type (
	QueryRestAnalysis struct{}
)

func (qa QueryRestAnalysis) Deserialize(partialQuery string) ksql.Query {
	return ksql.Query{}
}
