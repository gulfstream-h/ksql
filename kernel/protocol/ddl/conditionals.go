package ddl

import "ksql/ksql"

type (
	CondRestAnalysis struct{}
)

func (ca CondRestAnalysis) Deserialize(query string) ksql.Cond {
	return ksql.Cond{}
}
