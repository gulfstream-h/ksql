package ddl

import "ksql/ksql"

type (
	JoinRestAnalysis struct{}
)

func (ja JoinRestAnalysis) Deserialize(query string) ksql.Join {
	return ksql.Join{}
}
