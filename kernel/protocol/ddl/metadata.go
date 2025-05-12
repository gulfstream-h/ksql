package ddl

import "ksql/ksql"

type (
	MetadataRestAnalysis struct{}
)

func (ma MetadataRestAnalysis) Deserialize(query string) ksql.With {
	return ksql.With{}
}
