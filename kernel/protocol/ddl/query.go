package ddl

import (
	"ksql/kernel/protocol"
	"ksql/ksql"
	"regexp"
	"strings"
)

type QueryRestAnalysis struct{}

func (qa QueryRestAnalysis) Deserialize(query []byte) protocol.QueryDeserializeReport {
	var (
		q = string(query)
		r protocol.QueryDeserializeReport
	)

	if strings.Contains(q, "LIST") {
		r.Query = ksql.LIST
	}

	if strings.Contains(q, "DESCRIBE") {
		r.Query = ksql.DESCRIBE
	}

	if strings.Contains(q, "CREATE") {
		r.Query = ksql.CREATE
	}

	regex := regexp.MustCompile(`(?is)^WITH\s+(?:.|\s)*?\)\s*`)
	result := regex.ReplaceAllString(q, "")

	if strings.Contains(result, "SELECT") {
		r.Query = ksql.SELECT
		fromClause, found := strings.CutPrefix("FROM", result)
		if !found {
			return r
		}
		fields := strings.Split(fromClause, " ")
		if len(fields) < 2 {
			return r
		}

		r.From = fields[1]
		return r
	}

	if strings.Contains(result, "INSERT") {
		r.Query = ksql.INSERT
	}

	return r
}
