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

		if strings.Contains(q, "STREAMS") {
			r.Ref = ksql.STREAM
		}

		if strings.Contains(q, "TABLES") {
			r.Ref = ksql.TABLE
		}

		if strings.Contains(q, "TOPICS") {
			r.Ref = ksql.TOPIC
		}

		return r
	}

	if strings.Contains(q, "DESCRIBE") {
		r.Query = ksql.DESCRIBE
		words := strings.Split(q, " ")
		if len(words) < 2 {
			return r
		}
		r.Name = words[1]

		return r
	}

	if strings.Contains(q, "CREATE") {
		r.Query = ksql.CREATE

		regex := regexp.MustCompile(`(?i)\bCREATE\s+(STREAM|TABLE)\s+\w+\s+AS\b`)

		createQuery := regex.FindString(q)

		if strings.Contains(createQuery, "STREAM") {
			r.Ref = ksql.STREAM
			createQuery, _ = strings.CutPrefix(createQuery, "STREAM")
			createQuery, _ = strings.CutSuffix(createQuery, "AS")
		}

		if strings.Contains(createQuery, "TABLE") {
			r.Ref = ksql.TABLE
			createQuery, _ = strings.CutPrefix(createQuery, "TABLE")
			createQuery, _ = strings.CutSuffix(createQuery, "AS")
		}

		r.Name = strings.TrimSpace(createQuery)

		return r
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

		r.Name = fields[1]

		return r
	}

	if strings.Contains(result, "INSERT") {
		r.Query = ksql.INSERT
	}

	return r
}
