package ddl

import (
	"ksql/kernel/protocol"
	"ksql/ksql"
	"regexp"
	"strings"
)

type (
	QueryRestAnalysis struct{}
)

func (qa QueryRestAnalysis) Deserialize(q string) ksql.Query {
	var (
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

	if strings.Contains(q, "DROP") {
		r.Query = ksql.DROP

		if strings.Contains(q, "STREAM") {
			r.Ref = ksql.STREAM
			q, _ = strings.CutPrefix(q, "STREAM")
		}

		if strings.Contains(q, "TABLE") {
			r.Ref = ksql.TABLE
			q, _ = strings.CutPrefix(q, "TABLE")
		}

		if strings.Contains(q, "TOPIC") {
			r.Ref = ksql.TOPIC
			q, _ = strings.CutPrefix(q, "TOPIC")
		}

		r.Name = strings.TrimSpace(q)

		return r
	}

	if strings.Contains(q, "CREATE") {
		r.Query = ksql.CREATE

		if strings.Contains(q, "AS") &&
			strings.Contains(q, "SELECT") {

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

			r.PostProcessing = regex.ReplaceAllString(q, "")

			return r
		}
	}

	if strings.Contains(q, "INSERT") {
		r.Query = ksql.INSERT
		q, _ = strings.CutPrefix(q, "INSERT INTO")
		if strings.Contains(q, "WITH") {
			name, _ := strings.CutSuffix(q, "WITH")
			r.Name = strings.TrimSpace(name)
		}

		if strings.Contains(q, "SELECT") {
			name, _ := strings.CutSuffix(q, "SELECT")
			r.Name = strings.TrimSpace(name)
			q, _ = strings.CutPrefix(q, name)
			r.PostProcessing = q
		}
	}

	regex := regexp.MustCompile(`(?is)^WITH\s+(?:.|\s)*?\)\s*`)
	result := regex.ReplaceAllString(q, "")

	cteQuery, _ := strings.CutPrefix(regex.FindString(q), "WITH")
	parseCTE(cteQuery, r.CTE)

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

	return r
}

func parseCTE(
	query string,
	cte map[string]protocol.QueryDeserializeReport) {

	cteName, _ := strings.CutSuffix(query, "AS")

	rawSchema := strings.TrimSpace(trimBetween(query, "SELECT", "FROM"))

	fromSchema := strings.TrimSpace(getNextWord(query))

	report := protocol.QueryDeserializeReport{
		Query:     ksql.SELECT,
		Name:      fromSchema,
		RawSchema: rawSchema,
	}

	if strings.Contains(query, "),") {
		report.PostProcessing = trimBetween(query, fromSchema, "),")
		query, _ = strings.CutPrefix(query, "),")

		cte[cteName] = report
		parseCTE(query, cte)

		return
	}

	if strings.Contains(query, ")") {
		report.PostProcessing = trimBetween(query, fromSchema, ")")
		query, _ = strings.CutPrefix(query, ")")

		cte[cteName] = report

		return
	}
}

func trimBetween(
	query string,
	start string,
	end string) string {

	leftTrimmed, _ := strings.CutPrefix(query, start)
	rightTrimmed, _ := strings.CutSuffix(leftTrimmed, end)

	return rightTrimmed
}

func getNextWord(query string) string {
	rawQuery, _ := strings.CutPrefix(query, "FROM")

	fields := strings.Split(rawQuery, " ")

	if len(fields) < 2 {
		return ""
	}

	return fields[1]
}
