package protocol

import (
	"fmt"
	"ksql/ksql"
	"ksql/schema"
	"strings"
)

type KafkaSerializer struct {
	QueryAlgo    ksql.Query
	SchemaAlgo   []schema.SearchField
	JoinAlgo     ksql.Join
	CondAlgo     ksql.Cond
	GroupBy      []schema.SearchField
	MetadataAlgo ksql.With
	CTE          map[string]KafkaSerializer
}

func (ks KafkaSerializer) writeCTE() {
	var (
		str  strings.Builder
		iter uint
	)

	for k, v := range ks.CTE {
		if iter == 0 {
			str.WriteString("WITH ")
		} else {
			str.WriteString(",")
		}
		str.WriteString(k)
		str.WriteString(" AS (")
		str.WriteString(v.Query())
		str.WriteString(")")
		iter++
	}
}

func (ks KafkaSerializer) writeQuery() {
	var (
		str strings.Builder
	)

	switch ks.QueryAlgo.Query {
	case ksql.LIST:
		str.WriteString("LIST ")

		switch ks.QueryAlgo.Ref {
		case ksql.TOPIC:
			str.WriteString("TOPICS;")
		case ksql.STREAM:
			str.WriteString("STREAMS;")
		case ksql.TABLE:
			str.WriteString("TABLES;")
		}
	case ksql.DESCRIBE:
		str.WriteString("DESCRIBE ")
		str.WriteString(ks.QueryAlgo.Name)
		str.WriteString(";")
	case ksql.DROP:
		str.WriteString("DROP ")

		switch ks.QueryAlgo.Ref {
		case ksql.TOPIC:
			str.WriteString("TOPIC ")
		case ksql.STREAM:
			str.WriteString("STREAM ")
		case ksql.TABLE:
			str.WriteString("TABLE ")
		}

		str.WriteString(ks.QueryAlgo.Name)
		str.WriteString(";")
	case ksql.CREATE:
		str.WriteString("CREATE ")

		switch ks.QueryAlgo.Ref {
		case ksql.STREAM:
			str.WriteString("STREAM ")
			str.WriteString(ks.QueryAlgo.Name)
		case ksql.TABLE:
			str.WriteString("TABLE ")
			str.WriteString(ks.QueryAlgo.Name)
		default:
			return
		}

	case ksql.SELECT:
		str.WriteString("SELECT %s FROM ")
		str.WriteString(ks.QueryAlgo.Name)
	case ksql.INSERT:
		str.WriteString("INSERT INTO ")
		str.WriteString(ks.QueryAlgo.Name)
	}
}

func (ks KafkaSerializer) writeSchema() {
	var (
		str  strings.Builder
		iter uint8
	)

	switch ks.QueryAlgo.Query {
	case ksql.SELECT:
		for _, field := range ks.SchemaAlgo {
			if iter != 0 {
				str.WriteString(",")
			}

			str.WriteString(field.Referer)
			str.WriteString(".")
			str.WriteString(field.FieldName)
			iter++
		}

		return
	case ksql.CREATE:
		for _, field := range ks.SchemaAlgo {
			if iter != 0 {
				str.WriteString("(")
			} else {
				str.WriteString(",")
			}

			str.WriteString(field.FieldName)
			str.WriteString(" ")
			str.WriteString(field.KsqlKind.Marshal())

			iter++
		}
	case ksql.INSERT:
		var (
			fields, values []string
		)

		for _, field := range ks.SchemaAlgo {
			fields = append(fields, field.FieldName)
			values = append(values, field.Value)
		}

		str.WriteString(" (")
		str.WriteString(strings.Join(fields, ","))
		str.WriteString(") ")

		str.WriteString(" VALUES (")
		str.WriteString(strings.Join(values, ","))
		str.WriteString(")")
	default:
		return
	}
}

func (ks KafkaSerializer) writeJoin() {
	var (
		str strings.Builder
	)

	switch ks.JoinAlgo.Kind {
	case ksql.Left:
		str.WriteString(" LEFT JOIN ON ")
	case ksql.Inner:
		str.WriteString(" INNER JOIN ON ")
	case ksql.Right:
		str.WriteString(" RIGHT JOIN ON ")
	}

	sf := ks.JoinAlgo.SelectField
	jf := ks.JoinAlgo.JoinField

	if sf.KsqlKind != jf.KsqlKind {
		return
	}

	if !sf.CheckJoinCapability() ||
		!jf.CheckJoinCapability() {
		return
	}

	str.WriteString(sf.Referer)
	str.WriteString(".")
	str.WriteString(sf.FieldName)

	str.WriteString("=")

	str.WriteString(jf.Referer)
	str.WriteString(".")
	str.WriteString(jf.FieldName)
}

func (ks KafkaSerializer) writeCond() {
	var (
		str strings.Builder
	)

	for i, field := range ks.CondAlgo.WhereClause {
		if i == 0 {
			str.WriteString(" WHERE ")
		} else {
			str.WriteString(" AND ")
		}

		str.WriteString("")
	}

	for i, field := range ks.GroupBy {
		if i == 0 {
			str.WriteString(" GROUP BY ")
		} else {
			str.WriteString(",")
		}

		str.WriteString("")
	}

	for i, field := range ks.CondAlgo.HavingClause {
		if i == 0 {
			str.WriteString(" HAVING ")
		} else {
			str.WriteString(" AND ")
		}

		str.WriteString("")
	}
}

func (ks KafkaSerializer) writeMeta() {
	var (
		str   strings.Builder
		parts []string
	)

	if ks.MetadataAlgo.Topic != "" {
		parts = append(parts, fmt.Sprintf("KAFKA_TOPIC = '%s'", ks.MetadataAlgo.Topic))
	}
	if ks.MetadataAlgo.ValueFormat != "" {
		parts = append(parts, fmt.Sprintf("VALUE_FORMAT = '%s'", ks.MetadataAlgo.ValueFormat))
	}
	if ks.MetadataAlgo.KeyFormat != "" {
		parts = append(parts, fmt.Sprintf("KEY_FORMAT = '%s'", ks.MetadataAlgo.KeyFormat))
	}
	if ks.MetadataAlgo.Partitions != nil {
		parts = append(parts, fmt.Sprintf("PARTITIONS = %d", *ks.MetadataAlgo.Partitions))
	}
	if ks.MetadataAlgo.Replicas != nil {
		parts = append(parts, fmt.Sprintf("REPLICAS = %d", *ks.MetadataAlgo.Replicas))
	}
	if ks.MetadataAlgo.Timestamp != "" {
		parts = append(parts, fmt.Sprintf("TIMESTAMP = '%s'", ks.MetadataAlgo.Timestamp))
	}
	if ks.MetadataAlgo.TimestampFormat != "" {
		parts = append(parts, fmt.Sprintf("TIMESTAMP_FORMAT = '%s'", ks.MetadataAlgo.TimestampFormat))
	}

	str.WriteString("WITH (\n  ")
	str.WriteString(strings.Join(parts, ",\n  "))
	str.WriteString("\n);")
}

func (ks KafkaSerializer) Query() string {
	ks.writeCTE()
	ks.writeQuery()
	ks.writeSchema()
	ks.writeJoin()
	ks.writeJoin()
	ks.writeCond()
	ks.writeMeta()

	return ""
}
