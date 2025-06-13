package tests

import (
	"ksql/kernel/protocol/ddl"
	"ksql/ksql"
	"ksql/schema"
	"reflect"
	"testing"
)

func TestJoinRestAnalysis_Deserialize(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  ksql.Join
	}{
		{"INNER", "INNER JOIN reference.field2 ON field1=reference.field2",
			ksql.Join{Kind: ksql.Inner, SelectField: schema.SearchField{Name: "field1"}, JoinField: schema.SearchField{Name: "field2", Relation: "reference"}}},
		{"LEFT", "LEFT JOIN reference.field2 ON field1=reference.field2",
			ksql.Join{Kind: ksql.Left, SelectField: schema.SearchField{Name: "field1"}, JoinField: schema.SearchField{Name: "field2", Relation: "reference"}}},
		{
			"RIGHT", "RIGHT JOIN reference.field2 ON field1=reference.field2",
			ksql.Join{Kind: ksql.Right, SelectField: schema.SearchField{Name: "field1"}, JoinField: schema.SearchField{Name: "field2", Relation: "reference"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ja := ddl.JoinRestAnalysis{}
			if got := ja.Deserialize(tt.query); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Deserialize() = %v, want %v", got, tt.want)
			}
		})
	}
}
