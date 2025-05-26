package ddl

import (
	"ksql/schema"
	"reflect"
	"testing"
)

func TestGroupRestAnalysis_Deserialize(t *testing.T) {
	tests := []struct {
		name string
		args string
		want []schema.SearchField
	}{
		{
			"single arg",
			"GROUP BY name, age",
			[]schema.SearchField{
				{Name: "name", Relation: ""},
				{Name: "age", Relation: ""},
			},
		},
		{
			"mixed arg",
			"GROUP BY name, access_rules.age",
			[]schema.SearchField{
				{Name: "name", Relation: ""},
				{Name: "age", Relation: "access_rules"},
			},
		},
		{
			"multiply arg",
			"GROUP BY users.name, access_rules.age",
			[]schema.SearchField{
				{Name: "name", Relation: "users"},
				{Name: "age", Relation: "access_rules"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ga := GroupRestAnalysis{}
			if got := ga.Deserialize(tt.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Deserialize() = %v, want %v", got, tt.want)
			}
		})
	}
}
