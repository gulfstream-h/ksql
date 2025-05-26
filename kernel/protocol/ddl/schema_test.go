package ddl

import (
	"ksql/ksql"
	"ksql/schema"
	"reflect"
	"testing"
)

func TestSchemaRestAnalysis_Deserialize(t *testing.T) {
	type args struct {
		partialQuery string
		kind         ksql.QueryType
	}
	tests := []struct {
		name string
		args args
		want []schema.SearchField
	}{
		{
			"SELECT query",
			args{
				partialQuery: "SELECT name, age FROM users",
				kind:         ksql.SELECT,
			},
			[]schema.SearchField{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := SchemaRestAnalysis{}
			if got := s.Deserialize(tt.args.partialQuery, tt.args.kind); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Deserialize() = %v, want %v", got, tt.want)
			}
		})
	}
}
