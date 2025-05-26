package ddl

import (
	"ksql/ksql"
	"reflect"
	"testing"
)

func TestQueryRestAnalysis_Deserialize(t *testing.T) {
	type args struct {
		partialQuery string
		queryType    ksql.QueryType
	}
	tests := []struct {
		name string
		args args
		want ksql.Query
	}{
		{
			"SELECT query",
			args{
				partialQuery: "SELECT * FROM users",
				queryType:    ksql.SELECT,
			},
			ksql.Query{
				Query: ksql.SELECT,
				Ref:   ksql.STREAM,
				Name:  "users",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qa := QueryRestAnalysis{}
			if got := qa.Deserialize(tt.args.partialQuery, tt.args.queryType); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Deserialize() = %v, want %v", got, tt.want)
			}
		})
	}
}
