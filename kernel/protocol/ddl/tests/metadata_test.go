package tests

import (
	"ksql/kernel/protocol/ddl"
	"ksql/ksql"
	"reflect"
	"testing"
)

func TestMetadataRestAnalysis_Deserialize(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  ksql.With
	}{
		{
			name:  "simple metadata",
			query: "WITH KAFKA_TOPIC = 'test_topic', VALUE_FORMAT = 'JSON'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ma := ddl.MetadataRestAnalysis{}
			if got := ma.Deserialize(tt.query); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Deserialize() = %v, want %v", got, tt.want)
			}
		})
	}
}
