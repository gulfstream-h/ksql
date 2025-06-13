package ddl

import (
	"ksql/kernel/protocol/proto"
	"strings"
)

type (
	MetadataRestAnalysis struct{}
)

func (ma MetadataRestAnalysis) Deserialize(query string) proto.With {
	var (
		w proto.With
	)

	topic, found := strings.CutPrefix(query, "KAFKA_TOPIC=`")
	if found {
		topicParsed, found := strings.CutSuffix(topic, "`")
		if found {
			w.Topic = topicParsed
		}
	}

	vf, found := strings.CutPrefix(query, "VALUE_FORMAT=`")
	if found {
		vfParsed, found := strings.CutSuffix(vf, "`")
		if found {
			w.ValueFormat = vfParsed
		}
	}

	return w
}
