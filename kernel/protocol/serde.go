package protocol

import (
	jsoniter "github.com/json-iterator/go"
	"ksql/kernel/protocol/dao"
)

type RestDeserializer struct{}

func (rs RestDeserializer) Deserialize() {

}

func deserializeShowTopicsResponse(
	buffer []byte) (
	dao.ShowTopics, error) {

	var result dao.ShowTopics
	if err := jsoniter.Unmarshal(buffer, &result); err != nil {
		return dao.ShowTopics{}, err
	}
	return result, nil
}

func deserializeDescribeMyStream(buffer []byte) {

}
