package protocol

import (
	"errors"
	"ksql/schema"
)

type KafkaDeserializer struct {
	SchemaAlgo    SchemaDeserializeAlgo
	SeparatorAlgo SeparatorDeserializeAlgo
	MetadataAlgo  MetadataDeserializeAlgo
}

var (
	ErrUnprocessableResponseEntity = errors.New(
		"unprocessable kafka " +
			"response entity")
)

type SchemaDeserializeAlgo interface {
	Deserialize(data []byte) ([]schema.SearchField, error)
}

type SeparatorDeserializeAlgo interface {
	Deserialize(data []byte) ([]string, error)
}

type MetadataDeserializeAlgo interface {
	Deserialize(data []byte) (map[string]any, error)
}

func ShowTopicsDeserialize() {

}
