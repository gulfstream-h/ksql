package protocol

import "ksql/schema"

type KafkaDeserializer struct {
	SchemaAlgo    SchemaDeserializeAlgo
	SeparatorAlgo SeparatorDeserializeAlgo
	MetadataAlgo  MetadataDeserializeAlgo
}

type SchemaDeserializeAlgo interface {
	Deserialize(data []byte) ([]schema.SearchField, error)
}

type SeparatorDeserializeAlgo interface {
	Deserialize(data []byte) ([]string, error)
}

type MetadataDeserializeAlgo interface {
	Deserialize(data []byte) (map[string]any, error)
}
