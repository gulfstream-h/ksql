package protocol

import "ksql/schema"

type KafkaSerializer struct {
	SchemaAlgo    SchemaSerializeAlgo
	SeparatorAlgo SeparatorSerializeAlgo
	MetadataAlgo  MetadataSerializeAlgo
}

type SchemaSerializeAlgo interface {
	Deserialize(data []byte) ([]schema.SearchField, error)
}

type SeparatorSerializeAlgo interface {
	Deserialize(data []byte) ([]string, error)
}

type MetadataSerializeAlgo interface {
	Deserialize(data []byte) (map[string]any, error)
}
