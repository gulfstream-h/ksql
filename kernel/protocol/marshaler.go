package protocol

import "ksql/schema"

type KafkaSerializer struct {
	SchemaAlgo    SchemaSerializeAlgo
	SeparatorAlgo SeparatorSerializeAlgo
	MetadataAlgo  MetadataSerializeAlgo
}

type SchemaSerializeAlgo interface {
	Serialize(data []byte) ([]schema.SearchField, error)
}

type SeparatorSerializeAlgo interface {
	Serialize(data []byte) ([]string, error)
}

type MetadataSerializeAlgo interface {
	Serialize(data []byte) (map[string]any, error)
}
