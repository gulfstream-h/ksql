package proto

type With struct {
	Topic           string
	ValueFormat     string
	Partitions      *int
	Replicas        *int
	Timestamp       string
	TimestampFormat string
	KeyFormat       string
}
