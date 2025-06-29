package dto

// Field - field, existing in topic, that was described
type Field struct {
	Name string
	Kind string
}

// RelationDescription - filtered ksql describe response
// for table and stream relations
type RelationDescription struct {
	Name             string
	Fields           []Field
	Kind             string
	KeyFormat        string
	ValueFormat      string
	Topic            string
	Partitions       int
	Replication      int
	CreatedByCommand string
}
