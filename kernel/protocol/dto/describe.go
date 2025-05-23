package dto

type Field struct {
	Name string
	Kind string
}

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
