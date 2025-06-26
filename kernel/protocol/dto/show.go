package dto

type TopicInfo struct {
	Name string
}

// ShowTopics - filtered list of topics
type ShowTopics struct {
	Topics []TopicInfo
}

type RelationInfo struct {
	Name        string
	Topic       string
	KeyFormat   string
	ValueFormat string
	Windowed    bool
}

// ShowStreams - filtered list of streams
type ShowStreams struct {
	Streams []RelationInfo
}

// ShowTables - filtered list of tables
type ShowTables struct {
	Tables []RelationInfo
}
