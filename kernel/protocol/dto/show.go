package dto

type TopicInfo struct {
	Name string
}

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

type ShowStreams struct {
	Streams []RelationInfo
}

type ShowTables struct {
	Tables []RelationInfo
}
