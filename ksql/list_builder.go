package ksql

type (
	ListBuilder interface {
		Expression() (string, bool)
		Type() Reference
	}

	list struct {
		typ Reference
	}
)

func List(typ Reference) ListBuilder {
	return &list{
		typ: typ,
	}
}

func (l *list) Expression() (string, bool) {
	var operation string

	switch l.typ {
	case STREAM:
		operation = "LIST STREAMS"
	case TABLE:
		operation = "LIST TABLES"
	case TOPIC:
		operation = "LIST TOPICS"
	default:
		return "", false
	}

	return operation, true
}

func (l *list) Type() Reference {
	return l.typ
}
