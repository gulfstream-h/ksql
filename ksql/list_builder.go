package ksql

import "errors"

type (
	ListBuilder interface {
		Expression() (string, error)
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

func (l *list) Expression() (string, error) {
	var operation string

	switch l.typ {
	case STREAM:
		operation = "LIST STREAMS;"
	case TABLE:
		operation = "LIST TABLES;"
	case TOPIC:
		operation = "LIST TOPICS;"
	default:
		return "", errors.New("invalid list type, must be STREAM, TABLE, or TOPIC")
	}

	return operation, nil
}

func (l *list) Type() Reference {
	return l.typ
}
