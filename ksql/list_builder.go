package ksql

import "errors"

type (
	// ListBuilder - common contract for all LIST expressions
	ListBuilder interface {
		Expression() (string, error)
		Type() Reference
	}
	// list - base implementation of the ListBuilder interface
	list struct {
		typ Reference
	}
)

// List creates a new ListBuilder for the specified type
func List(typ Reference) ListBuilder {
	return &list{
		typ: typ,
	}
}

// Expression returns the KSQL expression for listing streams, tables, or topics
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

// Type returns the type of the list expression
func (l *list) Type() Reference {
	return l.typ
}
