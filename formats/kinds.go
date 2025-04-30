package formats

import (
	"errors"
)

type (
	valueFormat int
)

const (
	String = valueFormat(iota)
	Json
	Integer
	Long
	Double
	Float
	Bytes
	None
)

var (
	ErrUnknownValueFormat = errors.New("unknown value format have been provided")
)

func (v valueFormat) GetName() (string, error) {
	switch v {
	case String:
		return "string", nil
	case Json:
		return "json", nil
	case Integer:
		return "integer", nil
	case Long:
		return "long", nil
	case Double:
		return "double", nil
	case Float:
		return "float", nil
	case Bytes:
		return "bytes", nil
	case None:
		return "none", nil
	}
	return "", ErrUnknownValueFormat
}
