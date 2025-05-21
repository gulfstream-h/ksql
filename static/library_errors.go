package static

import (
	"errors"
)

var (
	ErrTopicNotExist      = errors.New("topic doesn't exist")
	ErrStreamDoesNotExist = errors.New("stream does not exist")
	ErrTableDoesNotExist  = errors.New("table does not exist")

	ErrMalformedResponse      = errors.New("unprocessable ksql response")
	ErrUnserializableResponse = errors.New("unserializable ksql response")
)
