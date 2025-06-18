package static

import (
	"errors"
)

var (
	ErrMissingHost             = errors.New("missing ksql host")
	ErrTimeoutIsZeroOrNegative = errors.New("await timeout cannot be equal or less then zero")

	ErrTopicNotExist      = errors.New("topic doesn't exist")
	ErrStreamDoesNotExist = errors.New("stream does not exist")
	ErrTableDoesNotExist  = errors.New("table does not exist")

	ErrMalformedResponse      = errors.New("unprocessable ksql response")
	ErrUnserializableResponse = errors.New("unserializable ksql response")
)
