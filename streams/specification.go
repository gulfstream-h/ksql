package streams

import (
	"context"
	"ksql/formats"
	"net"
)

type Stream[T any] struct {
	net          net.Conn
	vf           formats.valueFormat
	sourceTopic  *string
	sourceStream *string
	partitions   *uint8
}

var (
	ErrMissingSource = "Stream requires not nil source Stream or source topic"
)

func Get[T any](
	name string,
	values T,
	vf formats.valueFormat) (*Stream[T], string) {

	return &Stream[T]{
		vf: vf,
	}, ""
}

func Create[T any](
	name string,
	values T,
	sourceTopic *string,
	sourceStream *string,
	partitions *uint8,
	vf formats.valueFormat) (*Stream[T], string) {

	if sourceTopic == nil &&
		sourceStream == nil {
		return nil, ErrMissingSource
	}

	return &Stream[T]{
		vf: vf,
	}, ""
}

func Translate[T any](s any) Stream[T] {
	switch s.(type) {
	case *Stream[T]:
		return *s.(*Stream[T])
	default:
		return Stream[T]{}
	}
}

func (s *Stream[T]) SelectOnce(ctx context.Context, query string) {

}

func (s *Stream[T]) SelectWithEmit(ctx context.Context, query string) {
	s.net.Read()
}
