package util

func MustTrue[T any](fn func() (T, bool)) T {
	value, ok := fn()
	if !ok {
		panic("must return a true")
	}
	return value
}

func MustNoError[T any](fn func() (T, error)) T {
	value, err := fn()
	if err != nil {
		panic(err)
	}
	return value
}
