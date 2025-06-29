package util

func MustNoError[T any](fn func() (T, error)) T {
	value, err := fn()
	if err != nil {
		panic(err)
	}
	return value
}
