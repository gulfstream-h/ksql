package util

func MustBool[T any](fn func() (T, bool)) T {
	value, ok := fn()
	if !ok {
		panic("must return a true")
	}
	return value
}

func MustError[T any](fn func() (T, error)) T {
	value, err := fn()
	if err != nil {
		panic(err)
	}
	return value
}
