package util

// MustNoError - is used in functions where error signals about critical
// functionality and the program should terminate immediately
func MustNoError[T any](fn func() (T, error)) T {
	value, err := fn()
	if err != nil {
		panic(err)
	}
	return value
}
