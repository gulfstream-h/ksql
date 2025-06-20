package reflector

import (
	"errors"
	"reflect"
)

func GetType[T any]() (reflect.Type, error) {
	var (
		runtime T
	)

	t := reflect.TypeOf(runtime)

	if t == nil {
		return nil, errors.New("type is nil")
	}

	if t.Kind() == reflect.Ptr {
		for t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
	}

	if t.Kind() != reflect.Struct {
		return nil, errors.New("type is not a struct")
	}

	return t, nil
}

func GetValue[T any](runtime T) (reflect.Value, error) {
	val := reflect.ValueOf(runtime)

	if val.Kind() == reflect.Ptr || val.Kind() == reflect.Interface {
		if val.IsNil() {
			return reflect.Value{}, errors.New("value is nil")
		}
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return reflect.Value{}, errors.New("value is not a struct")
	}

	return val, nil
}
