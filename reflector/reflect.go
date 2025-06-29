package reflector

import (
	"errors"
	"reflect"
)

// GetType - safe function for checking reflect type
// of user-provided struct
func GetType(val any) (reflect.Type, error) {
	t := reflect.TypeOf(val)

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

// GetValue - safe function for checking reflect value
// of user-provided struct
func GetValue(runtime any) (reflect.Value, error) {
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
