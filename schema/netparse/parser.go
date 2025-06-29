package netparse

import (
	"fmt"
	"ksql/kernel/protocol/dao"
	"ksql/reflector"
	"strings"
)

func ParseNetResponse[S any](
	headers dao.Header,
	row dao.Row,
) (S, error) {
	var (
		streamDTO S
	)

	resultDict, err := ParseHeadersAndValues(headers.Header.Schema, row.Row.Columns)
	if err != nil {
		return streamDTO, fmt.Errorf("parse headers and values: %w", err)
	}

	val, err := reflector.GetValue(&streamDTO)
	if err != nil {
		return streamDTO, fmt.Errorf("reflector: get value: %w", err)
	}

	typ, err := reflector.GetType(&streamDTO)
	if err != nil {
		return streamDTO, fmt.Errorf("reflector: get type: %w", err)
	}

	for k, v := range resultDict {
		for i := 0; i < val.NumField(); i++ {
			structField := typ.Field(i)
			fieldVal := val.Field(i)

			if strings.EqualFold(structField.Tag.Get("ksql"), k) {
				if fieldVal.CanSet() && v != nil {
					val, ok := NormalizeValue(v, fieldVal.Type())
					if ok {
						fieldVal.Set(val)
					}
				}
				break
			}
		}
	}

	return streamDTO, nil
}
