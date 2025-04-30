package schema

import (
	"fmt"
	"github.com/fatih/structs"
	"strings"
)

type (
	Schema interface{}
)

func Serialize[T Schema](schema T) {
	var (
		values map[string]KsqlKind
	)

	fields := structs.Fields(schema)

	for _, field := range fields {
		tag := field.Tag("ksql")
		kind := field.Kind()

		ksqlKind, err := castType(kind)
		if err != nil {
			continue
		}

		values[tag] = ksqlKind
	}
}

func Deserialize(src []byte, dst any) map[string]KsqlKind {
	srcLiteral, found := strings.CutPrefix(string(src), "(")
	if !found {
		return nil
	}

	srcLiteral, found = strings.CutSuffix(string(src), ")")
	if !found {
		return nil
	}

	switch dst.(type) {
	case struct{}:
	default:
		return nil
	}

	pairs := strings.Split(srcLiteral, ",")

	if pairs == nil || len(pairs) == 0 {
		return nil
	}

	values := Unmarshal(pairs)

	fields := structs.Fields(dst)
	for _, field := range fields {
		tag := field.Tag("ksql")
		ksqlKind, ok := values[tag]
		if !ok {
			continue
		}

		fieldKind, err := castType(field.Kind())
		if err != nil {
			delete(values, tag)
			continue
		}

		if ksqlKind != fieldKind {
			delete(values, tag)
			continue
		}
	}

	return values
}

func FormatRegistry(fields map[string]KsqlKind) []byte {
	var (
		information string
	)

	information += "("

	for name, kind := range fields {
		kindLiteral, err := kind.Marshal()
		if err != nil {
			continue
		}
		information += fmt.Sprintf("%s %s", name, kindLiteral)
	}

	information += ")"

	return []byte(information)
}
