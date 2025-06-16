package schema

import (
	"ksql/kinds"
	"testing"
)

func TestSerializeFieldsToStruct(t *testing.T) {
	fields := []SearchField{
		{Name: "ID", Kind: kinds.Int},
		{Name: "Name", Kind: kinds.String},
	}

	typ := SerializeFieldsToStruct(fields)

	if typ.NumField() != len(fields) {
		t.Fatalf("expected %d fields, got %d", len(fields), typ.NumField())
	}

	for i, field := range fields {
		got := typ.Field(i)
		if got.Name != field.Name {
			t.Errorf("expected field name %s, got %s", field.Name, got.Name)
		}
	}
}

func TestParseStructToFieldsDictionary(t *testing.T) {
	fields := []SearchField{
		{Name: "ID", Kind: kinds.Int, Tag: "ID"},
		{Name: "IsActive", Kind: kinds.Bool, Tag: "IsActive"},
	}

	typ := SerializeFieldsToStruct(fields)
	result := ParseStructToFieldsDictionary("TestStruct", typ)

	if len(result) != len(fields) {
		t.Fatalf("expected %d fields, got %d", len(fields), len(result))
	}

	for _, field := range fields {
		got, ok := result[field.Name]
		if !ok {
			t.Errorf("missing field %s", field.Name)
			continue
		}
		if got.Kind != field.Kind {
			t.Errorf("field %s: expected kind %v, got %v", field.Name, field.Kind, got.Kind)
		}
	}
}

func TestParseStructToFields(t *testing.T) {
	fields := []SearchField{
		{Name: "ID", Kind: kinds.Int},
		{Name: "Score", Kind: kinds.Float},
	}

	typ := SerializeFieldsToStruct(fields)
	result := ParseStructToFields("TestStruct", typ)

	if len(result) != len(fields) {
		t.Fatalf("expected %d fields, got %d", len(fields), len(result))
	}
}

func TestSerializeRemoteSchema(t *testing.T) {
	schema := map[string]string{
		"AGE":    "INT",
		"NAME":   "STRING",
		"ACTIVE": "BOOL",
	}

	typ := SerializeRemoteSchema(schema)

	if typ.NumField() != len(schema) {
		t.Fatalf("expected %d fields, got %d", len(schema), typ.NumField())
	}
}

func TestParseHeadersAndValues(t *testing.T) {
	headers := "`id`,`name`,`score`"
	values := []any{1, "Alice", 98.5}

	result, err := ParseHeadersAndValues(headers, values)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result) != 3 {
		t.Fatalf("expected 3 values, got %d", len(result))
	}

	if result["name"] != "Alice" {
		t.Errorf("expected name to be Alice, got %v", result["name"])
	}
}
