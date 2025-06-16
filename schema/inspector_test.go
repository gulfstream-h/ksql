package schema

import (
	"reflect"
	"testing"
)

func TestCompareStructs(t *testing.T) {
	type StructA struct {
		ID    int
		Name  string
		Email string
	}

	type StructB struct {
		Name  string
		Email string
		Age   int
	}

	aType := reflect.TypeOf(StructA{})
	bType := reflect.TypeOf(StructB{})

	common, diff := CompareStructs(aType, bType)

	expectedCommon := map[string]bool{
		"ID":    false, // only in A
		"Name":  true,  // in both
		"Email": true,  // in both
	}

	expectedDiff := map[string]struct{}{
		"Age": {}, // only in B
	}

	for key, expected := range expectedCommon {
		got, ok := common[key]
		if !ok {
			t.Errorf("expected key %s in commonMap", key)
		}
		if got != expected {
			t.Errorf("expected commonMap[%s] = %v, got %v", key, expected, got)
		}
	}

	if len(diff) != len(expectedDiff) {
		t.Errorf("expected %d keys in diffMap, got %d", len(expectedDiff), len(diff))
	}

	for key := range expectedDiff {
		if _, ok := diff[key]; !ok {
			t.Errorf("expected key %s in diffMap", key)
		}
	}
}
