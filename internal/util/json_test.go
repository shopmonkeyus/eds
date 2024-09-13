package util

import (
	"reflect"
	"testing"
)

func TestJSONDiff(t *testing.T) {
	tests := []struct {
		obj    map[string]any
		found  []string
		wanted []string
	}{
		{
			obj:    map[string]any{"a": 1, "b": 2, "c": 3},
			found:  []string{"a", "b", "d"},
			wanted: []string{"c"},
		},
		{
			obj:    map[string]any{"a": 1, "b": 2, "c": 3},
			found:  []string{"a", "b", "c"},
			wanted: []string{},
		},
	}
	for _, test := range tests {
		got := JSONDiff(test.obj, test.found)
		if !reflect.DeepEqual(got, test.wanted) {
			t.Errorf("JSONDiff(%v, %v) = %v, wanted %v", test.obj, test.found, got, test.wanted)
		}
	}
}
