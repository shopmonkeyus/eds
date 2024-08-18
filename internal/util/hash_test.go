package util

import (
	"testing"
)

func TestHash(t *testing.T) {
	tests := []struct {
		name string
		vals []interface{}
		want string
	}{
		{
			name: "Empty input",
			vals: []interface{}{},
			want: "ef46db3751d8e999",
		},
		{
			name: "Single value",
			vals: []interface{}{"hello"},
			want: "26c7827d889f6da3",
		},
		{
			name: "Multiple values",
			vals: []interface{}{"hello", 42, true},
			want: "d481b75d0fa4abff",
		},
		{
			name: "Multiple values with nil",
			vals: []interface{}{"hello", 42, true, nil},
			want: "a668199a6b3fc355",
		},
		{
			name: "Nil only",
			vals: []interface{}{nil},
			want: "7c5b4e400f80bf7c",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Hash(tt.vals...)
			if got != tt.want {
				t.Errorf("Hash() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestModulo(t *testing.T) {
	tests := []struct {
		name string
		val  string
		num  int
		want int
	}{
		{
			name: "Empty input",
			val:  "",
			num:  10,
			want: 1,
		},
		{
			name: "Single",
			val:  "1",
			num:  1,
			want: 0,
		},
		{
			name: "Double",
			val:  "1",
			num:  2,
			want: 0,
		},
		{
			name: "Triple",
			val:  "1",
			num:  3,
			want: 1,
		},
		{
			name: "Multiple",
			val:  "1 2 3 4",
			num:  10,
			want: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Modulo(tt.val, tt.num)
			if got != tt.want {
				t.Errorf("Modulo() = %v, want %v", got, tt.want)
			}
		})
	}
}
