package searchbolt

import (
	"reflect"
	"testing"
)

func Test_mapUnion(t *testing.T) {
	type args struct {
		m1     map[string]byte
		other  []map[string]byte
		result map[string]byte
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "Other map is empty",
			args: args{
				m1:     map[string]byte{"1": 1},
				other:  make([]map[string]byte, 0),
				result: map[string]byte{"1": 1},
			},
		},
		{
			name: "Other maps has one entry",
			args: args{
				m1: map[string]byte{"1": 1},
				other: []map[string]byte{
					{"1": 1},
				},
				result: map[string]byte{"1": 1},
			},
		},
		{
			name: "Other maps has one entry but different keys",
			args: args{
				m1: map[string]byte{"1": 1, "2": 1},
				other: []map[string]byte{
					{"1": 1},
				},
				result: map[string]byte{"1": 1},
			},
		},
		{
			name: "Multiple other maps with different entries",
			args: args{
				m1: map[string]byte{"1": 1},
				other: []map[string]byte{
					{"2": 2},
					{"3": 3},
				},
				result: map[string]byte{},
			},
		},
		{
			name: "Multiple other maps with some common keys",
			args: args{
				m1: map[string]byte{"1": 1, "2": 2},
				other: []map[string]byte{
					{"1": 1},
					{"2": 2},
					{"3": 3},
				},
				result: map[string]byte{},
			},
		},
		{
			name: "Multiple other maps with all common keys",
			args: args{
				m1: map[string]byte{"1": 1, "2": 2},
				other: []map[string]byte{
					{"1": 1, "2": 2},
					{"2": 2, "1": 1},
				},
				result: map[string]byte{"1": 1, "2": 2},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mapUnion(tt.args.m1, tt.args.other...)
			if !reflect.DeepEqual(tt.args.m1, tt.args.result) {
				t.Errorf("Map was not equal to expected, was: %v, expected: %v", tt.args.m1, tt.args.result)
			}
		})
	}
}

func Benchmark_mapUnion(b *testing.B) {
	testCases := []struct {
		name  string
		m1    map[string]byte
		other []map[string]byte
	}{
		{
			name: "Small map and empty other maps",
			m1:   map[string]byte{"1": 1, "2": 1},
			other: []map[string]byte{
				{"1": 1},
				{},
				{},
			},
		},
		{
			name: "Large map and large other maps",
			m1: func() map[string]byte {
				m := make(map[string]byte, 1000)
				for i := 0; i < 1000; i++ {
					m[string(rune(i))] = 1
				}
				return m
			}(),
			other: func() []map[string]byte {
				var others []map[string]byte
				for j := 0; j < 100; j++ {
					m := make(map[string]byte, 1000)
					for i := 0; i < 1000; i++ {
						m[string(rune(i%42))] = 1
					}
					others = append(others, m)
				}
				return others
			}(),
		},
	}

	for _, tc := range testCases {
		b.ResetTimer()
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				mapUnion(tc.m1, tc.other...)
			}
		})
	}
}
