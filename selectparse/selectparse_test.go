//    Copyright 2018 Horacio Duran <horacio@shiftleft.io>, ShiftLeft Inc.
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

package selectparse

import (
	"testing"
)

func TestSelectParser_splitFields(t *testing.T) {
	tests := []struct {
		name     string
		s        *SelectParser
		expected []string
	}{
		{
			name: "basic set of columns",
			s: &SelectParser{
				Statement: "created_at, deleted_at, updated_at, name, age, location, DISTINCT field",
			},
			expected: []string{"created_at", "deleted_at", "updated_at", "name", "age", "location", "DISTINCT field"},
		},
		{
			name: "basic set of columns with simple function",
			s: &SelectParser{
				Statement: "created_at, deleted_at, updated_at, name, age, location, DISTINCT field, COALESCE(field, 0)",
			},
			expected: []string{"created_at", "deleted_at", "updated_at", "name", "age", "location", "DISTINCT field", "COALESCE(field, 0)"},
		},
		{
			name: "basic set of columns with keyword and function",
			s: &SelectParser{
				Statement: "created_at, deleted_at, updated_at, name, age, location, DISTINCT field, DISTINCT COALESCE(field, 0)",
			},
			expected: []string{"created_at", "deleted_at", "updated_at", "name", "age", "location", "DISTINCT field", "DISTINCT COALESCE(field, 0)"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.s.splitFields()
			if len(tt.expected) != len(tt.s.Columns) {
				t.Logf("got wrong column count, expected %d got %d", len(tt.expected), len(tt.s.Columns))
				t.FailNow()
			}
			for i := range tt.expected {
				if tt.expected[i] != tt.s.Columns[i] {
					t.Logf("got wrong columns, expected %q got %q", tt.expected[i], tt.s.Columns[i])
					t.FailNow()
				}
			}
		})
	}
}

func Test_extractFromKeywordsOrFunc(t *testing.T) {
	type args struct {
		column string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "basic function",
			args: args{column: "DISTINCT ON (column1, column2) column_name"},
			want: "column_name",
		},
		{
			name: "coalesce function",
			args: args{column: "COALESCE(column_name, 0)"},
			want: "coalesce",
		},
		{
			name: "coalesce function with space",
			args: args{column: "COALESCE (column_name, 0)"},
			want: "coalesce",
		},
		{
			name: "coalesce function with multiple spaces",
			args: args{column: "COALESCE    (column_name, 0)"},
			want: "coalesce",
		},
		{
			name: "esoteric max",
			args: args{column: "MAX(SELECT anumber FROM something WHERE a IN  (val1, val2, val3))"},
			want: "max",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := extractFromKeywordsOrFunc(tt.args.column); got != tt.want {
				t.Errorf("extractFromKeywordsOrFunc() = %v, want %v", got, tt.want)
			}
		})
	}
}
