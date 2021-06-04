package chain

import (
	"fmt"
	"testing"
)

func Test_digitSize(t *testing.T) {
	type args struct {
		argLen int
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{name: "one",
			args: args{argLen: 1},
			want: 1},
		{name: "five",
			args: args{argLen: 5},
			want: 5},
		{name: "ten",
			args: args{argLen: 10},
			want: 11},
		{name: "fiftythree",
			args: args{argLen: 53},
			want: 97},
		{name: "onehundred and 3",
			args: args{argLen: 103},
			want: 201},
		{name: "sixhundredandtwenty",
			args: args{argLen: 620},
			want: 1752},
		{name: "one k",
			args: args{argLen: 1024},
			want: 2989},
		{name: "twelvethousands",
			args: args{argLen: 12000},
			want: 48894},
		{name: "twentyfivethoudsands",
			args: args{argLen: 25000},
			want: 113894},
		{name: "thirtyeightthousands",
			args: args{argLen: 38000},
			want: 178894},
		{name: "fortythreethousands",
			args: args{argLen: 43000},
			want: 203894},
		{name: "max",
			args: args{argLen: 65000},
			want: 313894},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := digitSize(tt.args.argLen); got != tt.want {
				t.Errorf("digitSize() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPlaceholderEscaping(t *testing.T) {
	tests := []struct {
		q    string
		want string
		args []interface{}
	}{
		{
			q:    "? = 1",
			want: "$1 = 1",
			args: []interface{}{1},
		},
		{
			q:    "\\? = 1",
			want: "? = 1",
			args: []interface{}{},
		},
		{
			q:    "? = ? AND \\? = 1",
			want: "$1 = $2 AND ? = 1",
			args: []interface{}{1, 1},
		},
		{
			q:    `'["a", "b"]'::jsonb \?& array['a', 'b']`,
			want: `'["a", "b"]'::jsonb ?& array['a', 'b']`,
			args: []interface{}{},
		},
		{
			q:    `'["a", "b"]'::jsonb \?& array[?]`,
			want: `'["a", "b"]'::jsonb ?& array[$1]`,
			args: []interface{}{"a"},
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			result, _, _ := MarksToPlaceholders(tt.q, tt.args)
			if result != tt.want {
				t.Errorf("got %v, want %v", result, tt.want)
			}
		})
	}
}
