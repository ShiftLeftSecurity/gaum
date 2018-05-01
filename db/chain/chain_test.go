package chain

import (
	"reflect"
	"sync"
	"testing"
)

func TestExpresionChain_Render(t *testing.T) {
	type fields struct {
		lock          sync.Mutex
		segments      []querySegmentAtom
		table         string
		mainOperation querySegmentAtom
		limit         *querySegmentAtom
		offset        *querySegmentAtom
	}

	tests := []struct {
		name     string
		chain    *ExpresionChain
		want     string
		wantArgs []interface{}
		wantErr  bool
	}{
		{
			name: "basic selection with where",
			chain: (&ExpresionChain{}).Select([]string{"field1", "field2", "field3"}).
				Table("convenient_table").
				Where("field1 > ?", 1).
				Where("field2 == ?", 2).
				Where("field3 > ?", "pajarito"),
			want:     "SELECT field1, field2, field3 FROM convenient_table WHERE field1 > ? AND field2 == ? AND field3 > ?",
			wantArgs: []interface{}{1, 2, "pajarito"},
			wantErr:  false,
		},
		{
			name: "basic selection with where and join",
			chain: (&ExpresionChain{}).Select([]string{"field1", "field2", "field3"}).
				Table("convenient_table").
				Where("field1 > ?", 1).
				Where("field2 == ?", 2).
				Where("field3 > ?", "pajarito").
				Join("another_convenient_table ON pirulo = ?", "unpirulo"),
			want:     "SELECT field1, field2, field3 FROM convenient_table JOIN another_convenient_table ON pirulo = ? WHERE field1 > ? AND field2 == ? AND field3 > ?",
			wantArgs: []interface{}{"unpirulo", 1, 2, "pajarito"},
			wantErr:  false,
		},
		{
			name: "basic insert",
			chain: (&ExpresionChain{}).Insert([]string{"field1", "field2", "field3"}, []interface{}{"value1", 2, "blah"}).
				Table("convenient_table"),
			want:     "INSERT INTO ? (field1, field2, field3) VALUES (?, ?, ?)",
			wantArgs: []interface{}{"convenient_table", "value1", 2, "blah"},
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ec := tt.chain
			got, got1, err := ec.Render()
			if (err != nil) != tt.wantErr {
				t.Errorf("ExpresionChain.Render() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ExpresionChain.Render() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.wantArgs) {
				t.Errorf("ExpresionChain.Render() got1 = %v, want %v", got1, tt.wantArgs)
			}
		})
	}
}
