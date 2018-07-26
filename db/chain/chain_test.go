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
			name: "basic selection without table",
			chain: (&ExpresionChain{}).Select("field1", "field2", "field3").
				AndWhere("field1 > ?", 1).
				AndWhere("field2 = ?", 2).
				AndWhere("field3 > ?", "pajarito"),
			want:     "",
			wantArgs: nil,
			wantErr:  true,
		},
		{
			name: "basic selection with where",
			chain: (&ExpresionChain{}).Select("field1", "field2", "field3").
				Table("convenient_table").
				AndWhere("field1 > ?", 1).
				AndWhere("field2 = ?", 2).
				AndWhere("field3 > ?", "pajarito"),
			want:     "SELECT field1, field2, field3 FROM convenient_table WHERE field1 > $1 AND field2 = $2 AND field3 > $3",
			wantArgs: []interface{}{1, 2, "pajarito"},
			wantErr:  false,
		},
		{
			name: "basic selection with where and helpers",
			chain: (&ExpresionChain{}).Select("field1", "field2", "field3").
				Table("convenient_table").
				AndWhere(GreaterThan("field1", 1)).
				AndWhere(Equals("field2", 2)).
				AndWhere(GreaterThan("field3", "pajarito")).
				OrWhere(In("field3", "pajarito", "gatito", "perrito")).
				AndWhere(Null("field4")).
				AndWhere(NotNull("field5")),
			want:     "SELECT field1, field2, field3 FROM convenient_table WHERE field1 > $1 AND field2 = $2 AND field3 > $3 AND field4 IS NULL AND field5 IS NOT NULL OR field3 IN ($4, $5, $6)",
			wantArgs: []interface{}{1, 2, "pajarito", "pajarito", "gatito", "perrito"},
			wantErr:  false,
		},
		{
			name: "basic selection with or where",
			chain: (&ExpresionChain{}).Select("field1", "field2", "field3").
				Table("convenient_table").
				AndWhere("field1 > ?", 1).
				AndWhere("field2 = ?", 2).
				OrWhere("field3 > ?", "pajarito"),
			want:     "SELECT field1, field2, field3 FROM convenient_table WHERE field1 > $1 AND field2 = $2 OR field3 > $3",
			wantArgs: []interface{}{1, 2, "pajarito"},
			wantErr:  false,
		},
		{
			name: "basic selection with nested where",
			chain: (&ExpresionChain{}).Select("field1", "field2", "field3").
				Table("convenient_table").
				AndWhere("field1 > ?", 1).
				AndWhere("field2 = ?", 2).
				OrWhereGroup((&ExpresionChain{}).AndWhere("inner = ?", 1).AndWhere("inner2 > ?", 2)),
			want:     "SELECT field1, field2, field3 FROM convenient_table WHERE field1 > $1 AND field2 = $2 OR ( inner = $3 AND inner2 > $4)",
			wantArgs: []interface{}{1, 2, 1, 2},
			wantErr:  false,
		},
		{
			name: "basic selection with where and join",
			chain: (&ExpresionChain{}).Select("field1", "field2", "field3").
				Table("convenient_table").
				AndWhere("field1 > ?", 1).
				AndWhere("field2 = ?", 2).
				AndWhere("field3 > ?", "pajarito").
				Join("another_convenient_table ON pirulo = ?", "unpirulo"),
			want:     "SELECT field1, field2, field3 FROM convenient_table JOIN another_convenient_table ON pirulo = $1 WHERE field1 > $2 AND field2 = $3 AND field3 > $4",
			wantArgs: []interface{}{"unpirulo", 1, 2, "pajarito"},
			wantErr:  false,
		},
		{
			name: "basic deletion with where and join",
			chain: (&ExpresionChain{}).Delete().
				Table("convenient_table").
				AndWhere("field1 > ?", 1).
				AndWhere("field2 = ?", 2).
				AndWhere("field3 > ?", "pajarito").
				Join("another_convenient_table ON pirulo = ?", "unpirulo"),
			want:     "DELETE  FROM convenient_table JOIN another_convenient_table ON pirulo = $1 WHERE field1 > $2 AND field2 = $3 AND field3 > $4",
			wantArgs: []interface{}{"unpirulo", 1, 2, "pajarito"},
			wantErr:  false,
		},
		{
			name: "basic insert",
			chain: (&ExpresionChain{}).Insert(map[string]interface{}{"field1": "value1", "field2": 2, "field3": "blah"}).
				Table("convenient_table"),
			want:     "INSERT INTO convenient_table (field1, field2, field3) VALUES ($1, $2, $3)",
			wantArgs: []interface{}{"value1", 2, "blah"},
			wantErr:  false,
		},
		{
			name: "basic insert with conflict on column",
			chain: (&ExpresionChain{}).Insert(map[string]interface{}{"field1": "value1", "field2": 2, "field3": "blah"}).
				Table("convenient_table").Conflict("id", ConflictActionNothing),
			want:     "INSERT INTO convenient_table (field1, field2, field3) VALUES ($1, $2, $3) ON CONFLICT id DO NOTHING",
			wantArgs: []interface{}{"value1", 2, "blah"},
			wantErr:  false,
		},
		{
			name: "basic insert with conflict on constraint",
			chain: (&ExpresionChain{}).Insert(map[string]interface{}{"field1": "value1", "field2": 2, "field3": "blah"}).
				Table("convenient_table").Conflict(Constraint("id"), ConflictActionNothing),
			want:     "INSERT INTO convenient_table (field1, field2, field3) VALUES ($1, $2, $3) ON CONFLICT ON CONSTRAINT id DO NOTHING",
			wantArgs: []interface{}{"value1", 2, "blah"},
			wantErr:  false,
		},
		{
			name: "selection with where and join and order by",
			chain: (&ExpresionChain{}).Select("field1", "field2", "field3").
				Table("convenient_table").
				AndWhere("field1 > ?", 1).
				AndWhere("field2 = ?", 2).
				AndWhere("field3 > ?", "pajarito").
				OrderBy("field2, field3").
				Join("another_convenient_table ON pirulo = ?", "unpirulo"),
			want:     "SELECT field1, field2, field3 FROM convenient_table JOIN another_convenient_table ON pirulo = $1 WHERE field1 > $2 AND field2 = $3 AND field3 > $4 ORDER BY field2, field3",
			wantArgs: []interface{}{"unpirulo", 1, 2, "pajarito"},
			wantErr:  false,
		},
		{
			name: "basic selection with flavors of JOIN",
			chain: (&ExpresionChain{}).Select("field1", "field2", "field3").
				Table("convenient_table").
				AndWhere("field1 > ?", 1).
				AndWhere("field2 = ?", 2).
				AndWhere("field3 > ?", "pajarito").
				Join(JoinOn("another_convenient_table", "pirulo = ?", "unpirulo")).
				Join(JoinOn("yet_another_convenient_table", "pirulo = ?", "otrounpirulo")).
				LeftJoin(JoinOn("one_convenient_table", "pirulo2 = ?", "dospirulo")).
				RightJoin(JoinOn("three_convenient_table", "pirulo3 = ?", "trespirulo")).
				InnerJoin(JoinOn("four_convenient_table", "pirulo4 = ?", "cuatropirulo")).
				OuterJoin(JoinOn("five_convenient_table", "pirulo5 = ?", "cincopirulo")),
			want:     "SELECT field1, field2, field3 FROM convenient_table JOIN another_convenient_table ON pirulo = $1 JOIN yet_another_convenient_table ON pirulo = $2 LEFT JOIN one_convenient_table ON pirulo2 = $3 RIGHT JOIN three_convenient_table ON pirulo3 = $4 INNER JOIN four_convenient_table ON pirulo4 = $5 OUTER JOIN five_convenient_table ON pirulo5 = $6 WHERE field1 > $7 AND field2 = $8 AND field3 > $9",
			wantArgs: []interface{}{"unpirulo", "otrounpirulo", "dospirulo", "trespirulo", "cuatropirulo", "cincopirulo", 1, 2, "pajarito"},
			wantErr:  false,
		},
		{
			name: "basic selection with where and join and group by",
			chain: (&ExpresionChain{}).Select("field1", "field2", "field3").
				Table("convenient_table").
				AndWhere("field1 > ?", 1).
				AndWhere("field2 = ?", 2).
				AndWhere("field3 > ?", "pajarito").
				GroupBy("field2, field3").
				Join("another_convenient_table ON pirulo = ?", "unpirulo"),
			want:     "SELECT field1, field2, field3 FROM convenient_table JOIN another_convenient_table ON pirulo = $1 WHERE field1 > $2 AND field2 = $3 AND field3 > $4 GROUP BY field2, field3",
			wantArgs: []interface{}{"unpirulo", 1, 2, "pajarito"},
			wantErr:  false,
		},
		{
			name: "basic selection with where and join and group by and limit and offset",
			chain: (&ExpresionChain{}).Select("field1", "field2", "field3").
				Table("convenient_table").
				AndWhere("field1 > ?", 1).
				AndWhere("field2 = ?", 2).
				AndWhere("field3 > ?", "pajarito").
				GroupBy("field2, field3").
				Limit(100).
				Offset(10).
				Join("another_convenient_table ON pirulo = ?", "unpirulo"),
			want:     "SELECT field1, field2, field3 FROM convenient_table JOIN another_convenient_table ON pirulo = $1 WHERE field1 > $2 AND field2 = $3 AND field3 > $4 GROUP BY field2, field3 LIMIT 100 OFFSET 10",
			wantArgs: []interface{}{"unpirulo", 1, 2, "pajarito"},
			wantErr:  false,
		},
		{
			name: "basic update with where and join",
			chain: (&ExpresionChain{}).Update("field1 = ?, field3 = ?", "value2", 9).
				Table("convenient_table").
				AndWhere("field1 > ?", 1).
				AndWhere("field2 = ?", 2).
				AndWhere("field3 > ?", "pajarito").
				Join("another_convenient_table ON pirulo = ?", "unpirulo"),
			want:     "UPDATE convenient_table SET field1 = $1, field3 = $2 JOIN another_convenient_table ON pirulo = $3 WHERE field1 > $4 AND field2 = $5 AND field3 > $6",
			wantArgs: []interface{}{"value2", 9, "unpirulo", 1, 2, "pajarito"},
			wantErr:  false,
		},
		{
			name: "basic update with where and join but using map",
			chain: (&ExpresionChain{}).UpdateMap(map[string]interface{}{"field1": "value2", "field3": 9}).
				Table("convenient_table").
				AndWhere("field1 > ?", 1).
				AndWhere("field2 = ?", 2).
				AndWhere("field3 > ?", "pajarito").
				Join("another_convenient_table ON pirulo = ?", "unpirulo"),
			want:     "UPDATE convenient_table SET field1 = $1, field3 = $2 JOIN another_convenient_table ON pirulo = $3 WHERE field1 > $4 AND field2 = $5 AND field3 > $6",
			wantArgs: []interface{}{"value2", 9, "unpirulo", 1, 2, "pajarito"},
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
				t.Errorf("ExpresionChain.Render() got = %q, want %q", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.wantArgs) {
				t.Errorf("ExpresionChain.Render() got1 = %v, want %v", got1, tt.wantArgs)
			}
		})
	}
}
