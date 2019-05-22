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
				AndWhere(GreaterThan("field1"), 1).
				AndWhere(Equals("field2"), 2).
				AndWhere(GreaterThan("field3"), "pajarito").
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
			name: "basic selection with or having",
			chain: (&ExpresionChain{}).Select("field1", "field2", "field3").
				Table("convenient_table").
				AndWhere("field1 > ?", 1).
				AndWhere("field2 = ?", 2).
				OrWhere("field3 > ?", "pajarito").
				OrHaving("haveable < ?", 1).
				AndHaving("moreHaveable == ?", 3),
			want:     "SELECT field1, field2, field3 FROM convenient_table WHERE field1 > $1 AND field2 = $2 OR field3 > $3 HAVING  moreHaveable == $4 OR haveable < $5",
			wantArgs: []interface{}{1, 2, "pajarito", 3, 1},
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
				Join("another_convenient_table", "pirulo = ?", "unpirulo"),
			want:     "SELECT field1, field2, field3 FROM convenient_table JOIN another_convenient_table ON pirulo = $1 WHERE field1 > $2 AND field2 = $3 AND field3 > $4",
			wantArgs: []interface{}{"unpirulo", 1, 2, "pajarito"},
			wantErr:  false,
		},
		{
			name: "basic selection with distinct",
			chain: (&ExpresionChain{}).Select(Distinct("field1")).
				Table("convenient_table").
				AndWhere("field1 > ?", 1),
			want:     "SELECT DISTINCT field1 FROM convenient_table WHERE field1 > $1",
			wantArgs: []interface{}{1},
			wantErr:  false,
		},
		{
			name: "basic selection with distinct as",
			chain: (&ExpresionChain{}).Select(As(Distinct("field1"), "renamed")).
				Table("convenient_table").
				AndWhere("field1 > ?", 1),
			want:     "SELECT DISTINCT field1 AS renamed FROM convenient_table WHERE field1 > $1",
			wantArgs: []interface{}{1},
			wantErr:  false,
		},
		{
			name: "basic selection with not / like",
			chain: (&ExpresionChain{}).Select("field1", "field2").
				Table("convenient_table").
				AndWhere(Like("field1"), "%hello%").
				AndWhere(NotLike("field2"), "%world%"),
			want:     "SELECT field1, field2 FROM convenient_table WHERE field1 LIKE $1 AND field2 NOT LIKE $2",
			wantArgs: []interface{}{"%hello%", "%world%"},
			wantErr:  false,
		},
		{
			name: "basic deletion with where and join",
			chain: (&ExpresionChain{}).Delete().
				Table("convenient_table").
				AndWhere("field1 > ?", 1).
				AndWhere("field2 = ?", 2).
				AndWhere("field3 > ?", "pajarito").
				Join("another_convenient_table", "pirulo = ?", "unpirulo"),
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
			name: "basic insert with nulls",
			chain: (&ExpresionChain{}).Insert(map[string]interface{}{"field1": "value1", "field2": 2, "field3": nil}).
				Table("convenient_table"),
			want:     "INSERT INTO convenient_table (field1, field2, field3) VALUES ($1, $2, $3)",
			wantArgs: []interface{}{"value1", 2, "NULL"},
			wantErr:  false,
		},
		{
			name: "basic insert with conflict on column",
			chain: (&ExpresionChain{}).
				Insert(map[string]interface{}{"field1": "value1", "field2": 2, "field3": "blah"}).
				Table("convenient_table").
				OnConflict(func(c *OnConflict) {
					c.OnColumn("field2").DoNothing()
				}),
			want:     "INSERT INTO convenient_table (field1, field2, field3) VALUES ($1, $2, $3) ON CONFLICT ( field2 ) DO NOTHING",
			wantArgs: []interface{}{"value1", 2, "blah"},
			wantErr:  false,
		},
		{
			name: "advanced insert with conflict on column",
			chain: (&ExpresionChain{}).
				Insert(map[string]interface{}{"field1": "value1", "field2": 2, "field3": "blah"}).
				Table("convenient_table").
				OnConflict(func(c *OnConflict) {
					c.OnColumn("field2", "field3").DoNothing()
				}),
			want:     "INSERT INTO convenient_table (field1, field2, field3) VALUES ($1, $2, $3) ON CONFLICT ( field2, field3 ) DO NOTHING",
			wantArgs: []interface{}{"value1", 2, "blah"},
			wantErr:  false,
		},
		{
			name: "basic insert with conflict on constraint",
			chain: (&ExpresionChain{}).
				Insert(map[string]interface{}{"field1": "value1", "field2": 2, "field3": "blah"}).
				Table("convenient_table").
				OnConflict(func(c *OnConflict) {
					c.OnConstraint("id").DoNothing()
				}),
			want:     "INSERT INTO convenient_table (field1, field2, field3) VALUES ($1, $2, $3) ON CONFLICT ON CONSTRAINT id DO NOTHING",
			wantArgs: []interface{}{"value1", 2, "blah"},
			wantErr:  false,
		},
		{
			name: "basic insert with default conflict",
			chain: (&ExpresionChain{}).
				Insert(map[string]interface{}{"field1": "value1", "field2": 2, "field3": "blah"}).
				Table("convenient_table").
				OnConflict(func(c *OnConflict) {
					c.DoNothing()
				}),
			want:     "INSERT INTO convenient_table (field1, field2, field3) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
			wantArgs: []interface{}{"value1", 2, "blah"},
			wantErr:  false,
		},
		{
			name: "complex insert with an update to default clause",
			chain: (&ExpresionChain{}).
				Insert(map[string]interface{}{"field1": "value1", "field2": 2, "field3": "foo"}).
				Table("convenient_table").
				OnConflict(func(c *OnConflict) {
					c.OnConstraint("id").DoUpdate().Set("field2", 4, "field3", "bar")
				}),
			want:     "INSERT INTO convenient_table (field1, field2, field3) VALUES ($1, $2, $3) ON CONFLICT ON CONSTRAINT id DO UPDATE SET field2 = $4, field3 = $5",
			wantArgs: []interface{}{"value1", 2, "foo", 4, "bar"},
			wantErr:  false,
		},
		{
			name: "NOW THIS IS PODRACING!! Upsert WITH returning data",
			chain: (&ExpresionChain{}).
				Insert(map[string]interface{}{"field1": "value1", "field2": 2, "field3": "blah"}).
				Table("convenient_table").
				OnConflict(func(c *OnConflict) {
					c.OnConstraint("id").DoUpdate().Set("field2", 2)
				}).
				Returning("field1", "field2", "field3"),
			want:     "INSERT INTO convenient_table (field1, field2, field3) VALUES ($1, $2, $3) ON CONFLICT ON CONSTRAINT id DO UPDATE SET field2 = $4 RETURNING field1, field2, field3",
			wantArgs: []interface{}{"value1", 2, "blah", 2},
			wantErr:  false,
		},
		{
			name: "basic insert with conflict on constraint with nulls",
			chain: (&ExpresionChain{}).
				Insert(map[string]interface{}{"field1": "value1", "field2": nil, "field3": "blah"}).
				Table("convenient_table").
				OnConflict(func(c *OnConflict) {
					c.OnConstraint("id").DoNothing()
				}),
			want:     "INSERT INTO convenient_table (field1, field2, field3) VALUES ($1, $2, $3) ON CONFLICT ON CONSTRAINT id DO NOTHING",
			wantArgs: []interface{}{"value1", "NULL", "blah"},
			wantErr:  false,
		},
		{
			name: "selection with where and join and order by",
			chain: (&ExpresionChain{}).Select("field1", "field2", "field3").
				Table("convenient_table").
				AndWhere("field1 > ?", 1).
				AndWhere("field2 = ?", 2).
				AndWhere("field3 > ?", "pajarito").
				OrderBy(Asc("field2").Asc("field3")).
				Join("another_convenient_table", "pirulo = ?", "unpirulo"),
			want:     "SELECT field1, field2, field3 FROM convenient_table JOIN another_convenient_table ON pirulo = $1 WHERE field1 > $2 AND field2 = $3 AND field3 > $4 ORDER BY field2 ASC, field3 ASC",
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
				Join("another_convenient_table", "pirulo = ?", "unpirulo").
				Join("yet_another_convenient_table", "pirulo = ?", "otrounpirulo").
				LeftJoin("one_convenient_table", "pirulo2 = ?", "dospirulo").
				RightJoin("three_convenient_table", "pirulo3 = ?", "trespirulo").
				InnerJoin("four_convenient_table", "pirulo4 = ?", "cuatropirulo").
				FullJoin("five_convenient_table", "pirulo5 = ?", "cincopirulo"),
			want:     "SELECT field1, field2, field3 FROM convenient_table JOIN another_convenient_table ON pirulo = $1 JOIN yet_another_convenient_table ON pirulo = $2 LEFT JOIN one_convenient_table ON pirulo2 = $3 RIGHT JOIN three_convenient_table ON pirulo3 = $4 INNER JOIN four_convenient_table ON pirulo4 = $5 FULL JOIN five_convenient_table ON pirulo5 = $6 WHERE field1 > $7 AND field2 = $8 AND field3 > $9",
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
				Join("another_convenient_table", "pirulo = ?", "unpirulo"),
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
				Join("another_convenient_table", "pirulo = ?", "unpirulo"),
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
				Join("another_convenient_table", "pirulo = ?", "unpirulo"),
			want:     "UPDATE convenient_table SET field1 = $1, field3 = $2 JOIN another_convenient_table ON pirulo = $3 WHERE field1 > $4 AND field2 = $5 AND field3 > $6",
			wantArgs: []interface{}{"value2", 9, "unpirulo", 1, 2, "pajarito"},
			wantErr:  false,
		},
		{
			name: "update with bytea data",
			chain: (&ExpresionChain{}).Update("field1 = ?", []byte{0xde, 0xed, 0xbe, 0xef}).
				Table("convenient_table").
				Returning("*"),
			want:     "UPDATE convenient_table SET field1 = $1 RETURNING *",
			wantArgs: []interface{}{[]byte{0xde, 0xed, 0xbe, 0xef}},
			wantErr:  false,
		},
		{
			name: "basic update with RETURNING",
			chain: (&ExpresionChain{}).Update("status = ?", 9).
				Table("convenient_table").
				AndWhere("value IN (?, ?)", 1, 2).
				Returning("*"),
			want:     "UPDATE convenient_table SET status = $1 WHERE value IN ($2, $3) RETURNING *",
			wantArgs: []interface{}{9, 1, 2},
			wantErr:  false,
		},
		{
			name: "basic update with where and join",
			chain: (&ExpresionChain{}).Update("field1 = ?, field3 = ?", "value2", nil).
				Table("convenient_table").
				AndWhere("field1 > ?", 1).
				AndWhere("field2 = ?", 2).
				AndWhere("field3 > ?", "pajarito").
				Join("another_convenient_table", "pirulo = ?", "unpirulo"),
			want:     "UPDATE convenient_table SET field1 = $1, field3 = $2 JOIN another_convenient_table ON pirulo = $3 WHERE field1 > $4 AND field2 = $5 AND field3 > $6",
			wantArgs: []interface{}{"value2", "NULL", "unpirulo", 1, 2, "pajarito"},
			wantErr:  false,
		},
		{
			name: "basic update with where and join but using map",
			chain: (&ExpresionChain{}).UpdateMap(map[string]interface{}{"field1": "value2", "field3": 9}).
				Table("convenient_table").
				AndWhere("field1 > ?", 1).
				AndWhere("field2 = ?", 2).
				AndWhere("field3 > ?", "pajarito").
				Join("another_convenient_table", "pirulo = ?", "unpirulo"),
			want:     "UPDATE convenient_table SET field1 = $1, field3 = $2 JOIN another_convenient_table ON pirulo = $3 WHERE field1 > $4 AND field2 = $5 AND field3 > $6",
			wantArgs: []interface{}{"value2", 9, "unpirulo", 1, 2, "pajarito"},
			wantErr:  false,
		},
		{
			name: "heavy query",
			chain: (&ExpresionChain{}).Table("table1").
				Select("table1.field1",
					"table1.field2",
					"table1.field3",
					"table1.field4",
					"table1.field5",
					"table1.field6",
					"table1.field7",
					"table1.field8",
					"table1.field9",
					"table1.field10",
					"table1.field11",
					"table1.field12",
					"table1.field13",
					"table1.field14",
					As("sum(table2.field0)", "things")).
				LeftJoin("table2",
					`table2.field1 = table1.field1 AND 
					table2.field2 = table1.field2 AND
					table2.field3 = table1.field3`).
				AndWhere(In("field10", "oneproject", "twoproject")).
				AndWhere("table1.field14 = ?", "orgidasdasasds").
				AndWhere("table2.field8 = false").
				GroupBy(`table1.field1,
				table1.field2,
				table1.field3,
				table1.field4,
				table1.field5,
				table1.field6,
				table1.field7,
				table1.field8,
				table1.field9,
				table1.field10,
				table1.field11,
				table1.field12,
				table1.field13,
				table1.field14`),
			want:     "SELECT table1.field1, table1.field2, table1.field3, table1.field4, table1.field5, table1.field6, table1.field7, table1.field8, table1.field9, table1.field10, table1.field11, table1.field12, table1.field13, table1.field14, sum(table2.field0) AS things FROM table1 LEFT JOIN table2 ON table2.field1 = table1.field1 AND \n\t\t\t\t\ttable2.field2 = table1.field2 AND\n\t\t\t\t\ttable2.field3 = table1.field3 WHERE field10 IN ($1, $2) AND table1.field14 = $3 AND table2.field8 = false GROUP BY table1.field1,\n\t\t\t\ttable1.field2,\n\t\t\t\ttable1.field3,\n\t\t\t\ttable1.field4,\n\t\t\t\ttable1.field5,\n\t\t\t\ttable1.field6,\n\t\t\t\ttable1.field7,\n\t\t\t\ttable1.field8,\n\t\t\t\ttable1.field9,\n\t\t\t\ttable1.field10,\n\t\t\t\ttable1.field11,\n\t\t\t\ttable1.field12,\n\t\t\t\ttable1.field13,\n\t\t\t\ttable1.field14",
			wantArgs: []interface{}{"oneproject", "twoproject", "orgidasdasasds"},
			wantErr:  false,
		},
		{
			name: "insert returning with where, a bit of everything",
			chain: (&ExpresionChain{}).Insert(map[string]interface{}{
				"field1": "somethingelse",
				"field2": 2,
			}).
				Table("atablename").OnConflict(func(c *OnConflict) {
				c.OnColumn("field1").DoUpdate().SetSQL("field2", "atablename.field2 + 1").
					Where((&ExpresionChain{}).AndWhere(Equals("atablename.field1"), "something"))
			}).
				Returning("atablename.field2"),
			want:     "INSERT INTO atablename (field1, field2) VALUES ($1, $2) ON CONFLICT ( field1 ) DO UPDATE SET (field2) = (atablename.field2 + 1) WHERE  atablename.field1 = $3 RETURNING atablename.field2",
			wantArgs: []interface{}{"somethingelse", 2, "something"},
			wantErr:  false,
		},
		{
			name: "basic selection with CTEs",
			chain: (&ExpresionChain{}).Select("field1", "field2", "field3").
				With("a_cte", (&ExpresionChain{}).Select("*").From("some_table_in_cte")).
				Table("convenient_table").
				AndWhere("field1 > ?", 1).
				AndWhere("field2 = ?", 2).
				AndWhere("field3 > ?", "pajarito"),
			want:     "WITH a_cte AS (SELECT * FROM some_table_in_cte) SELECT field1, field2, field3 FROM convenient_table WHERE field1 > $1 AND field2 = $2 AND field3 > $3",
			wantArgs: []interface{}{1, 2, "pajarito"},
			wantErr:  false,
		},
		{
			name: "basic selection with CTEs with args",
			chain: (&ExpresionChain{}).Select("field1", "field2", "field3").
				With("a_cte", (&ExpresionChain{}).Select("*").From("some_table_in_cte").AndWhere("a_field = ?", "ctevalue")).
				Table("convenient_table").
				AndWhere("field1 > ?", 1).
				AndWhere("field2 = ?", 2).
				AndWhere("field3 > ?", "pajarito"),
			want:     "WITH a_cte AS (SELECT * FROM some_table_in_cte WHERE a_field = $1) SELECT field1, field2, field3 FROM convenient_table WHERE field1 > $2 AND field2 = $3 AND field3 > $4",
			wantArgs: []interface{}{"ctevalue", 1, 2, "pajarito"},
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
				t.Errorf("ExpresionChain.Render() \ngot %q, \nwant %q", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.wantArgs) {
				t.Errorf("ExpresionChain.Render() got1 %v, want %v", got1, tt.wantArgs)
			}
		})
	}
}
