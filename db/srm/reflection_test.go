package srm

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/pkg/errors"
)

func TestMapFromStruct(t *testing.T) {

	type empty struct{}

	type notags struct {
		One            string
		TwoWords       string
		ThreeMoreWords int
		littlePrivate  int
	}

	type onlyTags struct {
		One string `gaum:"field_name:different_name_one"`
		Two bool   `gaum:"field_name:name_two"`
	}

	type mixedTags struct {
		One   string
		Two   int64
		Three string `gaum:"field_name:three_uses_tag"`
	}

	type testCase struct {
		label  string
		input  interface{}
		output map[string]interface{}
		err    error
	}

	for _, tc := range []testCase{
		{
			label:  "cannot use ptr",
			input:  &empty{},
			output: nil,
			err:    errors.Errorf("cannot convert non-struct type %T to map", &empty{}),
		},
		{
			label:  "empty",
			input:  empty{},
			output: map[string]interface{}{},
			err:    nil,
		},
		{
			label: "notags",
			input: notags{"one", "two", 3, 4},
			output: map[string]interface{}{
				"one":              "one",
				"two_words":        "two",
				"three_more_words": 3,
			},
			err: nil,
		},
		{
			label: "mixedTags",
			input: mixedTags{"val1", int64(2), "val3"},
			output: map[string]interface{}{
				"one":            "val1",
				"two":            int64(2),
				"three_uses_tag": "val3",
			},
			err: nil,
		},
	} {
		out, err := MapFromStruct(tc.input)
		if fmt.Sprintf("%v", err) != fmt.Sprintf("%v", tc.err) {
			t.Errorf("%s - expected err %v to equal %v", tc.label, err, tc.err)
		}

		if !reflect.DeepEqual(out, tc.output) {
			t.Errorf("%s - expected %v to equal %v", tc.label, tc.output, out)
		}
	}
}
