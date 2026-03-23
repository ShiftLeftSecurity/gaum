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

package srm

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/ShiftLeftSecurity/gaum/v2/db/logging"
)

// TestNullScanner_JsonRawMessage tests that nullScanner can scan JSONB columns
// (returned as string by pgx v5, or as []byte) into json.RawMessage fields.
func TestNullScanner_JsonRawMessage(t *testing.T) {
	tests := []struct {
		name string
		src  interface{}
		want json.RawMessage
	}{
		{
			name: "string source (pgx v5 JSONB)",
			src:  `{"key":"value"}`,
			want: json.RawMessage(`{"key":"value"}`),
		},
		{
			name: "[]byte source",
			src:  []byte(`{"key":"value"}`),
			want: json.RawMessage(`{"key":"value"}`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var dest json.RawMessage
			ns := &nullScanner{fieldPtr: &dest}
			if err := ns.Scan(tt.src); err != nil {
				t.Fatalf("Scan() error = %v", err)
			}
			if !reflect.DeepEqual(dest, tt.want) {
				t.Errorf("got %q, want %q", dest, tt.want)
			}
		})
	}
}

// TestNullScanner_JsonRawMessage_Nil confirms that a NULL source leaves the
// destination unchanged (the nil-early-return path).
func TestNullScanner_JsonRawMessage_Nil(t *testing.T) {
	dest := json.RawMessage(`original`)
	ns := &nullScanner{fieldPtr: &dest}
	if err := ns.Scan(nil); err != nil {
		t.Fatalf("Scan(nil) error = %v", err)
	}
	if string(dest) != "original" {
		t.Errorf("expected dest to be unchanged, got %q", dest)
	}
}

// TestNullScanner_JsonRawMessage_UnsupportedSrc confirms that an unsupported
// source type returns an error rather than silently succeeding.
func TestNullScanner_JsonRawMessage_UnsupportedSrc(t *testing.T) {
	var dest json.RawMessage
	ns := &nullScanner{fieldPtr: &dest}
	if err := ns.Scan(42); err == nil {
		t.Error("expected error for unsupported source type, got nil")
	}
}

// TestNullScanner_TimeUTCNormalization tests that pgx v5's non-singleton UTC
// location is normalized so that reflect.DeepEqual comparisons against time.UTC work.
func TestNullScanner_TimeUTCNormalization(t *testing.T) {
	// Simulate the non-singleton UTC location pgx v5 returns.
	nonSingletonUTC := time.FixedZone("UTC", 0)
	src := time.Date(2024, 1, 15, 12, 0, 0, 0, nonSingletonUTC)

	// Verify the premise: the non-singleton location breaks DeepEqual.
	expected := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)
	if reflect.DeepEqual(src, expected) {
		t.Skip("this Go version treats FixedZone(UTC,0) as the singleton; test not applicable")
	}

	var dest time.Time
	ns := &nullScanner{fieldPtr: &dest}
	if err := ns.Scan(src); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if !reflect.DeepEqual(dest, expected) {
		t.Errorf("DeepEqual failed after normalization: got location %v, want %v",
			dest.Location(), expected.Location())
	}
}

// TestNullScanner_TimeNonUTCUnchanged confirms that non-UTC locations are not
// altered by the normalization.
func TestNullScanner_TimeNonUTCUnchanged(t *testing.T) {
	berlin, err := time.LoadLocation("Europe/Berlin")
	if err != nil {
		t.Skip("Europe/Berlin timezone not available:", err)
	}
	src := time.Date(2024, 6, 1, 10, 0, 0, 0, berlin)

	var dest time.Time
	ns := &nullScanner{fieldPtr: &dest}
	if err := ns.Scan(src); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if dest.Location() != berlin {
		t.Errorf("expected location %v to be unchanged, got %v", berlin, dest.Location())
	}
}

// TestNullScanner_PtrJsonRawMessage tests the **T branch (lines 236–249):
// when the struct field is *json.RawMessage (nullable JSONB), fieldPtr is
// **json.RawMessage and nullScanner must allocate a fresh json.RawMessage and
// set the pointer to it.
func TestNullScanner_PtrJsonRawMessage(t *testing.T) {
	tests := []struct {
		name string
		src  interface{}
		want *json.RawMessage
	}{
		{
			name: "string source (pgx v5 JSONB)",
			src:  `{"key":"value"}`,
			want: func() *json.RawMessage { v := json.RawMessage(`{"key":"value"}`); return &v }(),
		},
		{
			name: "[]byte source",
			src:  []byte(`{"arr":[1,2,3]}`),
			want: func() *json.RawMessage { v := json.RawMessage(`{"arr":[1,2,3]}`); return &v }(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var dest *json.RawMessage
			ns := &nullScanner{fieldPtr: &dest}
			if err := ns.Scan(tt.src); err != nil {
				t.Fatalf("Scan() error = %v", err)
			}
			if dest == nil {
				t.Fatal("expected dest to be non-nil after Scan")
			}
			if !reflect.DeepEqual(*dest, *tt.want) {
				t.Errorf("got %q, want %q", *dest, *tt.want)
			}
		})
	}
}

// TestNullScanner_PtrJsonRawMessage_Nil confirms that a NULL DB value leaves a
// *json.RawMessage field as nil (the nil early-return path fires before **T).
func TestNullScanner_PtrJsonRawMessage_Nil(t *testing.T) {
	var dest *json.RawMessage
	ns := &nullScanner{fieldPtr: &dest}
	if err := ns.Scan(nil); err != nil {
		t.Fatalf("Scan(nil) error = %v", err)
	}
	if dest != nil {
		t.Errorf("expected dest to remain nil, got %v", dest)
	}
}

// TestNullScanner_PtrJsonRawMessage_UnsupportedSrc confirms that an unsupported
// source type (e.g. int) returns an error for the **T path.
func TestNullScanner_PtrJsonRawMessage_UnsupportedSrc(t *testing.T) {
	var dest *json.RawMessage
	ns := &nullScanner{fieldPtr: &dest}
	if err := ns.Scan(42); err == nil {
		t.Error("expected error for unsupported source type, got nil")
	}
}

// TestNullScanner_PtrNamedByteSlice verifies the **T branch is not specific to
// json.RawMessage — any named []byte type works, matching the reflection-based
// implementation.
func TestNullScanner_PtrNamedByteSlice(t *testing.T) {
	type rawBytes []byte

	var dest *rawBytes
	ns := &nullScanner{fieldPtr: &dest}
	if err := ns.Scan(`hello`); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if dest == nil {
		t.Fatal("expected dest to be non-nil")
	}
	if string(*dest) != "hello" {
		t.Errorf("got %q, want %q", *dest, "hello")
	}
}

// TestFieldRecipientsFromValueOf_JsonRawMessage confirms that named []byte fields
// (e.g. json.RawMessage) are routed through nullScanner so that scanning works
// end-to-end rather than failing via database/sql's convertAssign.
func TestFieldRecipientsFromValueOf_JsonRawMessage(t *testing.T) {
	type Row struct {
		Data json.RawMessage
	}

	row := Row{}
	vod := reflect.ValueOf(&row).Elem()

	_, fieldMap, err := MapFromPtrType(&row, nil, nil)
	if err != nil {
		t.Fatalf("MapFromPtrType() error = %v", err)
	}

	logger := logging.NewGoTestingLogger(t)
	recipients := FieldRecipientsFromValueOf(logger, []string{"data"}, fieldMap, vod)
	if len(recipients) != 1 {
		t.Fatalf("expected 1 recipient, got %d", len(recipients))
	}

	ns, ok := recipients[0].(*nullScanner)
	if !ok {
		t.Fatalf("expected recipient to be *nullScanner, got %T", recipients[0])
	}

	// Scan a value through it to confirm the full path works.
	if err := ns.Scan(`{"hello":"world"}`); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	want := json.RawMessage(`{"hello":"world"}`)
	if !reflect.DeepEqual(row.Data, want) {
		t.Errorf("got %q, want %q", row.Data, want)
	}
}
