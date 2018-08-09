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
	"fmt"
	//"strings"
	"testing"
)

func TestAscConstruction(t *testing.T) {

	fieldValue := "my_field"
	operation := Asc(fieldValue)

	if operation.others != nil {
		t.Fatal("Asc() should not by default populate the others field")
	}
	if operation.direction {
		t.Fatal("Asc() should set direction to false")
	}
	if len(operation.data) != 1 {
		t.Fatal("Asc() when give 1 field, should only populate 1 item")
	}
	if operation.data[0] != fieldValue {
		t.Fatalf("Expected value:(%s) Found value:(%s)", fieldValue, operation.data[0])
	}

	value := operation.String()
	result := fmt.Sprintf("%s ASC", fieldValue)
	if value != result {
		t.Fatalf("Expected value:(%s) Found value:(%s)", result, value)
	}
}

func TestDescConstruct(t *testing.T) {

	fieldValue := "my_field"
	operation := Desc(fieldValue)

	if operation.others != nil {
		t.Fatal("Desc() should not by default populate the others field")
	}
	if !operation.direction {
		t.Fatal("Desc() should set direction to true")
	}
	if len(operation.data) != 1 {
		t.Fatal("Desc() when give 1 field, should only populate 1 item")
	}
	if operation.data[0] != fieldValue {
		t.Fatalf("Expected value:(%s) Found value:(%s)", fieldValue, operation.data[0])
	}

	value := operation.String()
	result := fmt.Sprintf("%s DESC", fieldValue)
	if value != result {
		t.Fatalf("Expected value:(%s) Found value:(%s)", result, value)
	}
}

func TestDescConstructMultiple(t *testing.T) {

	fieldValue0 := "my_field"
	fieldValue1 := "my_other_field"
	operation := Desc(fieldValue0, fieldValue1)

	if operation.others != nil {
		t.Fatal("Desc() should not by default populate the others field")
	}
	if !operation.direction {
		t.Fatal("Desc() should set direction to true")
	}
	if len(operation.data) != 2 {
		t.Fatal("Desc() when give 2 fields, should only populate 2 item")
	}
	if operation.data[0] != fieldValue0 {
		t.Fatalf("Expected value:(%s) Found value:(%s)", fieldValue0, operation.data[0])
	}
	if operation.data[1] != fieldValue1 {
		t.Fatalf("Expected value:(%s) Found value:(%s)", fieldValue1, operation.data[1])
	}

	value := operation.String()
	result := fmt.Sprintf("%s DESC, %s DESC", fieldValue0, fieldValue1)
	if value != result {
		t.Fatalf("Expected value:(%s) Found value:(%s)", result, value)
	}
}

func TestAppendOperation(t *testing.T) {

	fieldValue0 := "my_field"
	fieldValue1 := "my_other_field"
	fieldValue2 := "another_field"

	operation0 := Desc(fieldValue0)
	operation1 := Desc(fieldValue1)
	operation2 := Desc(fieldValue2)

	if operation0.others != nil {
		t.Fatal("only chaining should modify others")
	}
	if operation1.others != nil {
		t.Fatal("only chaining should modify others")
	}
	if operation2.others != nil {
		t.Fatal("only chaining should modify others")
	}

	// append stuff
	operation0.append(operation1)
	operation0.append(operation2)
	if operation0.others == nil {
		t.Fatal("chaining operators should modify others")
	}
	if operation0.others.others == nil {
		t.Fatal("chaining operators should modify others")
	}

	// check everything is in the right place
	if operation0.others != operation1 {
		t.Fatal("chaining operators should modify others")
	}
	if operation0.others.others != operation2 {
		t.Fatal("chaining operators should modify others")
	}

	// build an output
	// since everything is DESC we should be good to go
	operationTest := Desc(fieldValue0, fieldValue1, fieldValue2)
	operationTestString := operationTest.String()
	operation0TestString := operation0.String()
	if operation0TestString != operationTestString {
		t.Fatalf("Expected these values will be identical. A:(%s) B:(%s)", operation0TestString, operationTestString)
	}
}

func TestSerializeMixed(t *testing.T) {

	type testData struct {
		orderBy *OrderByOperator
		output  string
	}

	tests := []testData{
		{
			orderBy: Desc("hello").Asc("world"),
			output:  "hello DESC, world ASC",
		},
		{
			orderBy: Desc("hello", "world").Asc("test"),
			output:  "hello DESC, world DESC, test ASC",
		},
		{
			orderBy: Desc("hello").Desc("world").Asc("test"),
			output:  "hello DESC, world DESC, test ASC",
		},
		{
			orderBy: Desc("hello").Asc("world").Asc("test"),
			output:  "hello DESC, world ASC, test ASC",
		},
		{
			orderBy: Desc("hello").Asc("world", "test"),
			output:  "hello DESC, world ASC, test ASC",
		},
		{
			orderBy: Desc("hello").Asc("").Asc("test"),
			output:  "hello DESC, test ASC",
		},
		{
			orderBy: Desc("hello").Desc("").Asc("test"),
			output:  "hello DESC, test ASC",
		},
		{
			orderBy: Desc("hello", "").Asc("test"),
			output:  "hello DESC, test ASC",
		},
		{
			orderBy: Desc("hello").Asc("", "test"),
			output:  "hello DESC, test ASC",
		},
		{
			orderBy: Desc("hello").Desc().Asc("test"),
			output:  "hello DESC, test ASC",
		},
		{
			orderBy: Desc("hello").Asc().Asc("test"),
			output:  "hello DESC, test ASC",
		},
	}

	for _, aTest := range tests {
		if aTest.output != aTest.orderBy.String() {
			t.Fatalf("Expected:(%s) Found:(%s)", aTest.output, aTest.orderBy.String())
		}
	}
}
