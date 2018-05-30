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
	"reflect"
	"strings"
	"unicode"

	"github.com/ShiftLeftSecurity/gaum/db/logging"
	"github.com/pkg/errors"
)

// ErrNoPointer indicates that the passed type is not a pointer.
var ErrNoPointer = errors.Errorf("destination needs to be pointer")

// ErrInquisition indicates that the type passed was not one expected.
var ErrInquisition = errors.Errorf("found an unexpected type")

const (
	// SubTagNameFieldName holds the name of a sub-tag containing the sql field for a struct attribute.
	SubTagNameFieldName = "field_name"
	// TagName holds the name of the tag that contains all of gaum possible sub tags.
	TagName = "gaum"
)

// nameFromTagOrName extracts field name from `gaum:"field_name:something"` or returns the
// field name.
func nameFromTagOrName(field reflect.StructField) string {
	tag := field.Tag
	tagText, ok := tag.Lookup(TagName)
	if ok {
		tagContents := strings.Split(tagText, ";")
		for _, segment := range tagContents {
			pair := strings.Split(segment, ":")
			if len(pair) != 2 {
				// TODO log when there is an invalid tag
				continue
			}
			tagName, tagValue := pair[0], pair[1]
			if tagName == SubTagNameFieldName {
				return tagValue
			}
		}
	}

	return camelsToSnakes(field.Name)
}

func camelsToSnakes(s string) string {
	snake := ""
	for i, v := range s {
		if unicode.IsUpper(v) {
			if i != 0 {
				snake += "_"
			}
			snake += string(unicode.ToLower(v))
		} else {
			snake += string(v)
		}
	}
	return snake
}

func snakesToCamels(s string) string {
	var c string
	var snake bool
	for i, v := range s {
		if i == 0 {
			c += strings.ToUpper(string(v))
			continue
		}
		if v == '_' {
			snake = true
			continue
		}
		if snake {
			c += strings.ToUpper(string(v))
			continue
		}
		c += string(v)
	}
	return c
}

// MapFromPtrType returns the name of the passed type, a map of field name to field or error.
func MapFromPtrType(aType interface{},
	include []reflect.Kind,
	exclude []reflect.Kind) (string, map[string]reflect.StructField, error) {
	tod := reflect.TypeOf(aType)
	if tod.Kind() != reflect.Ptr {
		return "", nil, errors.Wrapf(ErrNoPointer, "obtained: type %T, kind %v, content %#v",
			aType, tod.Kind(), aType)
	}
	tod = tod.Elem()
	return MapFromTypeOf(tod, include, exclude)
}

// MapFromTypeOf returns the name of the passed reflect.Type, a map of field name to field or error.
func MapFromTypeOf(tod reflect.Type,
	include []reflect.Kind,
	exclude []reflect.Kind) (string, map[string]reflect.StructField, error) {

	// Expect the passed in type to be any of these.
	if len(include) != 0 {
		expected := false
		for _, k := range include {
			if tod.Kind() == k {
				expected = true
				break
			}
		}
		if !expected {
			return "", nil, errors.Wrapf(ErrInquisition,
				"did not expect type to be one of %#v", include)
		}
	}

	// Expect the passed in type to be none of these.
	if len(exclude) != 0 {
		for _, k := range exclude {
			if tod.Kind() == k {
				return "", nil, errors.Wrapf(ErrInquisition,
					"did not expect passed type to be of kind %s", k)
			}
		}
	}

	// We want the inner component.
	if tod.Kind() == reflect.Slice {
		// If this is a slice I want the type of the slice.
		tod = tod.Elem()
	}

	typeName := tod.Name()
	fieldMap := make(map[string]reflect.StructField, tod.NumField())
	for fieldIndex := 0; fieldIndex < tod.NumField(); fieldIndex++ {
		field := tod.Field(fieldIndex)
		name := nameFromTagOrName(field)
		fieldMap[name] = field
	}
	return typeName, fieldMap, nil
}

// FieldNamesFromType returns a list of strings with the field names for sql extracted from a type
func FieldNamesFromType(aType interface{}) []string {
	tod := reflect.TypeOf(aType)
	fields := []string{}
	for fieldIndex := 0; fieldIndex < tod.NumField(); fieldIndex++ {
		field := tod.Field(fieldIndex)
		name := nameFromTagOrName(field)
		fields = append(fields, name)
	}
	return fields
}

// FieldRecipientsFromType returns an array of pointer to attributes from the passed in instance.
func FieldRecipientsFromType(logger logging.Logger, sqlFields []string,
	fieldMap map[string]reflect.StructField, aType interface{}) []interface{} {
	vod := reflect.ValueOf(aType)
	if vod.Type().Kind() == reflect.Ptr {
		vod = vod.Elem()
	}
	return FieldRecipientsFromValueOf(logger, sqlFields, fieldMap, vod)
}

// FieldRecipientsFromValueOf returns an array of pointer to attributes fomr the passed
// in reflect.Value.
func FieldRecipientsFromValueOf(logger logging.Logger, sqlFields []string,
	fieldMap map[string]reflect.StructField, vod reflect.Value) []interface{} {
	fieldRecipients := make([]interface{}, len(sqlFields), len(sqlFields))
	for i, field := range sqlFields {

		// TODO, check datatype compatibility or let it burn?
		fVal, ok := fieldMap[field]
		if !ok {
			var empty interface{}
			fieldRecipients[i] = empty
			continue
		}
		fieldRecipients[i] = vod.FieldByIndex(fVal.Index).Addr().Interface()
	}
	return fieldRecipients
}
