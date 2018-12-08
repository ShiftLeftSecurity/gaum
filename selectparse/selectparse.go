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
	"regexp"
	"strings"

	"github.com/pkg/errors"
)

const (
	openParens  = '('
	closeParens = ')'
	comma       = ','
	escapeChar  = '\\'
	space       = ' '
)

// SelectParser contains the fields part of a SQL SELECT Statement and
// its parsed columns and respectives names and encapsulates the ability
// to produce said parsed data.
type SelectParser struct {
	Statement   string
	Columns     []string
	ColumnNames []string
}

func (s *SelectParser) splitFields() {
	var column = []string{}
	var depth = 0
	var nextIgnore = false
	for _, r := range s.Statement {
		if nextIgnore {
			nextIgnore = !nextIgnore
			column = append(column, string(r))
			continue
		}
		switch r {
		case openParens:
			depth++
		case closeParens:
			depth--
		case escapeChar:
			nextIgnore = !nextIgnore
		case comma:
			if depth == 0 {
				s.Columns = append(s.Columns, strings.Trim(strings.Join(column, ""), " "))
				column = []string{}
				continue
			}
		}
		column = append(column, string(r))
	}
	s.Columns = append(s.Columns, strings.Trim(strings.Join(column, ""), " "))
}

func (s *SelectParser) extractNames() error {
	s.ColumnNames = make([]string, len(s.Columns), len(s.Columns))
	for i, c := range s.Columns {
		// are we lucky enough to get column or table.column ?
		fromSimpleColumn := extractFromSingleWord(c)
		if fromSimpleColumn != "" {
			s.ColumnNames[i] = fromSimpleColumn
			continue
		}

		// is this perhaps column as label?
		fromAs := extractAsIfAny(c)
		if fromAs != "" {
			s.ColumnNames[i] = fromAs
			continue
		}

		// well of course it isn't life is complicated
		fromComplex := extractFromKeywordsOrFunc(c)
		if fromComplex != "" {
			s.ColumnNames[i] = fromComplex
			continue
		}
		return errors.Errorf("could not extract potential column name from %q please use AS in your query", c)
	}
	return nil
}

const as = " as "

func extractAsIfAny(column string) string {
	lowerColumn := strings.ToLower(column)
	potentials := strings.Split(lowerColumn, " as ")
	if len(potentials) == 1 {
		return ""
	}
	lastSegment := potentials[len(potentials)-1]
	if len(lastSegment) == 0 {
		return ""
	}
	for _, r := range lastSegment {
		switch r {
		case openParens, closeParens, comma:
			return ""
		}
	}
	return lastSegment
}

var wordRe = regexp.MustCompile("([\\.0-9a-z_-]+)")

func extractFromSingleWord(column string) string {
	lowerColumn := strings.ToLower(column)
	if wordRe.FindString(lowerColumn) != lowerColumn {
		return ""
	}
	// Extract table prefix if any
	parts := strings.Split(lowerColumn, ".")
	return parts[len(parts)-1]
}

func extractFromKeywordsOrFunc(column string) string {
	// IF this is a function call the column will be called after it, for instance
	// `DISTINCT some_wicked_pl(arg1, column, blah)` will most likely be called `some_wicked_pl`
	lowerColumn := strings.ToLower(column)
	if strings.HasPrefix(lowerColumn, string(openParens)) && strings.HasSuffix(lowerColumn, string(closeParens)) {
		// Honestly, why would you do that?
		lowerColumn = strings.TrimPrefix(strings.TrimSuffix(lowerColumn, ")"), "(")
	}
	buffer := []string{}
	previousToken := []string{}
	previousWasSpace := false
	depth := 0
	for _, r := range lowerColumn {
		switch r {
		case openParens:
			if depth == 0 && len(buffer) != 0 {
				previousToken = make([]string, len(buffer), len(buffer))
				copy(previousToken, buffer)
				buffer = []string{}
			}
			depth++
			previousWasSpace = false
			continue
		case closeParens:
			depth--
			previousWasSpace = false
			continue
		case space:
			if depth != 0 {
				continue
			}
			// At this point this might be a keyword
			if !previousWasSpace && len(buffer) != 0 {
				previousToken = make([]string, len(buffer), len(buffer))
				copy(previousToken, buffer)
				buffer = []string{}
			}
			previousWasSpace = true
			continue
		default:
			previousWasSpace = false
			// we dont care for things inside a function argument set
			if depth != 0 {
				continue
			}
		}
		buffer = append(buffer, string(r))
	}
	if len(buffer) != 0 && depth == 0 {
		return strings.Trim(strings.Join(buffer, ""), " ")
	}
	if len(previousToken) != 0 && depth == 0 {
		return strings.Trim(strings.Join(previousToken, ""), " ")
	}

	return ""
}
