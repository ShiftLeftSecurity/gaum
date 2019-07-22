//    Copyright 2019 Horacio Duran <horacio@shiftleft.io>, ShiftLeft Inc.
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
	"io"
	"text/template"

	"github.com/pkg/errors"
)

// Formatter holds a set of key/values for replacements in queries generated by gaum, this
// is designed around tablename aliases.
type Formatter struct {
	FormatTable map[string]string
}

// TablePrefixes returns the formatter for this expression, if none exists one will be
// created
func (ec *ExpresionChain) TablePrefixes() *Formatter {
	if ec.formatter == nil {
		ec.formatter = &Formatter{
			FormatTable: map[string]string{},
		}
	}
	return ec.formatter
}

func (ec *ExpresionChain) populateTablePrefixes() error {
	return nil
}

func (f *Formatter) format(src string, dst io.Writer) error {
	tmpl, err := template.New("query").Parse(src)
	if err != nil {
		return errors.Wrap(err, "parsing the query")
	}
	return tmpl.Execute(dst, f.FormatTable)
}

// List returns a list of the keys for table prefixes.
func (f *Formatter) List() []string {
	keys := make([]string, 0, len(f.FormatTable))
	for k := range f.FormatTable {
		keys = append(keys, k)
	}
	return keys
}

// Add adds the passed in prefix to the the Formatting table, returns "replaced"
func (f *Formatter) Add(key, prefix string) bool {
	_, ok := f.FormatTable[key]
	f.FormatTable[key] = prefix
	return ok
}

// Del removes the passed key, if exists, from the formatting table.
func (f *Formatter) Del(key string) {
	delete(f.FormatTable, key)
}
