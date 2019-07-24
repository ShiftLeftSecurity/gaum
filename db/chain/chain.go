package chain

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

import (
	"strings"
	"sync"

	"github.com/ShiftLeftSecurity/gaum/db/connection"
	"github.com/pkg/errors"
)

// NewExpressionChain returns a new instance of ExpressionChain hooked to the passed DB
// Deprecated: please use New instead
func NewExpressionChain(db connection.DB) *ExpressionChain {
	return &ExpressionChain{db: db}
}

// NewNoDB creates an expression chain withouth the db, mostly with the purpose of making a more
// abbreviated syntax for transient ExpresionChains such as CTE or subquery ones.
func NewNoDB() *ExpressionChain {
	return &ExpressionChain{}
}

// New returns a new instance of ExpressionChain hooked to the passed DB
func New(db connection.DB) *ExpressionChain {
	return NewExpressionChain(db)
}

// ExpressionChain holds all the atoms for the SQL expressions that make a query and allows to chain
// more assuming the chaining is valid.
type ExpressionChain struct {
	lock          sync.Mutex
	segments      []querySegmentAtom
	table         string
	mainOperation *querySegmentAtom
	ctes          map[string]*ExpressionChain
	ctesOrder     []string // because deterministic tests and co-dependency

	limit  *querySegmentAtom
	offset *querySegmentAtom

	set string

	conflict *OnConflict
	err      []error

	db connection.DB

	formatter    *Formatter
	minQuerySize uint64
}

// SetMinQuerySize will make sure that at least <size> bytes (runes actually) are allocated
// before rendering to avoid costly resize and copy operations while rendering, use only
// if you know what you are doing, 0 uses go allocator.
func (ec *ExpressionChain) SetMinQuerySize(size uint64) {
	ec.minQuerySize = size
}

// Set will produce your chain to be run inside a Transaction and used for `SET LOCAL`
// For the moment this is only used with Exec.
func (ec *ExpressionChain) Set(set string) *ExpressionChain {
	ec.set = set
	return ec
}

// NewDB sets the passed db as this chain's db.
func (ec *ExpressionChain) NewDB(db connection.DB) *ExpressionChain {
	ec.db = db
	return ec
}

// DB returns the chain DB
func (ec *ExpressionChain) DB() connection.DB {
	return ec.db
}

// Clone returns a copy of the ExpressionChain
func (ec *ExpressionChain) Clone() *ExpressionChain {
	var limit *querySegmentAtom
	var offset *querySegmentAtom
	var mainOperation *querySegmentAtom
	if ec.limit != nil {
		eclimit := ec.limit.clone()
		limit = &eclimit
	}
	if ec.offset != nil {
		ecoffset := ec.offset.clone()
		offset = &ecoffset
	}
	if ec.mainOperation != nil {
		ecmainOperation := ec.mainOperation.clone()
		mainOperation = &ecmainOperation
	}
	segments := make([]querySegmentAtom, len(ec.segments))
	for i, s := range ec.segments {
		segments[i] = s.clone()
	}
	ctes := make(map[string]*ExpressionChain, len(ec.ctes))
	order := make([]string, len(ec.ctesOrder), len(ec.ctesOrder))
	for i, k := range ec.ctesOrder {
		ctes[k] = ec.ctes[k].Clone()
		order[i] = k
	}
	newFormatter := Formatter{FormatTable: map[string]string{}}
	for k, v := range ec.TablePrefixes().FormatTable {
		newFormatter.FormatTable[k] = v
	}
	return &ExpressionChain{
		limit:         limit,
		offset:        offset,
		segments:      segments,
		mainOperation: mainOperation,
		table:         ec.table,
		ctes:          ctes,
		ctesOrder:     order,

		db: ec.db,

		formatter:    &newFormatter,
		minQuerySize: ec.minQuerySize,
	}
}

func (ec *ExpressionChain) setLimit(limit *querySegmentAtom) {
	ec.lock.Lock()
	defer ec.lock.Unlock()
	ec.limit = limit
}

func (ec *ExpressionChain) setOffset(offset *querySegmentAtom) {
	ec.lock.Lock()
	defer ec.lock.Unlock()
	ec.offset = offset
}

func (ec *ExpressionChain) setTable(table string) {
	ec.lock.Lock()
	defer ec.lock.Unlock()
	// This will override whetever has been set and might be in turn ignored if the finalization
	// method used (ie Find(Object)) specifies one.
	ec.table = table
}

func (ec *ExpressionChain) append(atom querySegmentAtom) {
	ec.lock.Lock()
	defer ec.lock.Unlock()
	ec.segments = append(ec.segments, atom)
}

func (ec *ExpressionChain) removeOfType(atomType sqlSegment) {
	ec.lock.Lock()
	defer ec.lock.Unlock()
	newSegments := []querySegmentAtom{}
	for i, s := range ec.segments {
		if s.segment == atomType {
			continue
		}
		newSegments = append(newSegments, ec.segments[i])
	}
	ec.segments = newSegments
}

func segmentsPresent(ec *ExpressionChain, seg sqlSegment) int {
	segmentCount := 0
	for _, item := range ec.segments {
		if item.segment == seg {
			segmentCount++
		}
	}
	return segmentCount
}

func extract(ec *ExpressionChain, seg sqlSegment) []querySegmentAtom {
	qs := []querySegmentAtom{}
	for _, item := range ec.segments {
		if item.segment == seg {
			qs = append(qs, item)
		}
	}
	return qs
}

// fetchErrors is a private thingy for checking if errors exist
func (ec *ExpressionChain) hasErr() bool {
	return len(ec.err) > 0
}

// getErr returns an error message about the stuff
func (ec *ExpressionChain) getErr() error {
	if ec.err == nil {
		return nil
	}
	errMsg := make([]string, len(ec.err))
	for index, anErr := range ec.err {
		errMsg[index] = anErr.Error()
	}
	return errors.New(strings.Join(errMsg, " "))
}
