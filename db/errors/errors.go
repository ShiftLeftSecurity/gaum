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

package errors

import pkgErrors "github.com/pkg/errors"

// ErrNoRows should be returned when a query that is supposed to yield results does not.
var ErrNoRows = pkgErrors.New("no rows in result set")

// NoTX is encountered when an operation is done that assumes a transaction exists, but isn't present
var NoTX = pkgErrors.New("transaction does not exist")

// NoDB is encountered when an operation is preformed without a valid transaction or connection to the DB
var NoDB = pkgErrors.New("neither transaction or database connection exists")

// AlreadyInTX is encountered when one attempts to start a transaction within a transaction, recursive transactions are not supported at this time.
var AlreadyInTX = pkgErrors.New("cannot begin a transaction within a transaction")

// NotImplemented is returned when a feature not on a driver is invoked
var NotImplemented = pkgErrors.New("not implemented for this driver")
