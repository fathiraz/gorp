// Copyright 2012 James Cooper. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package gorp

import (
	"fmt"
)

// NoFieldInTypeError is returned when a select query returns columns that do not exist
// as fields in the struct it is being mapped to.
// This is a non-fatal error, as encoding/json silently ignores missing fields.
type NoFieldInTypeError struct {
	TypeName        string   // Name of the struct type
	MissingColNames []string // Names of columns that don't have corresponding struct fields
}

func (err *NoFieldInTypeError) Error() string {
	return fmt.Sprintf("gorp: no fields %+v in type %s", err.MissingColNames, err.TypeName)
}

// InvalidDialectError is returned when an unsupported or invalid dialect is specified
type InvalidDialectError struct {
	Dialect string // Name of the invalid dialect
}

func (err *InvalidDialectError) Error() string {
	return fmt.Sprintf("gorp: invalid dialect specified: %s", err.Dialect)
}

// TableNotFoundError is returned when operations are attempted on a table that doesn't exist
type TableNotFoundError struct {
	Table  string // Name of the missing table
	Schema string // Optional schema name
}

func (err *TableNotFoundError) Error() string {
	if err.Schema != "" {
		return fmt.Sprintf("gorp: table %s.%s not found", err.Schema, err.Table)
	}
	return fmt.Sprintf("gorp: table %s not found", err.Table)
}

// NonFatalError returns true if the error is non-fatal (i.e., we shouldn't immediately return).
// Non-fatal errors include:
// - NoFieldInTypeError: when select results contain columns not in the target struct
// - TableNotFoundError: when a table is not found (allows for table creation)
func NonFatalError(err error) bool {
	if err == nil {
		return false
	}
	switch err.(type) {
	case *NoFieldInTypeError, *TableNotFoundError:
		return true
	default:
		return false
	}
}
