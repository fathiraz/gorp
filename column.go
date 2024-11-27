// Copyright 2012 James Cooper. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package gorp

import (
	"database/sql"
	"reflect"
	"sync"
)

// ColumnScanner represents any type that can be scanned from a database column.
type ColumnScanner interface {
	sql.Scanner
}

// ColumnMap represents a mapping between a Go struct field and a single
// column in a table.
// Unique and MaxSize only inform the
// CreateTables() function and are not used by Insert/Update/Delete/Get.
type ColumnMap struct {
	// Column name in db table
	ColumnName string

	// If true, this column is skipped in generated SQL statements
	Transient bool

	// If true, " unique" is added to create table statements.
	Unique bool

	// Query used for getting generated id after insert
	GeneratedIdQuery string

	// Passed to Dialect.ToSqlType() to assist in informing the
	// correct column type to map to in CreateTables()
	MaxSize int

	// DefaultValue specifies the default value for this column in CREATE TABLE statements.
	// The value will be passed through the dialect's ToSqlType method.
	DefaultValue string

	fieldName  string
	gotype     reflect.Type
	isPK       bool
	isAutoIncr bool
	isNotNull  bool

	// Cache for frequently accessed computed values
	sqlType     string
	sqlTypeOnce sync.Once
	scanner     reflect.Type
	scanned     bool
}

// TypedColumnMap provides type-safe column mapping for a specific Go type.
// It wraps the original ColumnMap with type information.
type TypedColumnMap[T any] struct {
	*ColumnMap
	value T
}

// NewTypedColumnMap creates a new TypedColumnMap for the given type.
// This allows for type-safe column operations.
//
// Example:
//
//	// For a string column
//	col := NewTypedColumnMap[string](existingColMap)
//
//	// For a custom type
//	type UserID int64
//	col := NewTypedColumnMap[UserID](existingColMap)
func NewTypedColumnMap[T any](c *ColumnMap) *TypedColumnMap[T] {
	return &TypedColumnMap[T]{
		ColumnMap: c,
	}
}

// Rename allows you to specify the column name in the table.
// Returns a type-safe column map.
//
// Example:
//
//	table.ColMap("Updated").Rename("date_updated")
//	// With type safety:
//	col := NewTypedColumnMap[time.Time](table.ColMap("Updated"))
//	col.Rename("date_updated")
func (c *ColumnMap) Rename(colname string) *ColumnMap {
	c.ColumnName = colname
	return c
}

// SetTransient allows you to mark the column as transient. If true
// this column will be skipped when SQL statements are generated.
//
// Example:
//
//	table.ColMap("TempField").SetTransient(true)
func (c *ColumnMap) SetTransient(b bool) *ColumnMap {
	c.Transient = b
	return c
}

// SetUnique adds "unique" to the create table statements for this
// column, if b is true.
//
// Example:
//
//	table.ColMap("Email").SetUnique(true)
func (c *ColumnMap) SetUnique(b bool) *ColumnMap {
	c.Unique = b
	return c
}

// SetNotNull adds "not null" to the create table statements for this
// column, if nn is true.
//
// Example:
//
//	table.ColMap("Required").SetNotNull(true)
func (c *ColumnMap) SetNotNull(nn bool) *ColumnMap {
	c.isNotNull = nn
	return c
}

// SetMaxSize specifies the max length of values of this column. This is
// passed to the dialect.ToSqlType() function, which can use the value
// to alter the generated type for "create table" statements.
//
// Example:
//
//	table.ColMap("Name").SetMaxSize(100) // VARCHAR(100)
func (c *ColumnMap) SetMaxSize(size int) *ColumnMap {
	c.MaxSize = size
	return c
}

// SetDefaultValue sets the default value for this column in CREATE TABLE statements.
// The value will be passed through the dialect's ToSqlType method.
//
// Example:
//
//	table.ColMap("Status").SetDefaultValue("active")
//	table.ColMap("CreatedAt").SetDefaultValue("CURRENT_TIMESTAMP")
func (c *ColumnMap) SetDefaultValue(value string) *ColumnMap {
	c.DefaultValue = value
	return c
}

// IsKey returns true if this column is configured as a primary key
func (c *ColumnMap) IsKey() bool {
	return c.isPK
}

// IsAutoIncrement returns true if this column is configured as auto-increment
func (c *ColumnMap) IsAutoIncrement() bool {
	return c.isAutoIncr
}

// IsNotNull returns true if this column is configured as NOT NULL
func (c *ColumnMap) IsNotNull() bool {
	return c.isNotNull
}
