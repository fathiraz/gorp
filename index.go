// Copyright 2012 James Cooper. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package gorp

import (
	"fmt"
	"strings"
)

// IndexType represents supported index types for different dialects
type IndexType string

// Supported index types for different dialects
const (
	// Common index types
	IndexTypeBTree IndexType = "BTREE"
	IndexTypeHash  IndexType = "HASH"

	// PostgreSQL index types
	IndexTypeGiST IndexType = "GIST"
	IndexTypeGIN  IndexType = "GIN"

	// SQLite doesn't support specific index types
	IndexTypeSQLite IndexType = ""
)

// IndexMap represents a mapping between Go struct fields and a single
// index in a table. It supports both single and multi-column indexes
// with various index types depending on the dialect.
//
// Example usage:
//
//	// Single column index
//	t.AddIndex("user_email_idx", "BTREE", true, "Email")
//
//	// Multi-column index
//	t.AddIndex("user_name_idx", "BTREE", false, "FirstName", "LastName")
//
//	// Unique GIN index (PostgreSQL)
//	t.AddIndex("document_tokens_idx", "GIN", true, "Tokens")
type IndexMap struct {
	// Index name in db table
	IndexName string

	// If true, " unique" is added to create index statements.
	// Not used elsewhere
	Unique bool

	// Index type supported by Dialect
	// Postgres:  B-tree, Hash, GiST and GIN.
	// Mysql: Btree, Hash.
	// Sqlite: nil.
	IndexType IndexType

	// Columns name for single and multiple indexes
	columns []string
}

// NewIndex creates a new IndexMap with the specified parameters.
// It provides a fluent interface for index configuration.
//
// Example:
//
//	idx := NewIndex("user_email_idx").
//		SetType(IndexTypeBTree).
//		SetUnique(true).
//		AddColumns("Email")
func NewIndex(name string) *IndexMap {
	return &IndexMap{
		IndexName: name,
		columns:   make([]string, 0),
	}
}

// Rename allows you to specify the index name in the table.
//
// Example:
//
//	table.IndMap("customer_test_idx").Rename("customer_idx")
func (idx *IndexMap) Rename(indname string) *IndexMap {
	idx.IndexName = indname
	return idx
}

// SetUnique adds "unique" to the create index statements for this
// index, if b is true.
//
// Example:
//
//	idx.SetUnique(true) // CREATE UNIQUE INDEX ...
func (idx *IndexMap) SetUnique(b bool) *IndexMap {
	idx.Unique = b
	return idx
}

// SetType specifies the index type supported by the chosen SQL Dialect.
// Returns an error if the index type is not supported by the dialect.
//
// Example:
//
//	// PostgreSQL
//	idx.SetType(IndexTypeGIN)
//
//	// MySQL
//	idx.SetType(IndexTypeBtree)
func (idx *IndexMap) SetType(indexType IndexType) *IndexMap {
	idx.IndexType = indexType
	return idx
}

// AddColumns adds one or more columns to the index.
// The order of columns matters for multi-column indexes.
//
// Example:
//
//	// Single column
//	idx.AddColumns("Email")
//
//	// Multi-column index
//	idx.AddColumns("LastName", "FirstName")
func (idx *IndexMap) AddColumns(columns ...string) *IndexMap {
	idx.columns = append(idx.columns, columns...)
	return idx
}

// GetColumns returns the list of columns in this index
func (idx *IndexMap) GetColumns() []string {
	return append([]string{}, idx.columns...)
}

// IsUnique returns whether this index is unique
func (idx *IndexMap) IsUnique() bool {
	return idx.Unique
}

// GetType returns the index type
func (idx *IndexMap) GetType() IndexType {
	return idx.IndexType
}

// String returns a string representation of the index for debugging
func (idx *IndexMap) String() string {
	uniqueStr := ""
	if idx.Unique {
		uniqueStr = "UNIQUE "
	}
	typeStr := ""
	if idx.IndexType != "" {
		typeStr = fmt.Sprintf(" USING %s", idx.IndexType)
	}
	return fmt.Sprintf("%sINDEX %s%s ON (%s)",
		uniqueStr,
		idx.IndexName,
		typeStr,
		strings.Join(idx.columns, ", "))
}

// Validate checks if the index configuration is valid
func (idx *IndexMap) Validate() error {
	if idx.IndexName == "" {
		return fmt.Errorf("index name cannot be empty")
	}
	if len(idx.columns) == 0 {
		return fmt.Errorf("index %s must have at least one column", idx.IndexName)
	}
	return nil
}
