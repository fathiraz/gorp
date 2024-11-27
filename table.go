// Copyright 2012 James Cooper. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package gorp

import (
	"bytes"
	"fmt"
	"reflect"
	"slices"
	"strings"
)

// TableMap represents a mapping between a Go struct and a database table.
// Use dbmap.AddTable() or dbmap.AddTableWithName() to create these.
//
// Example:
//
//	type User struct {
//	    ID        int64  `db:"id"`
//	    Name      string `db:"name"`
//	    CreatedAt time.Time
//	}
//
//	// Basic table mapping
//	dbmap.AddTable(User{}).SetKeys(true, "ID")
//
//	// Custom table name with generic support
//	tm := NewTypedTableMap[User](dbmap.AddTableWithName(User{}, "users"))
//	tm.SetKeys(true, "ID").SetUniqueTogether("Name", "CreatedAt")
type TableMap struct {
	// Name of database table.
	TableName  string
	SchemaName string

	gotype         reflect.Type
	Columns        []*ColumnMap
	keys           []*ColumnMap
	indexes        []*IndexMap
	uniqueTogether [][]string
	version        *ColumnMap
	insertPlan     bindPlan
	updatePlan     bindPlan
	deletePlan     bindPlan
	getPlan        bindPlan
	dbmap          *DbMap

	// Cache maps for O(1) column lookups
	columnsByName  map[string]*ColumnMap
	columnsByField map[string]*ColumnMap
}

// TypedTableMap provides type-safe operations for a specific struct type
type TypedTableMap[T any] struct {
	*TableMap
}

// NewTypedTableMap creates a new TypedTableMap for type-safe operations
func NewTypedTableMap[T any](t *TableMap) *TypedTableMap[T] {
	return &TypedTableMap[T]{TableMap: t}
}

// ResetSql removes cached insert/update/select/delete SQL strings
// associated with this TableMap. Call this if you've modified
// any column names or the table name itself.
func (t *TableMap) ResetSql() {
	t.insertPlan = bindPlan{}
	t.updatePlan = bindPlan{}
	t.deletePlan = bindPlan{}
	t.getPlan = bindPlan{}
}

// SetKeys lets you specify the fields on a struct that map to primary
// key columns on the table. If isAutoIncr is true, result.LastInsertId()
// will be used after INSERT to bind the generated id to the Go struct.
//
// Example:
//
//	// Single auto-increment primary key
//	table.SetKeys(true, "ID")
//
//	// Composite primary key
//	table.SetKeys(false, "CountryCode", "PhoneNumber")
//
// Automatically calls ResetSql() to ensure SQL statements are regenerated.
//
// Panics if isAutoIncr is true and fieldNames length != 1
func (t *TableMap) SetKeys(isAutoIncr bool, fieldNames ...string) *TableMap {
	if isAutoIncr && len(fieldNames) != 1 {
		panic(fmt.Sprintf(
			"gorp: SetKeys: fieldNames length must be 1 if key is auto-increment. (Saw %v fieldNames)",
			len(fieldNames)))
	}
	t.keys = make([]*ColumnMap, 0)
	for _, name := range fieldNames {
		colmap := t.ColMap(name)
		colmap.isPK = true
		colmap.isAutoIncr = isAutoIncr
		t.keys = append(t.keys, colmap)
	}
	t.ResetSql()

	return t
}

// SetUniqueTogether lets you specify uniqueness constraints across multiple
// columns on the table. Each call adds an additional constraint for the
// specified columns.
//
// Example:
//
//	// Unique constraint on first_name, last_name
//	table.SetUniqueTogether("FirstName", "LastName")
//
//	// Another unique constraint on email, tenant_id
//	table.SetUniqueTogether("Email", "TenantID")
//
// Automatically calls ResetSql() to ensure SQL statements are regenerated.
//
// Panics if fieldNames length < 2.
func (t *TableMap) SetUniqueTogether(fieldNames ...string) *TableMap {
	if len(fieldNames) < 2 {
		panic(fmt.Sprintf(
			"gorp: SetUniqueTogether: must provide at least two fieldNames to set uniqueness constraint."))
	}

	columns := make([]string, 0, len(fieldNames))
	for _, name := range fieldNames {
		columns = append(columns, name)
	}

	for _, existingColumns := range t.uniqueTogether {
		if slices.Equal(existingColumns, columns) {
			return t
		}
	}
	t.uniqueTogether = append(t.uniqueTogether, columns)
	t.ResetSql()

	return t
}

// GetPrimaryKeys returns the primary key columns for this table
func (t *TableMap) GetPrimaryKeys() []*ColumnMap {
	return append([]*ColumnMap{}, t.keys...)
}

// GetColumns returns all columns for this table
func (t *TableMap) GetColumns() []*ColumnMap {
	return append([]*ColumnMap{}, t.Columns...)
}

// GetIndexes returns all indexes for this table
func (t *TableMap) GetIndexes() []*IndexMap {
	return append([]*IndexMap{}, t.indexes...)
}

// HasVersion returns true if this table has a version column
func (t *TableMap) HasVersion() bool {
	return t.version != nil
}

// GetVersionCol returns the version column if it exists, nil otherwise
func (t *TableMap) GetVersionCol() *ColumnMap {
	return t.version
}

// GetSchema returns the schema name for this table
func (t *TableMap) GetSchema() string {
	return t.SchemaName
}

// GetTableName returns the table name
func (t *TableMap) GetTableName() string {
	return t.TableName
}

// GetFullName returns the fully qualified table name (schema.table)
func (t *TableMap) GetFullName() string {
	if t.SchemaName != "" {
		return fmt.Sprintf("%s.%s", t.SchemaName, t.TableName)
	}
	return t.TableName
}

// ColMap returns the ColumnMap pointer matching the given struct field
// name. It panics if the struct does not contain a field matching this
// name.
//
// Example:
//
//	// Set max length of name column
//	table.ColMap("Name").SetMaxSize(50)
//
//	// Set multiple attributes
//	table.ColMap("Code").SetUnique(true).SetNotNull(true)
func (t *TableMap) ColMap(field string) *ColumnMap {
	col := t.colMapOrNil(field)
	if col == nil {
		panic(fmt.Sprintf("No ColumnMap in table %s type %s with field %s",
			t.TableName, t.gotype.Name(), field))
	}
	return col
}

// initColumnCaches initializes the column name caches if they don't exist
func (t *TableMap) initColumnCaches() {
	if t.columnsByName == nil {
		t.columnsByName = make(map[string]*ColumnMap, len(t.Columns))
		t.columnsByField = make(map[string]*ColumnMap, len(t.Columns))
		for _, col := range t.Columns {
			if col.ColumnName != "" {
				t.columnsByName[col.ColumnName] = col
			}
			if col.fieldName != "" {
				t.columnsByField[col.fieldName] = col
			}
		}
	}
}

// addColumn adds a column to the table and updates the caches
func (t *TableMap) addColumn(col *ColumnMap) {
	t.Columns = append(t.Columns, col)
	if t.columnsByName != nil {
		if col.ColumnName != "" {
			t.columnsByName[col.ColumnName] = col
		}
		if col.fieldName != "" {
			t.columnsByField[col.fieldName] = col
		}
	}
}

func (t *TableMap) colMapOrNil(field string) *ColumnMap {
	// Initialize caches if needed
	t.initColumnCaches()

	// Try field name first (most common case)
	if col, ok := t.columnsByField[field]; ok {
		return col
	}
	// Fall back to column name
	return t.columnsByName[field]
}

// IdxMap returns the IndexMap pointer matching the given index name.
//
// Example:
//
//	// Get existing index
//	idx := table.IdxMap("user_email_idx")
//
//	// Modify index properties
//	idx.SetUnique(true)
func (t *TableMap) IdxMap(field string) *IndexMap {
	for _, idx := range t.indexes {
		if idx.IndexName == field {
			return idx
		}
	}
	return nil
}

// AddIndex registers the index with gorp for specified table with given parameters.
// This operation is idempotent. If index is already mapped, the
// existing *IndexMap is returned.
//
// Example:
//
//	// Add B-tree index on email
//	table.AddIndex("user_email_idx", "btree", []string{"Email"})
//
//	// Add unique composite index
//	table.AddIndex("user_name_tenant_idx", "btree", []string{"Name", "TenantID"})
//
// Function will panic if one of the given index columns does not exist.
//
// Automatically calls ResetSql() to ensure SQL statements are regenerated.
func (t *TableMap) AddIndex(name string, idxtype string, columns []string) *IndexMap {
	// check if columns exists
	for _, column := range columns {
		if t.colMapOrNil(column) == nil {
			panic(fmt.Sprintf("No ColumnMap in table %s with field %s",
				t.TableName, column))
		}
	}

	for _, idx := range t.indexes {
		if idx.IndexName == name {
			return idx
		}
	}

	idx := &IndexMap{
		IndexName: name,
		IndexType: IndexType(idxtype),
		columns:   columns,
	}
	t.indexes = append(t.indexes, idx)
	t.ResetSql()
	return idx
}

// SetVersionCol sets the column to use as the Version field. By default
// the "Version" field is used. Returns the column found, or panics
// if the struct does not contain a field matching this name.
//
// Example:
//
//	// Use 'Revision' as version column
//	table.SetVersionCol("Revision")
//
// Automatically calls ResetSql() to ensure SQL statements are regenerated.
func (t *TableMap) SetVersionCol(field string) *ColumnMap {
	c := t.ColMap(field)
	t.version = c
	t.ResetSql()
	return c
}

// TypedColMap returns a type-safe ColumnMap for the TypedTableMap
func (t *TypedTableMap[T]) TypedColMap(field string) *TypedColumnMap[T] {
	return NewTypedColumnMap[T](t.ColMap(field))
}

// AddTypedIndex adds an index with type safety for the TypedTableMap
func (t *TypedTableMap[T]) AddTypedIndex(name string, idxtype string, columns []string) *IndexMap {
	return t.AddIndex(name, idxtype, columns)
}

// SqlForCreateTable gets a sequence of SQL commands that will create
// the specified table and any associated schema
func (t *TableMap) SqlForCreate(ifNotExists bool) string {
	s := bytes.Buffer{}
	dialect := t.dbmap.Dialect

	if strings.TrimSpace(t.SchemaName) != "" {
		schemaCreate := "CREATE SCHEMA"
		if ifNotExists {
			s.WriteString(dialect.IfSchemaNotExists(schemaCreate, t.SchemaName))
		} else {
			s.WriteString(schemaCreate)
		}
		s.WriteString(fmt.Sprintf(" %s;", t.SchemaName))
	}

	tableCreate := "CREATE TABLE"
	if ifNotExists {
		s.WriteString(dialect.IfTableNotExists(tableCreate, t.SchemaName, t.TableName))
	} else {
		s.WriteString(tableCreate)
	}
	s.WriteString(fmt.Sprintf(" %s (", dialect.QuotedTableForQuery(t.SchemaName, t.TableName)))

	x := 0
	for _, col := range t.Columns {
		if !col.Transient {
			if x > 0 {
				s.WriteString(", ")
			}
			stype := dialect.ToSqlType(col.gotype, ColumnOptions{
				MaxSize:    col.MaxSize,
				IsAutoIncr: col.isAutoIncr,
			})
			s.WriteString(fmt.Sprintf("%s %s", dialect.QuoteField(col.ColumnName), stype))

			if col.isPK || col.isNotNull {
				s.WriteString(" NOT NULL")
			}
			if col.isPK && len(t.keys) == 1 {
				s.WriteString(" PRIMARY KEY")
			}
			if col.Unique {
				s.WriteString(" UNIQUE")
			}
			if col.isAutoIncr {
				s.WriteString(fmt.Sprintf(" %s", dialect.AutoIncrStr()))
			}

			x++
		}
	}
	if len(t.keys) > 1 {
		s.WriteString(", PRIMARY KEY (")
		for x := range t.keys {
			if x > 0 {
				s.WriteString(", ")
			}
			s.WriteString(dialect.QuoteField(t.keys[x].ColumnName))
		}
		s.WriteString(")")
	}
	if len(t.uniqueTogether) > 0 {
		for _, columns := range t.uniqueTogether {
			s.WriteString(", UNIQUE (")
			for i, column := range columns {
				if i > 0 {
					s.WriteString(", ")
				}
				s.WriteString(dialect.QuoteField(column))
			}
			s.WriteString(")")
		}
	}
	s.WriteString(") ")
	s.WriteString(dialect.CreateTableSuffix())
	s.WriteString(dialect.QuerySuffix())
	return s.String()
}
