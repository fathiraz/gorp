// Copyright 2012 James Cooper. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package gorp_test

import (
	"testing"
	"time"

	"github.com/go-gorp/gorp/v3"
	"github.com/stretchr/testify/suite"
)

type TableTestSuite struct {
	suite.Suite
}

func TestTableTestSuite(t *testing.T) {
	suite.Run(t, new(TableTestSuite))
}

// Test struct types
type TestUser struct {
	ID        int64     `db:"id"`
	Name      string    `db:"name"`
	Email     string    `db:"email"`
	CreatedAt time.Time `db:"created_at"`
	TenantID  int64     `db:"tenant_id"`
	Version   int       `db:"version"`
}

type TestUserWithCustomVersion struct {
	ID       int64  `db:"id"`
	Name     string `db:"name"`
	Revision int    `db:"revision"`
}

func (s *TableTestSuite) TestBasicTableMap() {
	// Create a new DbMap
	dbmap := &gorp.DbMap{}

	// Test basic table mapping
	table := dbmap.AddTable(TestUser{})
	s.Equal("TestUser", table.TableName)
	s.Equal("", table.SchemaName)
	s.Equal(6, len(table.GetColumns()))
}

func (s *TableTestSuite) TestTableWithCustomName() {
	dbmap := &gorp.DbMap{}

	// Test table with custom name
	table := dbmap.AddTableWithName(TestUser{}, "users")
	s.Equal("users", table.TableName)
	s.Equal("", table.SchemaName)
}

func (s *TableTestSuite) TestPrimaryKeys() {
	dbmap := &gorp.DbMap{}
	table := dbmap.AddTable(TestUser{})

	// Test single auto-increment primary key
	table.SetKeys(true, "ID")
	keys := table.GetPrimaryKeys()
	s.Equal(1, len(keys))
	s.True(keys[0].IsKey())
	s.True(keys[0].IsAutoIncrement())
	s.Equal("id", keys[0].ColumnName)

	// Test panic on multiple auto-increment keys
	s.Panics(func() {
		table.SetKeys(true, "ID", "TenantID")
	})

	// Test composite primary key
	table2 := dbmap.AddTableWithName(TestUser{}, "users_composite")
	table2.SetKeys(false, "ID", "TenantID")
	keys = table2.GetPrimaryKeys()
	s.Equal(2, len(keys))
	s.True(keys[0].IsKey())
	s.False(keys[0].IsAutoIncrement())
	s.True(keys[1].IsKey())
	s.False(keys[1].IsAutoIncrement())
}

func (s *TableTestSuite) TestColumnMapping() {
	dbmap := &gorp.DbMap{}
	table := dbmap.AddTable(TestUser{})

	// Test column retrieval
	col := table.ColMap("Name")
	s.NotNil(col)
	s.Equal("name", col.ColumnName)

	// Test panic on non-existent column
	s.Panics(func() {
		table.ColMap("NonExistentField")
	})

	// Test column attributes
	col.SetMaxSize(50)
	col.SetUnique(true)
	col.SetNotNull(true)
	s.Equal(50, col.MaxSize)
	s.True(col.Unique)
	s.True(col.IsNotNull())
}

func (s *TableTestSuite) TestVersioning() {
	dbmap := &gorp.DbMap{}
	table := dbmap.AddTable(TestUser{})

	// Test default version column
	table.SetVersionCol("Version")
	s.True(table.HasVersion())
	s.Equal("version", table.GetVersionCol().ColumnName)

	// Test custom version column
	table2 := dbmap.AddTable(TestUserWithCustomVersion{})
	table2.SetVersionCol("Revision")
	s.True(table2.HasVersion())
	s.Equal("revision", table2.GetVersionCol().ColumnName)

	// Test panic on non-existent version column
	s.Panics(func() {
		table.SetVersionCol("NonExistentVersion")
	})
}

func (s *TableTestSuite) TestIndexing() {
	dbmap := &gorp.DbMap{}
	table := dbmap.AddTable(TestUser{})

	// Test adding B-tree index
	idx1 := table.AddIndex("user_email_idx", "btree", []string{"Email"})
	s.NotNil(idx1)
	s.Equal("user_email_idx", idx1.IndexName)
	s.Equal("btree", string(idx1.IndexType))  // Convert IndexType to string for comparison
	s.Equal(1, len(idx1.GetColumns()))

	// Test adding composite index
	idx2 := table.AddIndex("user_name_tenant_idx", "btree", []string{"Name", "TenantID"})
	s.NotNil(idx2)
	s.Equal(2, len(idx2.GetColumns()))

	// Test index retrieval
	idx := table.IdxMap("user_email_idx")
	s.NotNil(idx)
	s.Equal("user_email_idx", idx.IndexName)

	// Test panic on invalid column
	s.Panics(func() {
		table.AddIndex("invalid_idx", "btree", []string{"NonExistentColumn"})
	})
}

func (s *TableTestSuite) TestTypedTableMap() {
	dbmap := &gorp.DbMap{}
	table := dbmap.AddTable(TestUser{})

	// Test typed table map creation
	typedTable := gorp.NewTypedTableMap[TestUser](table)
	s.NotNil(typedTable)
	s.Equal(table, typedTable.TableMap)

	// Test typed column mapping
	col := typedTable.TypedColMap("Name")
	s.NotNil(col)
	s.Equal("name", col.ColumnName)

	// Test typed index creation
	idx := typedTable.AddTypedIndex("user_email_idx", "btree", []string{"Email"})
	s.NotNil(idx)
	s.Equal("user_email_idx", idx.IndexName)
}

func (s *TableTestSuite) TestSqlGeneration() {
	dbmap := &gorp.DbMap{}
	dialect := &gorp.MySQLDialect{Engine: "InnoDB", Encoding: "UTF8"}
	dbmap.Dialect = dialect

	table := dbmap.AddTable(TestUser{})
	table.SetKeys(true, "ID")

	// Test SQL generation with IF NOT EXISTS
	sql := table.SqlForCreate(true)
	s.Contains(sql, "IF NOT EXISTS")
	s.Contains(sql, "CREATE TABLE")
	s.Contains(sql, "id")
	s.Contains(sql, "name")
	s.Contains(sql, "email")
	s.Contains(sql, "created_at")
	s.Contains(sql, "tenant_id")
	s.Contains(sql, "version")

	// Test SQL generation without IF NOT EXISTS
	sql = table.SqlForCreate(false)
	s.NotContains(sql, "IF NOT EXISTS")
}

func (s *TableTestSuite) TestSchemaHandling() {
	dbmap := &gorp.DbMap{}

	// Test table with schema
	table := dbmap.AddTableWithNameAndSchema(TestUser{}, "app", "users")
	s.Equal("app", table.GetSchema())
	s.Equal("users", table.GetTableName())
	s.Equal("app.users", table.GetFullName())

	// Test table without schema
	table2 := dbmap.AddTableWithName(TestUser{}, "users2")
	s.Equal("app", table2.GetSchema())
	s.Equal("users2", table2.GetTableName())
	s.Equal("app.users2", table2.GetFullName())
}
