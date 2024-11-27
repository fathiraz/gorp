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

type ColumnTestSuite struct {
	suite.Suite
}

func TestColumnTestSuite(t *testing.T) {
	suite.Run(t, new(ColumnTestSuite))
}

func (s *ColumnTestSuite) TestColumnMapBasicOperations() {
	col := &gorp.ColumnMap{
		ColumnName: "original_name",
	}

	// Test Rename
	col.Rename("new_name")
	s.Equal("new_name", col.ColumnName)

	// Test SetTransient
	s.False(col.Transient)
	col.SetTransient(true)
	s.True(col.Transient)
	col.SetTransient(false)
	s.False(col.Transient)

	// Test SetUnique
	s.False(col.Unique)
	col.SetUnique(true)
	s.True(col.Unique)
	col.SetUnique(false)
	s.False(col.Unique)

	// Test SetNotNull
	s.False(col.IsNotNull())
	col.SetNotNull(true)
	s.True(col.IsNotNull())
	col.SetNotNull(false)
	s.False(col.IsNotNull())

	// Test SetMaxSize
	s.Equal(0, col.MaxSize)
	col.SetMaxSize(100)
	s.Equal(100, col.MaxSize)

	// Test SetDefaultValue
	s.Equal("", col.DefaultValue)
	col.SetDefaultValue("test_value")
	s.Equal("test_value", col.DefaultValue)
}

func (s *ColumnTestSuite) TestTypedColumnMap() {
	// Test with string type
	col := &gorp.ColumnMap{ColumnName: "string_col"}
	typedCol := gorp.NewTypedColumnMap[string](col)
	s.Equal(col, typedCol.ColumnMap)

	// Test with custom type
	type UserID int64
	idCol := &gorp.ColumnMap{ColumnName: "id_col"}
	typedIDCol := gorp.NewTypedColumnMap[UserID](idCol)
	s.Equal(idCol, typedIDCol.ColumnMap)

	// Test with time.Time
	timeCol := &gorp.ColumnMap{ColumnName: "time_col"}
	typedTimeCol := gorp.NewTypedColumnMap[time.Time](timeCol)
	s.Equal(timeCol, typedTimeCol.ColumnMap)
}

func (s *ColumnTestSuite) TestColumnMapChaining() {
	// Test method chaining
	col := &gorp.ColumnMap{ColumnName: "test_col"}
	col.SetTransient(true).
		SetUnique(true).
		SetNotNull(true).
		SetMaxSize(255).
		SetDefaultValue("default")

	s.True(col.Transient)
	s.True(col.Unique)
	s.True(col.IsNotNull())
	s.Equal(255, col.MaxSize)
	s.Equal("default", col.DefaultValue)
}

func (s *ColumnTestSuite) TestColumnMapKeyOperations() {
	col := &gorp.ColumnMap{ColumnName: "id"}

	// Test IsKey
	s.False(col.IsKey())

	// Test IsAutoIncrement
	s.False(col.IsAutoIncrement())

	// Test GeneratedIdQuery
	s.Equal("", col.GeneratedIdQuery)
	col.GeneratedIdQuery = "SELECT LAST_INSERT_ID()"
	s.Equal("SELECT LAST_INSERT_ID()", col.GeneratedIdQuery)
}
