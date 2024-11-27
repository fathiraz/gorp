// Copyright 2012 James Cooper. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package gorp_test

import (
	"errors"
	"testing"

	"github.com/go-gorp/gorp/v3"
	"github.com/stretchr/testify/suite"
)

type ErrorsTestSuite struct {
	suite.Suite
}

func TestErrorsTestSuite(t *testing.T) {
	suite.Run(t, new(ErrorsTestSuite))
}

func (s *ErrorsTestSuite) TestNoFieldInTypeError() {
	// Test with single missing column
	err1 := &gorp.NoFieldInTypeError{
		TypeName:        "User",
		MissingColNames: []string{"age"},
	}
	s.Equal("gorp: no fields [age] in type User", err1.Error())

	// Test with multiple missing columns
	err2 := &gorp.NoFieldInTypeError{
		TypeName:        "Product",
		MissingColNames: []string{"price", "quantity", "category"},
	}
	s.Equal("gorp: no fields [price quantity category] in type Product", err2.Error())

	// Test with empty missing columns
	err3 := &gorp.NoFieldInTypeError{
		TypeName:        "Empty",
		MissingColNames: []string{},
	}
	s.Equal("gorp: no fields [] in type Empty", err3.Error())
}

func (s *ErrorsTestSuite) TestInvalidDialectError() {
	// Test with standard SQL dialect
	err1 := &gorp.InvalidDialectError{
		Dialect: "mysql",
	}
	s.Equal("gorp: invalid dialect specified: mysql", err1.Error())

	// Test with empty dialect
	err2 := &gorp.InvalidDialectError{
		Dialect: "",
	}
	s.Equal("gorp: invalid dialect specified: ", err2.Error())

	// Test with non-standard dialect
	err3 := &gorp.InvalidDialectError{
		Dialect: "custom-dialect",
	}
	s.Equal("gorp: invalid dialect specified: custom-dialect", err3.Error())
}

func (s *ErrorsTestSuite) TestTableNotFoundError() {
	// Test without schema
	err1 := &gorp.TableNotFoundError{
		Table: "users",
	}
	s.Equal("gorp: table users not found", err1.Error())

	// Test with schema
	err2 := &gorp.TableNotFoundError{
		Schema: "app",
		Table:  "products",
	}
	s.Equal("gorp: table app.products not found", err2.Error())

	// Test with empty table name
	err3 := &gorp.TableNotFoundError{
		Table: "",
	}
	s.Equal("gorp: table  not found", err3.Error())

	// Test with empty schema but table name
	err4 := &gorp.TableNotFoundError{
		Schema: "",
		Table:  "orders",
	}
	s.Equal("gorp: table orders not found", err4.Error())
}

func (s *ErrorsTestSuite) TestNonFatalError() {
	// Test nil error
	s.False(gorp.NonFatalError(nil))

	// Test NoFieldInTypeError (should be non-fatal)
	noFieldErr := &gorp.NoFieldInTypeError{
		TypeName:        "User",
		MissingColNames: []string{"age"},
	}
	s.True(gorp.NonFatalError(noFieldErr))

	// Test TableNotFoundError (should be non-fatal)
	tableNotFoundErr := &gorp.TableNotFoundError{
		Table: "users",
	}
	s.True(gorp.NonFatalError(tableNotFoundErr))

	// Test InvalidDialectError (should be fatal)
	dialectErr := &gorp.InvalidDialectError{
		Dialect: "invalid",
	}
	s.False(gorp.NonFatalError(dialectErr))

	// Test standard error (should be fatal)
	standardErr := errors.New("standard error")
	s.False(gorp.NonFatalError(standardErr))
}
