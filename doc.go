// Copyright 2012 James Cooper. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

/*
Package gorp provides a simple yet powerful ORM (Object-Relational Mapping) for Go,
allowing you to marshal Go structs to and from SQL databases. It uses the database/sql
package and works with any compliant database/sql driver.

# Supported Databases

Gorp supports multiple database dialects out of the box:
  - MySQL
  - PostgreSQL
  - SQLite
  - Oracle
  - SQLServer
  - Snowflake

# Key Features

  - Automatic table creation from struct definitions
  - Support for multiple primary key fields
  - Support for embedding and inheritance
  - Support for relationships (one-to-many, many-to-many)
  - Pre/post hooks for create, update, delete operations
  - Transaction support with commit/rollback hooks
  - Comprehensive error handling with type-safe errors
  - Support for raw SQL queries and custom table/column naming
  - Optimistic locking using a version column

# Basic Usage

Here's a quick example of how to use gorp:

	type Post struct {
		Id      int64  `db:"post_id"`
		Created int64
		Title   string `db:"title"`
		Body    string `db:"body"`
	}

	// Initialize DbMap
	dbmap := &gorp.DbMap{Db: db, Dialect: gorp.MySQLDialect{}}

	// Register table
	dbmap.AddTableWithName(Post{}, "posts").SetKeys(true, "Id")

	// Create table
	err := dbmap.CreateTablesIfNotExists()

	// Insert row
	post := &Post{Title: "My Post", Body: "Hello"}
	err = dbmap.Insert(post)

	// Update
	post.Title = "Updated Title"
	_, err = dbmap.Update(post)

	// Select
	var posts []Post
	_, err = dbmap.Select(&posts, "select * from posts")

	// Delete
	_, err = dbmap.Delete(post)

# Advanced Features

Hooks:
Gorp supports hooks that are called before/after operations:

	func (p *Post) PreInsert(s gorp.SqlExecutor) error {
		p.Created = time.Now().UnixNano()
		return nil
	}

Transactions:
Gorp provides transaction support with commit/rollback hooks:

	trans, err := dbmap.Begin()
	if err != nil {
		return err
	}

	// do work
	post1 := &Post{Title: "Post 1"}
	post2 := &Post{Title: "Post 2"}
	err = trans.Insert(post1, post2)

	if err != nil {
		trans.Rollback()
		return err
	}

	return trans.Commit()

Custom Types:
Gorp allows you to define custom types that implement the Scanner and Valuer interfaces:

	type CustomInt int

	func (c *CustomInt) Scan(value interface{}) error {
		// implement
	}

	func (c CustomInt) Value() (driver.Value, error) {
		// implement
	}

# Error Handling

Gorp provides structured error types for common database operations:

	switch err.(type) {
	case *gorp.NoFieldInTypeError:
		// handle missing field
	case *gorp.TableNotFoundError:
		// handle missing table
	case *gorp.ColumnNotFoundError:
		// handle missing column
	}

# Best Practices

 1. Always close your database connection:
    defer dbmap.Db.Close()

 2. Use transactions for multiple operations:
    tx, err := dbmap.Begin()
    defer tx.Rollback() // will be ignored if Commit() is called
    // do work
    return tx.Commit()

 3. Use prepared statements for repeated operations:
    stmt, err := dbmap.Prepare("select * from posts where id = ?")
    defer stmt.Close()

 4. Use table/column naming conventions:
    dbmap.AddTableWithName(Post{}, "posts").SetKeys(true, "Id")

Source code and project home: https://github.com/go-gorp/gorp
*/
package gorp
