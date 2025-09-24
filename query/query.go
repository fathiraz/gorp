// Package query provides fluent, type-safe query builders using Go 1.24 generics
package query

import (
	"context"
	"fmt"
	"strings"
)

// QueryBuilder provides a fluent interface for building SQL queries
type QueryBuilder[T any] struct {
	selectFields []string
	fromTable    string
	whereClause  []string
	orderByClause []string
	limitValue   *int
	offsetValue  *int
	args         []interface{}
	ctx          context.Context
}

// NewQueryBuilder creates a new QueryBuilder for type T
func NewQueryBuilder[T any](ctx context.Context) *QueryBuilder[T] {
	return &QueryBuilder[T]{
		selectFields: make([]string, 0),
		whereClause:  make([]string, 0),
		orderByClause: make([]string, 0),
		args:         make([]interface{}, 0),
		ctx:          ctx,
	}
}

// Select specifies the columns to select
func (qb *QueryBuilder[T]) Select(fields ...string) *QueryBuilder[T] {
	qb.selectFields = append(qb.selectFields, fields...)
	return qb
}

// From specifies the table to select from
func (qb *QueryBuilder[T]) From(table string) *QueryBuilder[T] {
	qb.fromTable = table
	return qb
}

// Where adds a WHERE condition
func (qb *QueryBuilder[T]) Where(condition string, args ...interface{}) *QueryBuilder[T] {
	qb.whereClause = append(qb.whereClause, condition)
	qb.args = append(qb.args, args...)
	return qb
}

// OrderBy adds an ORDER BY clause
func (qb *QueryBuilder[T]) OrderBy(column string, direction ...string) *QueryBuilder[T] {
	orderBy := column
	if len(direction) > 0 {
		orderBy += " " + strings.ToUpper(direction[0])
	}
	qb.orderByClause = append(qb.orderByClause, orderBy)
	return qb
}

// Limit sets the LIMIT clause
func (qb *QueryBuilder[T]) Limit(limit int) *QueryBuilder[T] {
	qb.limitValue = &limit
	return qb
}

// Offset sets the OFFSET clause
func (qb *QueryBuilder[T]) Offset(offset int) *QueryBuilder[T] {
	qb.offsetValue = &offset
	return qb
}

// Build constructs the SQL query string
func (qb *QueryBuilder[T]) Build() (string, []interface{}) {
	var query strings.Builder

	// SELECT clause
	if len(qb.selectFields) > 0 {
		query.WriteString("SELECT ")
		query.WriteString(strings.Join(qb.selectFields, ", "))
	} else {
		query.WriteString("SELECT *")
	}

	// FROM clause
	if qb.fromTable != "" {
		query.WriteString(" FROM ")
		query.WriteString(qb.fromTable)
	}

	// WHERE clause
	if len(qb.whereClause) > 0 {
		query.WriteString(" WHERE ")
		query.WriteString(strings.Join(qb.whereClause, " AND "))
	}

	// ORDER BY clause
	if len(qb.orderByClause) > 0 {
		query.WriteString(" ORDER BY ")
		query.WriteString(strings.Join(qb.orderByClause, ", "))
	}

	// LIMIT clause
	if qb.limitValue != nil {
		query.WriteString(fmt.Sprintf(" LIMIT %d", *qb.limitValue))
	}

	// OFFSET clause
	if qb.offsetValue != nil {
		query.WriteString(fmt.Sprintf(" OFFSET %d", *qb.offsetValue))
	}

	return query.String(), qb.args
}

// Context returns the query context
func (qb *QueryBuilder[T]) Context() context.Context {
	return qb.ctx
}

// InsertBuilder provides a fluent interface for building INSERT queries
type InsertBuilder[T any] struct {
	intoTable string
	columns   []string
	values    []interface{}
	ctx       context.Context
}

// NewInsertBuilder creates a new InsertBuilder for type T
func NewInsertBuilder[T any](ctx context.Context) *InsertBuilder[T] {
	return &InsertBuilder[T]{
		columns: make([]string, 0),
		values:  make([]interface{}, 0),
		ctx:     ctx,
	}
}

// Into specifies the table to insert into
func (ib *InsertBuilder[T]) Into(table string) *InsertBuilder[T] {
	ib.intoTable = table
	return ib
}

// Values adds values to insert
func (ib *InsertBuilder[T]) Values(columns []string, values []interface{}) *InsertBuilder[T] {
	ib.columns = columns
	ib.values = values
	return ib
}

// Build constructs the INSERT SQL query string
func (ib *InsertBuilder[T]) Build() (string, []interface{}) {
	var query strings.Builder

	query.WriteString("INSERT INTO ")
	query.WriteString(ib.intoTable)

	if len(ib.columns) > 0 {
		query.WriteString(" (")
		query.WriteString(strings.Join(ib.columns, ", "))
		query.WriteString(")")
	}

	if len(ib.values) > 0 {
		query.WriteString(" VALUES (")
		placeholders := make([]string, len(ib.values))
		for i := range placeholders {
			placeholders[i] = "?"
		}
		query.WriteString(strings.Join(placeholders, ", "))
		query.WriteString(")")
	}

	return query.String(), ib.values
}

// UpdateBuilder provides a fluent interface for building UPDATE queries
type UpdateBuilder[T any] struct {
	updateTable string
	setClause   map[string]interface{}
	whereClause []string
	whereArgs   []interface{}
	ctx         context.Context
}

// NewUpdateBuilder creates a new UpdateBuilder for type T
func NewUpdateBuilder[T any](ctx context.Context) *UpdateBuilder[T] {
	return &UpdateBuilder[T]{
		setClause:   make(map[string]interface{}),
		whereClause: make([]string, 0),
		whereArgs:   make([]interface{}, 0),
		ctx:         ctx,
	}
}

// Update specifies the table to update
func (ub *UpdateBuilder[T]) Update(table string) *UpdateBuilder[T] {
	ub.updateTable = table
	return ub
}

// Set adds a SET clause
func (ub *UpdateBuilder[T]) Set(column string, value interface{}) *UpdateBuilder[T] {
	ub.setClause[column] = value
	return ub
}

// Where adds a WHERE condition
func (ub *UpdateBuilder[T]) Where(condition string, args ...interface{}) *UpdateBuilder[T] {
	ub.whereClause = append(ub.whereClause, condition)
	ub.whereArgs = append(ub.whereArgs, args...)
	return ub
}

// Build constructs the UPDATE SQL query string
func (ub *UpdateBuilder[T]) Build() (string, []interface{}) {
	var query strings.Builder
	var args []interface{}

	query.WriteString("UPDATE ")
	query.WriteString(ub.updateTable)
	query.WriteString(" SET ")

	setPairs := make([]string, 0, len(ub.setClause))
	for column, value := range ub.setClause {
		setPairs = append(setPairs, column+" = ?")
		args = append(args, value)
	}
	query.WriteString(strings.Join(setPairs, ", "))

	if len(ub.whereClause) > 0 {
		query.WriteString(" WHERE ")
		query.WriteString(strings.Join(ub.whereClause, " AND "))
		args = append(args, ub.whereArgs...)
	}

	return query.String(), args
}

// DeleteBuilder provides a fluent interface for building DELETE queries
type DeleteBuilder[T any] struct {
	fromTable   string
	whereClause []string
	whereArgs   []interface{}
	ctx         context.Context
}

// NewDeleteBuilder creates a new DeleteBuilder for type T
func NewDeleteBuilder[T any](ctx context.Context) *DeleteBuilder[T] {
	return &DeleteBuilder[T]{
		whereClause: make([]string, 0),
		whereArgs:   make([]interface{}, 0),
		ctx:         ctx,
	}
}

// From specifies the table to delete from
func (db *DeleteBuilder[T]) From(table string) *DeleteBuilder[T] {
	db.fromTable = table
	return db
}

// Where adds a WHERE condition
func (db *DeleteBuilder[T]) Where(condition string, args ...interface{}) *DeleteBuilder[T] {
	db.whereClause = append(db.whereClause, condition)
	db.whereArgs = append(db.whereArgs, args...)
	return db
}

// Build constructs the DELETE SQL query string
func (db *DeleteBuilder[T]) Build() (string, []interface{}) {
	var query strings.Builder

	query.WriteString("DELETE FROM ")
	query.WriteString(db.fromTable)

	if len(db.whereClause) > 0 {
		query.WriteString(" WHERE ")
		query.WriteString(strings.Join(db.whereClause, " AND "))
	}

	return query.String(), db.whereArgs
}