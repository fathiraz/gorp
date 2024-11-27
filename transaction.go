// Copyright 2012 James Cooper. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package gorp

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/jmoiron/sqlx"
)

// ErrTxClosed is returned when attempting to use a closed transaction
var ErrTxClosed = errors.New("transaction already closed")

// ErrTxNamedQueryNotSupported is returned when attempting to use NamedQuery with a transaction
var ErrTxNamedQueryNotSupported = errors.New("NamedQuery is not supported with transactions")

// TxOptions represents transaction options that can be passed to BeginTxWithOptions
type TxOptions struct {
	sql.TxOptions
	Context context.Context
}

// Transaction represents a database transaction.
// Insert/Update/Delete/Get/Exec operations will be run in the context
// of that transaction.  Transactions should be terminated with
// a Commit() or Rollback()
type Transaction struct {
	dbmap  *DbMap
	ctx    context.Context
	tx     *sqlx.Tx
	closed bool
}

// TypedTransaction provides type-safe transaction operations
type TypedTransaction[T any] struct {
	*Transaction
}

// NewTypedTransaction creates a new typed transaction
func NewTypedTransaction[T any](tx *Transaction) *TypedTransaction[T] {
	return &TypedTransaction[T]{Transaction: tx}
}

// WithContext returns a copy of the transaction with the given context
func (t *Transaction) WithContext(ctx context.Context) SqlExecutor {
	if ctx == nil {
		panic("nil context")
	}
	copy := &Transaction{}
	*copy = *t
	copy.ctx = ctx
	return copy
}

// Insert has the same behavior as DbMap.Insert(), but runs in a transaction.
func (t *Transaction) Insert(list ...interface{}) error {
	if t.closed {
		return ErrTxClosed
	}
	return insert(t.dbmap, t, list...)
}

// TypedInsert provides type-safe insert operations
func (t *TypedTransaction[T]) TypedInsert(list ...T) error {
	if t.closed {
		return ErrTxClosed
	}
	iList := make([]interface{}, len(list))
	for i, v := range list {
		iList[i] = v
	}
	return t.Insert(iList...)
}

// Update has the same behavior as DbMap.Update(), but runs in a transaction.
func (t *Transaction) Update(list ...interface{}) (int64, error) {
	if t.closed {
		return 0, ErrTxClosed
	}
	return update(t.dbmap, t, nil, list...)
}

// TypedUpdate provides type-safe update operations
func (t *TypedTransaction[T]) TypedUpdate(list ...T) (int64, error) {
	if t.closed {
		return 0, ErrTxClosed
	}
	iList := make([]interface{}, len(list))
	for i, v := range list {
		iList[i] = v
	}
	return t.Update(iList...)
}

// UpdateColumns has the same behavior as DbMap.UpdateColumns(), but runs in a transaction.
func (t *Transaction) UpdateColumns(filter ColumnFilter, list ...interface{}) (int64, error) {
	if t.closed {
		return 0, ErrTxClosed
	}
	return update(t.dbmap, t, filter, list...)
}

// Delete has the same behavior as DbMap.Delete(), but runs in a transaction.
func (t *Transaction) Delete(list ...interface{}) (int64, error) {
	if t.closed {
		return 0, ErrTxClosed
	}
	return delete(t.dbmap, t, list...)
}

// TypedDelete provides type-safe delete operations
func (t *TypedTransaction[T]) TypedDelete(list ...T) (int64, error) {
	if t.closed {
		return 0, ErrTxClosed
	}
	iList := make([]interface{}, len(list))
	for i, v := range list {
		iList[i] = v
	}
	return t.Delete(iList...)
}

// Get has the same behavior as DbMap.Get(), but runs in a transaction.
func (t *Transaction) Get(i interface{}, keys ...interface{}) (interface{}, error) {
	if t.closed {
		return nil, ErrTxClosed
	}
	return get(t.dbmap, t, i, keys...)
}

// TypedGet provides type-safe get operations
func (t *TypedTransaction[T]) TypedGet(keys ...interface{}) (*T, error) {
	if t.closed {
		return nil, ErrTxClosed
	}
	var result T
	i, err := t.Get(&result, keys...)
	if err != nil {
		return nil, err
	}
	if i == nil {
		return nil, nil
	}
	return i.(*T), nil
}

// Select has the same behavior as DbMap.Select(), but runs in a transaction.
func (t *Transaction) Select(i interface{}, query string, args ...interface{}) ([]interface{}, error) {
	if t.closed {
		return nil, ErrTxClosed
	}
	if t.dbmap.ExpandSliceArgs {
		expandSliceArgs(&query, args...)
	}
	return hookedselect(t.dbmap, t, i, query, args...)
}

// TypedSelect provides type-safe select operations
func (t *TypedTransaction[T]) TypedSelect(query string, args ...interface{}) ([]T, error) {
	if t.closed {
		return nil, ErrTxClosed
	}
	var result T
	list, err := t.Select(&result, query, args...)
	if err != nil {
		return nil, err
	}
	typedList := make([]T, len(list))
	for i, v := range list {
		typedList[i] = *v.(*T)
	}
	return typedList, nil
}

// Exec has the same behavior as DbMap.Exec(), but runs in a transaction.
func (t *Transaction) Exec(query string, args ...interface{}) (sql.Result, error) {
	if t.closed {
		return nil, ErrTxClosed
	}
	if t.dbmap.ExpandSliceArgs {
		expandSliceArgs(&query, args...)
	}
	if t.dbmap.logger != nil {
		now := time.Now()
		defer t.dbmap.trace(now, query, args...)
	}
	return maybeExpandNamedQueryAndExec(t, query, args...)
}

// Getx using this transaction with sqlx
func (t *Transaction) Getx(dest interface{}, query string, args ...interface{}) error {
	if t.closed {
		return ErrTxClosed
	}
	if t.ctx != nil {
		return t.tx.GetContext(t.ctx, dest, query, args...)
	}
	return t.tx.Get(dest, query, args...)
}

// Selectx using this transaction with sqlx
func (t *Transaction) Selectx(dest interface{}, query string, args ...interface{}) error {
	if t.closed {
		return ErrTxClosed
	}
	if t.ctx != nil {
		return t.tx.SelectContext(t.ctx, dest, query, args...)
	}
	return t.tx.Select(dest, query, args...)
}

// NamedExec using this transaction
func (t *Transaction) NamedExec(query string, arg interface{}) (sql.Result, error) {
	if t.closed {
		return nil, ErrTxClosed
	}
	if t.ctx != nil {
		return t.tx.NamedExecContext(t.ctx, query, arg)
	}
	return t.tx.NamedExec(query, arg)
}

// NamedQuery using this transaction
func (t *Transaction) NamedQuery(query string, arg interface{}) (*sqlx.Rows, error) {
	if t.closed {
		return nil, ErrTxClosed
	}
	if t.dbmap.logger != nil {
		now := time.Now()
		defer t.dbmap.trace(now, query, arg)
	}
	if t.ctx != nil {
		return nil, ErrTxNamedQueryNotSupported
	}
	return t.tx.NamedQuery(query, arg)
}

// PreparexNamed creates a prepared statement for later named execution
func (t *Transaction) PreparexNamed(query string) (*sqlx.NamedStmt, error) {
	if t.closed {
		return nil, ErrTxClosed
	}
	if t.dbmap.logger != nil {
		now := time.Now()
		defer t.dbmap.trace(now, query)
	}
	if t.ctx != nil {
		return t.tx.PrepareNamedContext(t.ctx, query)
	}
	return t.tx.PrepareNamed(query)
}

// MustExec executes the query without returning an error
func (t *Transaction) MustExec(query string, args ...interface{}) sql.Result {
	if t.closed {
		panic(ErrTxClosed)
	}
	if t.dbmap.ExpandSliceArgs {
		expandSliceArgs(&query, args...)
	}
	if t.dbmap.logger != nil {
		now := time.Now()
		defer t.dbmap.trace(now, query, args...)
	}
	if t.ctx != nil {
		return t.tx.MustExecContext(t.ctx, query, args...)
	}
	return t.tx.MustExec(query, args...)
}

// Commit commits the underlying database transaction.
func (t *Transaction) Commit() error {
	if t.closed {
		return ErrTxClosed
	}
	t.closed = true
	if t.dbmap.logger != nil {
		now := time.Now()
		defer t.dbmap.trace(now, "commit;")
	}
	return t.tx.Commit()
}

// Rollback rolls back the underlying database transaction.
func (t *Transaction) Rollback() error {
	if t.closed {
		return ErrTxClosed
	}
	t.closed = true
	if t.dbmap.logger != nil {
		now := time.Now()
		defer t.dbmap.trace(now, "rollback;")
	}
	return t.tx.Rollback()
}

// Savepoint creates a savepoint with the given name. The name is interpolated
// directly into the SQL SAVEPOINT statement, so you must sanitize it if it is
// derived from user input.
func (t *Transaction) Savepoint(name string) error {
	if t.closed {
		return ErrTxClosed
	}
	query := "savepoint " + t.dbmap.Dialect.QuoteField(name)
	if t.dbmap.logger != nil {
		now := time.Now()
		defer t.dbmap.trace(now, query, nil)
	}
	_, err := exec(t, query)
	return err
}

// RollbackToSavepoint rolls back to the savepoint with the given name. The
// name is interpolated directly into the SQL SAVEPOINT statement, so you must
// sanitize it if it is derived from user input.
func (t *Transaction) RollbackToSavepoint(savepoint string) error {
	if t.closed {
		return ErrTxClosed
	}
	query := "rollback to savepoint " + t.dbmap.Dialect.QuoteField(savepoint)
	if t.dbmap.logger != nil {
		now := time.Now()
		defer t.dbmap.trace(now, query, nil)
	}
	_, err := exec(t, query)
	return err
}

// ReleaseSavepoint releases the savepoint with the given name. The name is
// interpolated directly into the SQL SAVEPOINT statement, so you must sanitize
// it if it is derived from user input.
func (t *Transaction) ReleaseSavepoint(savepoint string) error {
	if t.closed {
		return ErrTxClosed
	}
	query := "release savepoint " + t.dbmap.Dialect.QuoteField(savepoint)
	if t.dbmap.logger != nil {
		now := time.Now()
		defer t.dbmap.trace(now, query, nil)
	}
	_, err := exec(t, query)
	return err
}

// Prepare has the same behavior as DbMap.Prepare(), but runs in a transaction.
func (t *Transaction) Prepare(query string) (*sqlx.Stmt, error) {
	if t.closed {
		return nil, ErrTxClosed
	}
	return t.tx.PreparexContext(t.ctx, query)
}

// Query using this transaction with standard sql.Rows result
func (t *Transaction) Query(query string, args ...interface{}) (*sql.Rows, error) {
	if t.closed {
		return nil, ErrTxClosed
	}
	if t.dbmap.ExpandSliceArgs {
		expandSliceArgs(&query, args...)
	}
	if t.ctx != nil {
		return t.tx.QueryContext(t.ctx, query, args...)
	}
	return t.tx.Query(query, args...)
}

// QueryRow using this transaction with standard sql.Row result
func (t *Transaction) QueryRow(query string, args ...interface{}) *sql.Row {
	if t.closed {
		return nil
	}
	if t.dbmap.ExpandSliceArgs {
		expandSliceArgs(&query, args...)
	}
	if t.ctx != nil {
		return t.tx.QueryRowContext(t.ctx, query, args...)
	}
	return t.tx.QueryRow(query, args...)
}

// Queryx using this transaction with sqlx.Rows result
func (t *Transaction) Queryx(query string, args ...interface{}) (*sqlx.Rows, error) {
	if t.closed {
		return nil, ErrTxClosed
	}
	if t.dbmap.ExpandSliceArgs {
		expandSliceArgs(&query, args...)
	}
	if t.ctx != nil {
		return t.tx.QueryxContext(t.ctx, query, args...)
	}
	return t.tx.Queryx(query, args...)
}

// QueryRowx using this transaction with sqlx.Row result
func (t *Transaction) QueryRowx(query string, args ...interface{}) *sqlx.Row {
	if t.closed {
		return nil
	}
	if t.dbmap.ExpandSliceArgs {
		expandSliceArgs(&query, args...)
	}
	if t.ctx != nil {
		return t.tx.QueryRowxContext(t.ctx, query, args...)
	}
	return t.tx.QueryRowx(query, args...)
}

// SelectInt is a convenience wrapper around the gorp.SelectInt function.
func (t *Transaction) SelectInt(query string, args ...interface{}) (int64, error) {
	if t.closed {
		return 0, ErrTxClosed
	}
	if t.dbmap.ExpandSliceArgs {
		expandSliceArgs(&query, args...)
	}
	return SelectInt(t, query, args...)
}

// SelectNullInt is a convenience wrapper around the gorp.SelectNullInt function.
func (t *Transaction) SelectNullInt(query string, args ...interface{}) (sql.NullInt64, error) {
	if t.closed {
		return sql.NullInt64{}, ErrTxClosed
	}
	if t.dbmap.ExpandSliceArgs {
		expandSliceArgs(&query, args...)
	}
	return SelectNullInt(t, query, args...)
}

// SelectFloat is a convenience wrapper around the gorp.SelectFloat function.
func (t *Transaction) SelectFloat(query string, args ...interface{}) (float64, error) {
	if t.closed {
		return 0, ErrTxClosed
	}
	if t.dbmap.ExpandSliceArgs {
		expandSliceArgs(&query, args...)
	}
	return SelectFloat(t, query, args...)
}

// SelectNullFloat is a convenience wrapper around the gorp.SelectNullFloat function.
func (t *Transaction) SelectNullFloat(query string, args ...interface{}) (sql.NullFloat64, error) {
	if t.closed {
		return sql.NullFloat64{}, ErrTxClosed
	}
	if t.dbmap.ExpandSliceArgs {
		expandSliceArgs(&query, args...)
	}
	return SelectNullFloat(t, query, args...)
}

// SelectStr is a convenience wrapper around the gorp.SelectStr function.
func (t *Transaction) SelectStr(query string, args ...interface{}) (string, error) {
	if t.closed {
		return "", ErrTxClosed
	}
	if t.dbmap.ExpandSliceArgs {
		expandSliceArgs(&query, args...)
	}
	return SelectStr(t, query, args...)
}

// SelectNullStr is a convenience wrapper around the gorp.SelectNullStr function.
func (t *Transaction) SelectNullStr(query string, args ...interface{}) (sql.NullString, error) {
	if t.closed {
		return sql.NullString{}, ErrTxClosed
	}
	if t.dbmap.ExpandSliceArgs {
		expandSliceArgs(&query, args...)
	}
	return SelectNullStr(t, query, args...)
}

// SelectBool is a convenience wrapper around the gorp.SelectBool function.
func (t *Transaction) SelectBool(query string, args ...interface{}) (bool, error) {
	if t.closed {
		return false, ErrTxClosed
	}
	if t.dbmap.ExpandSliceArgs {
		expandSliceArgs(&query, args...)
	}
	return SelectBool(t, query, args...)
}

// SelectNullBool is a convenience wrapper around the gorp.SelectNullBool function.
func (t *Transaction) SelectNullBool(query string, args ...interface{}) (sql.NullBool, error) {
	if t.closed {
		return sql.NullBool{}, ErrTxClosed
	}
	if t.dbmap.ExpandSliceArgs {
		expandSliceArgs(&query, args...)
	}
	return SelectNullBool(t, query, args...)
}

// SelectOne is a convenience wrapper around the gorp.SelectOne function.
func (t *Transaction) SelectOne(holder interface{}, query string, args ...interface{}) error {
	if t.closed {
		return ErrTxClosed
	}
	if t.dbmap.ExpandSliceArgs {
		expandSliceArgs(&query, args...)
	}
	return SelectOne(t.dbmap, t, holder, query, args...)
}
