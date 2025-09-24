package db

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

// SQLiteConnection implements Connection interface using sqlx
type SQLiteConnection struct {
	db     *sqlx.DB
	config ConnectionConfig
	mu     sync.RWMutex
	closed int64
}

// NewSQLiteConnection creates a new SQLite connection
func NewSQLiteConnection(db *sqlx.DB, config ConnectionConfig) *SQLiteConnection {
	return &SQLiteConnection{
		db:     db,
		config: config,
	}
}

// Type returns the connection type
func (c *SQLiteConnection) Type() ConnectionType {
	return SQLiteConnectionType
}

// Role returns the connection role
func (c *SQLiteConnection) Role() ConnectionRole {
	return c.config.Role
}

// IsHealthy returns whether the connection is healthy
func (c *SQLiteConnection) IsHealthy() bool {
	if atomic.LoadInt64(&c.closed) == 1 {
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return c.Ping(ctx) == nil
}

// Ping tests the database connection
func (c *SQLiteConnection) Ping(ctx context.Context) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return fmt.Errorf("connection is closed")
	}

	return c.db.PingContext(ctx)
}

// Close closes the database connection
func (c *SQLiteConnection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if atomic.CompareAndSwapInt64(&c.closed, 0, 1) {
		return c.db.Close()
	}
	return nil
}

// Stats returns connection statistics
func (c *SQLiteConnection) Stats() ConnectionStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return ConnectionStats{}
	}

	stats := c.db.Stats()
	return ConnectionStats{
		MaxOpenConnections: stats.MaxOpenConnections,
		OpenConnections:    stats.OpenConnections,
		InUse:              stats.InUse,
		Idle:               stats.Idle,
		WaitCount:          stats.WaitCount,
		WaitDuration:       stats.WaitDuration,
		MaxIdleClosed:      stats.MaxIdleClosed,
		MaxIdleTimeClosed:  stats.MaxIdleTimeClosed,
		MaxLifetimeClosed:  stats.MaxLifetimeClosed,
	}
}

// Query executes a query that returns rows
func (c *SQLiteConnection) Query(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return nil, fmt.Errorf("connection is closed")
	}

	rows, err := c.db.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	return &SQLiteRows{rows: rows}, nil
}

// QueryRow executes a query that returns a single row
func (c *SQLiteConnection) QueryRow(ctx context.Context, query string, args ...interface{}) Row {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return &SQLiteRow{err: fmt.Errorf("connection is closed")}
	}

	row := c.db.QueryRowxContext(ctx, query, args...)
	return &SQLiteRow{row: row}
}

// Exec executes a query without returning rows
func (c *SQLiteConnection) Exec(ctx context.Context, query string, args ...interface{}) (Result, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return nil, fmt.Errorf("connection is closed")
	}

	result, err := c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	return &SQLiteResult{result: result}, nil
}

// Select executes a query and scans the result into dest
func (c *SQLiteConnection) Select(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return fmt.Errorf("connection is closed")
	}

	return c.db.SelectContext(ctx, dest, query, args...)
}

// Get executes a query and scans a single row into dest
func (c *SQLiteConnection) Get(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return fmt.Errorf("connection is closed")
	}

	return c.db.GetContext(ctx, dest, query, args...)
}

// NamedExec executes a named query without returning rows
func (c *SQLiteConnection) NamedExec(ctx context.Context, query string, arg interface{}) (Result, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return nil, fmt.Errorf("connection is closed")
	}

	result, err := c.db.NamedExecContext(ctx, query, arg)
	if err != nil {
		return nil, err
	}

	return &SQLiteResult{result: result}, nil
}

// NamedQuery executes a named query that returns rows
func (c *SQLiteConnection) NamedQuery(ctx context.Context, query string, arg interface{}) (Rows, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return nil, fmt.Errorf("connection is closed")
	}

	rows, err := c.db.NamedQueryContext(ctx, query, arg)
	if err != nil {
		return nil, err
	}

	return &SQLiteRows{rows: rows}, nil
}

// Begin starts a new transaction
func (c *SQLiteConnection) Begin(ctx context.Context) (Transaction, error) {
	return c.BeginTx(ctx, nil)
}

// BeginTx starts a new transaction with options
func (c *SQLiteConnection) BeginTx(ctx context.Context, opts *sql.TxOptions) (Transaction, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return nil, fmt.Errorf("connection is closed")
	}

	tx, err := c.db.BeginTxx(ctx, opts)
	if err != nil {
		return nil, err
	}

	return &SQLiteTransaction{tx: tx}, nil
}

// SQLiteRows implements the Rows interface for SQLite
type SQLiteRows struct {
	rows *sqlx.Rows
}

func (r *SQLiteRows) Next() bool {
	return r.rows.Next()
}

func (r *SQLiteRows) Scan(dest ...interface{}) error {
	return r.rows.Scan(dest...)
}

func (r *SQLiteRows) Close() error {
	return r.rows.Close()
}

func (r *SQLiteRows) Err() error {
	return r.rows.Err()
}

func (r *SQLiteRows) Columns() ([]string, error) {
	return r.rows.Columns()
}

func (r *SQLiteRows) ColumnTypes() ([]ColumnType, error) {
	types, err := r.rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	result := make([]ColumnType, len(types))
	for i, t := range types {
		result[i] = &SQLiteColumnType{columnType: t}
	}

	return result, nil
}

func (r *SQLiteRows) StructScan(dest interface{}) error {
	return r.rows.StructScan(dest)
}

func (r *SQLiteRows) MapScan(dest map[string]interface{}) error {
	return r.rows.MapScan(dest)
}

func (r *SQLiteRows) SliceScan() ([]interface{}, error) {
	return r.rows.SliceScan()
}

// SQLiteRow implements the Row interface for SQLite
type SQLiteRow struct {
	row *sqlx.Row
	err error
}

func (r *SQLiteRow) Scan(dest ...interface{}) error {
	if r.err != nil {
		return r.err
	}
	return r.row.Scan(dest...)
}

func (r *SQLiteRow) StructScan(dest interface{}) error {
	if r.err != nil {
		return r.err
	}
	return r.row.StructScan(dest)
}

func (r *SQLiteRow) MapScan(dest map[string]interface{}) error {
	if r.err != nil {
		return r.err
	}
	return r.row.MapScan(dest)
}

// SQLiteResult implements the Result interface for SQLite
type SQLiteResult struct {
	result sql.Result
}

func (r *SQLiteResult) LastInsertId() (int64, error) {
	return r.result.LastInsertId()
}

func (r *SQLiteResult) RowsAffected() (int64, error) {
	return r.result.RowsAffected()
}

// SQLiteColumnType implements the ColumnType interface for SQLite
type SQLiteColumnType struct {
	columnType *sql.ColumnType
}

func (c *SQLiteColumnType) Name() string {
	return c.columnType.Name()
}

func (c *SQLiteColumnType) DatabaseTypeName() string {
	return c.columnType.DatabaseTypeName()
}

func (c *SQLiteColumnType) ScanType() interface{} {
	return c.columnType.ScanType()
}

func (c *SQLiteColumnType) Nullable() (nullable, ok bool) {
	return c.columnType.Nullable()
}

func (c *SQLiteColumnType) Length() (length int64, ok bool) {
	return c.columnType.Length()
}

func (c *SQLiteColumnType) DecimalSize() (precision, scale int64, ok bool) {
	return c.columnType.DecimalSize()
}

// SQLiteTransaction implements the Transaction interface for SQLite
type SQLiteTransaction struct {
	tx *sqlx.Tx
}

func (t *SQLiteTransaction) Query(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	rows, err := t.tx.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &SQLiteRows{rows: rows}, nil
}

func (t *SQLiteTransaction) QueryRow(ctx context.Context, query string, args ...interface{}) Row {
	row := t.tx.QueryRowxContext(ctx, query, args...)
	return &SQLiteRow{row: row}
}

func (t *SQLiteTransaction) Exec(ctx context.Context, query string, args ...interface{}) (Result, error) {
	result, err := t.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &SQLiteResult{result: result}, nil
}

func (t *SQLiteTransaction) Select(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return t.tx.SelectContext(ctx, dest, query, args...)
}

func (t *SQLiteTransaction) Get(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return t.tx.GetContext(ctx, dest, query, args...)
}

func (t *SQLiteTransaction) NamedExec(ctx context.Context, query string, arg interface{}) (Result, error) {
	result, err := t.tx.NamedExecContext(ctx, query, arg)
	if err != nil {
		return nil, err
	}
	return &SQLiteResult{result: result}, nil
}

func (t *SQLiteTransaction) NamedQuery(ctx context.Context, query string, arg interface{}) (Rows, error) {
	rows, err := t.tx.NamedQuery(query, arg)
	if err != nil {
		return nil, err
	}
	return &SQLiteRows{rows: rows}, nil
}

func (t *SQLiteTransaction) Commit() error {
	return t.tx.Commit()
}

func (t *SQLiteTransaction) Rollback() error {
	return t.tx.Rollback()
}