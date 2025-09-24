package db

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/microsoft/go-mssqldb"
)

// SQLServerConnection implements Connection interface using sqlx
type SQLServerConnection struct {
	db     *sqlx.DB
	config ConnectionConfig
	mu     sync.RWMutex
	closed int64
}

// NewSQLServerConnection creates a new SQL Server connection
func NewSQLServerConnection(db *sqlx.DB, config ConnectionConfig) *SQLServerConnection {
	return &SQLServerConnection{
		db:     db,
		config: config,
	}
}

// Type returns the connection type
func (c *SQLServerConnection) Type() ConnectionType {
	return SQLServerConnectionType
}

// Role returns the connection role
func (c *SQLServerConnection) Role() ConnectionRole {
	return c.config.Role
}

// IsHealthy returns whether the connection is healthy
func (c *SQLServerConnection) IsHealthy() bool {
	if atomic.LoadInt64(&c.closed) == 1 {
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return c.Ping(ctx) == nil
}

// Ping tests the database connection
func (c *SQLServerConnection) Ping(ctx context.Context) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return fmt.Errorf("connection is closed")
	}

	return c.db.PingContext(ctx)
}

// Close closes the database connection
func (c *SQLServerConnection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if atomic.CompareAndSwapInt64(&c.closed, 0, 1) {
		return c.db.Close()
	}
	return nil
}

// Stats returns connection statistics
func (c *SQLServerConnection) Stats() ConnectionStats {
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
func (c *SQLServerConnection) Query(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return nil, fmt.Errorf("connection is closed")
	}

	rows, err := c.db.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	return &SQLServerRows{rows: rows}, nil
}

// QueryRow executes a query that returns a single row
func (c *SQLServerConnection) QueryRow(ctx context.Context, query string, args ...interface{}) Row {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return &SQLServerRow{err: fmt.Errorf("connection is closed")}
	}

	row := c.db.QueryRowxContext(ctx, query, args...)
	return &SQLServerRow{row: row}
}

// Exec executes a query without returning rows
func (c *SQLServerConnection) Exec(ctx context.Context, query string, args ...interface{}) (Result, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return nil, fmt.Errorf("connection is closed")
	}

	result, err := c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	return &SQLServerResult{result: result}, nil
}

// Select executes a query and scans the result into dest
func (c *SQLServerConnection) Select(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return fmt.Errorf("connection is closed")
	}

	return c.db.SelectContext(ctx, dest, query, args...)
}

// Get executes a query and scans a single row into dest
func (c *SQLServerConnection) Get(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return fmt.Errorf("connection is closed")
	}

	return c.db.GetContext(ctx, dest, query, args...)
}

// NamedExec executes a named query without returning rows
func (c *SQLServerConnection) NamedExec(ctx context.Context, query string, arg interface{}) (Result, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return nil, fmt.Errorf("connection is closed")
	}

	result, err := c.db.NamedExecContext(ctx, query, arg)
	if err != nil {
		return nil, err
	}

	return &SQLServerResult{result: result}, nil
}

// NamedQuery executes a named query that returns rows
func (c *SQLServerConnection) NamedQuery(ctx context.Context, query string, arg interface{}) (Rows, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return nil, fmt.Errorf("connection is closed")
	}

	rows, err := c.db.NamedQueryContext(ctx, query, arg)
	if err != nil {
		return nil, err
	}

	return &SQLServerRows{rows: rows}, nil
}

// Begin starts a new transaction
func (c *SQLServerConnection) Begin(ctx context.Context) (Transaction, error) {
	return c.BeginTx(ctx, nil)
}

// BeginTx starts a new transaction with options
func (c *SQLServerConnection) BeginTx(ctx context.Context, opts *sql.TxOptions) (Transaction, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return nil, fmt.Errorf("connection is closed")
	}

	tx, err := c.db.BeginTxx(ctx, opts)
	if err != nil {
		return nil, err
	}

	return &SQLServerTransaction{tx: tx}, nil
}

// SQLServerRows implements the Rows interface for SQL Server
type SQLServerRows struct {
	rows *sqlx.Rows
}

func (r *SQLServerRows) Next() bool {
	return r.rows.Next()
}

func (r *SQLServerRows) Scan(dest ...interface{}) error {
	return r.rows.Scan(dest...)
}

func (r *SQLServerRows) Close() error {
	return r.rows.Close()
}

func (r *SQLServerRows) Err() error {
	return r.rows.Err()
}

func (r *SQLServerRows) Columns() ([]string, error) {
	return r.rows.Columns()
}

func (r *SQLServerRows) ColumnTypes() ([]ColumnType, error) {
	types, err := r.rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	result := make([]ColumnType, len(types))
	for i, t := range types {
		result[i] = &SQLServerColumnType{columnType: t}
	}

	return result, nil
}

func (r *SQLServerRows) StructScan(dest interface{}) error {
	return r.rows.StructScan(dest)
}

func (r *SQLServerRows) MapScan(dest map[string]interface{}) error {
	return r.rows.MapScan(dest)
}

func (r *SQLServerRows) SliceScan() ([]interface{}, error) {
	return r.rows.SliceScan()
}

// SQLServerRow implements the Row interface for SQL Server
type SQLServerRow struct {
	row *sqlx.Row
	err error
}

func (r *SQLServerRow) Scan(dest ...interface{}) error {
	if r.err != nil {
		return r.err
	}
	return r.row.Scan(dest...)
}

func (r *SQLServerRow) StructScan(dest interface{}) error {
	if r.err != nil {
		return r.err
	}
	return r.row.StructScan(dest)
}

func (r *SQLServerRow) MapScan(dest map[string]interface{}) error {
	if r.err != nil {
		return r.err
	}
	return r.row.MapScan(dest)
}

// SQLServerResult implements the Result interface for SQL Server
type SQLServerResult struct {
	result sql.Result
}

func (r *SQLServerResult) LastInsertId() (int64, error) {
	return r.result.LastInsertId()
}

func (r *SQLServerResult) RowsAffected() (int64, error) {
	return r.result.RowsAffected()
}

// SQLServerColumnType implements the ColumnType interface for SQL Server
type SQLServerColumnType struct {
	columnType *sql.ColumnType
}

func (c *SQLServerColumnType) Name() string {
	return c.columnType.Name()
}

func (c *SQLServerColumnType) DatabaseTypeName() string {
	return c.columnType.DatabaseTypeName()
}

func (c *SQLServerColumnType) ScanType() interface{} {
	return c.columnType.ScanType()
}

func (c *SQLServerColumnType) Nullable() (nullable, ok bool) {
	return c.columnType.Nullable()
}

func (c *SQLServerColumnType) Length() (length int64, ok bool) {
	return c.columnType.Length()
}

func (c *SQLServerColumnType) DecimalSize() (precision, scale int64, ok bool) {
	return c.columnType.DecimalSize()
}

// SQLServerTransaction implements the Transaction interface for SQL Server
type SQLServerTransaction struct {
	tx *sqlx.Tx
}

func (t *SQLServerTransaction) Query(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	rows, err := t.tx.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &SQLServerRows{rows: rows}, nil
}

func (t *SQLServerTransaction) QueryRow(ctx context.Context, query string, args ...interface{}) Row {
	row := t.tx.QueryRowxContext(ctx, query, args...)
	return &SQLServerRow{row: row}
}

func (t *SQLServerTransaction) Exec(ctx context.Context, query string, args ...interface{}) (Result, error) {
	result, err := t.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &SQLServerResult{result: result}, nil
}

func (t *SQLServerTransaction) Select(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return t.tx.SelectContext(ctx, dest, query, args...)
}

func (t *SQLServerTransaction) Get(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return t.tx.GetContext(ctx, dest, query, args...)
}

func (t *SQLServerTransaction) NamedExec(ctx context.Context, query string, arg interface{}) (Result, error) {
	result, err := t.tx.NamedExecContext(ctx, query, arg)
	if err != nil {
		return nil, err
	}
	return &SQLServerResult{result: result}, nil
}

func (t *SQLServerTransaction) NamedQuery(ctx context.Context, query string, arg interface{}) (Rows, error) {
	rows, err := t.tx.NamedQuery(query, arg)
	if err != nil {
		return nil, err
	}
	return &SQLServerRows{rows: rows}, nil
}

func (t *SQLServerTransaction) Commit() error {
	return t.tx.Commit()
}

func (t *SQLServerTransaction) Rollback() error {
	return t.tx.Rollback()
}