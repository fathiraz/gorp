package db

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/go-sql-driver/mysql"
)

// MySQLConnection implements Connection interface using sqlx
type MySQLConnection struct {
	db     *sqlx.DB
	config ConnectionConfig
	mu     sync.RWMutex
	closed int64
}

// NewMySQLConnection creates a new MySQL connection
func NewMySQLConnection(db *sqlx.DB, config ConnectionConfig) *MySQLConnection {
	return &MySQLConnection{
		db:     db,
		config: config,
	}
}

// Type returns the connection type
func (c *MySQLConnection) Type() ConnectionType {
	return MySQLConnectionType
}

// Role returns the connection role
func (c *MySQLConnection) Role() ConnectionRole {
	return c.config.Role
}

// IsHealthy returns whether the connection is healthy
func (c *MySQLConnection) IsHealthy() bool {
	if atomic.LoadInt64(&c.closed) == 1 {
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return c.Ping(ctx) == nil
}

// Ping tests the database connection
func (c *MySQLConnection) Ping(ctx context.Context) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return fmt.Errorf("connection is closed")
	}

	return c.db.PingContext(ctx)
}

// Close closes the database connection
func (c *MySQLConnection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if atomic.CompareAndSwapInt64(&c.closed, 0, 1) {
		return c.db.Close()
	}
	return nil
}

// Stats returns connection statistics
func (c *MySQLConnection) Stats() ConnectionStats {
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
func (c *MySQLConnection) Query(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return nil, fmt.Errorf("connection is closed")
	}

	rows, err := c.db.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	return &MySQLRows{rows: rows}, nil
}

// QueryRow executes a query that returns a single row
func (c *MySQLConnection) QueryRow(ctx context.Context, query string, args ...interface{}) Row {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return &MySQLRow{err: fmt.Errorf("connection is closed")}
	}

	row := c.db.QueryRowxContext(ctx, query, args...)
	return &MySQLRow{row: row}
}

// Exec executes a query without returning rows
func (c *MySQLConnection) Exec(ctx context.Context, query string, args ...interface{}) (Result, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return nil, fmt.Errorf("connection is closed")
	}

	result, err := c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	return &MySQLResult{result: result}, nil
}

// Select executes a query and scans the result into dest
func (c *MySQLConnection) Select(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return fmt.Errorf("connection is closed")
	}

	return c.db.SelectContext(ctx, dest, query, args...)
}

// Get executes a query and scans a single row into dest
func (c *MySQLConnection) Get(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return fmt.Errorf("connection is closed")
	}

	return c.db.GetContext(ctx, dest, query, args...)
}

// NamedExec executes a named query without returning rows
func (c *MySQLConnection) NamedExec(ctx context.Context, query string, arg interface{}) (Result, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return nil, fmt.Errorf("connection is closed")
	}

	result, err := c.db.NamedExecContext(ctx, query, arg)
	if err != nil {
		return nil, err
	}

	return &MySQLResult{result: result}, nil
}

// NamedQuery executes a named query that returns rows
func (c *MySQLConnection) NamedQuery(ctx context.Context, query string, arg interface{}) (Rows, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return nil, fmt.Errorf("connection is closed")
	}

	rows, err := c.db.NamedQueryContext(ctx, query, arg)
	if err != nil {
		return nil, err
	}

	return &MySQLRows{rows: rows}, nil
}

// Begin starts a new transaction
func (c *MySQLConnection) Begin(ctx context.Context) (Transaction, error) {
	return c.BeginTx(ctx, nil)
}

// BeginTx starts a new transaction with options
func (c *MySQLConnection) BeginTx(ctx context.Context, opts *sql.TxOptions) (Transaction, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return nil, fmt.Errorf("connection is closed")
	}

	tx, err := c.db.BeginTxx(ctx, opts)
	if err != nil {
		return nil, err
	}

	return &MySQLTransaction{tx: tx}, nil
}

// MySQLRows implements the Rows interface for MySQL
type MySQLRows struct {
	rows *sqlx.Rows
}

func (r *MySQLRows) Next() bool {
	return r.rows.Next()
}

func (r *MySQLRows) Scan(dest ...interface{}) error {
	return r.rows.Scan(dest...)
}

func (r *MySQLRows) Close() error {
	return r.rows.Close()
}

func (r *MySQLRows) Err() error {
	return r.rows.Err()
}

func (r *MySQLRows) Columns() ([]string, error) {
	return r.rows.Columns()
}

func (r *MySQLRows) ColumnTypes() ([]ColumnType, error) {
	types, err := r.rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	result := make([]ColumnType, len(types))
	for i, t := range types {
		result[i] = &MySQLColumnType{columnType: t}
	}

	return result, nil
}

func (r *MySQLRows) StructScan(dest interface{}) error {
	return r.rows.StructScan(dest)
}

func (r *MySQLRows) MapScan(dest map[string]interface{}) error {
	return r.rows.MapScan(dest)
}

func (r *MySQLRows) SliceScan() ([]interface{}, error) {
	return r.rows.SliceScan()
}

// MySQLRow implements the Row interface for MySQL
type MySQLRow struct {
	row *sqlx.Row
	err error
}

func (r *MySQLRow) Scan(dest ...interface{}) error {
	if r.err != nil {
		return r.err
	}
	return r.row.Scan(dest...)
}

func (r *MySQLRow) StructScan(dest interface{}) error {
	if r.err != nil {
		return r.err
	}
	return r.row.StructScan(dest)
}

func (r *MySQLRow) MapScan(dest map[string]interface{}) error {
	if r.err != nil {
		return r.err
	}
	return r.row.MapScan(dest)
}

// MySQLResult implements the Result interface for MySQL
type MySQLResult struct {
	result sql.Result
}

func (r *MySQLResult) LastInsertId() (int64, error) {
	return r.result.LastInsertId()
}

func (r *MySQLResult) RowsAffected() (int64, error) {
	return r.result.RowsAffected()
}

// MySQLColumnType implements the ColumnType interface for MySQL
type MySQLColumnType struct {
	columnType *sql.ColumnType
}

func (c *MySQLColumnType) Name() string {
	return c.columnType.Name()
}

func (c *MySQLColumnType) DatabaseTypeName() string {
	return c.columnType.DatabaseTypeName()
}

func (c *MySQLColumnType) ScanType() interface{} {
	return c.columnType.ScanType()
}

func (c *MySQLColumnType) Nullable() (nullable, ok bool) {
	return c.columnType.Nullable()
}

func (c *MySQLColumnType) Length() (length int64, ok bool) {
	return c.columnType.Length()
}

func (c *MySQLColumnType) DecimalSize() (precision, scale int64, ok bool) {
	return c.columnType.DecimalSize()
}

// MySQLTransaction implements the Transaction interface for MySQL
type MySQLTransaction struct {
	tx *sqlx.Tx
}

func (t *MySQLTransaction) Query(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	rows, err := t.tx.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &MySQLRows{rows: rows}, nil
}

func (t *MySQLTransaction) QueryRow(ctx context.Context, query string, args ...interface{}) Row {
	row := t.tx.QueryRowxContext(ctx, query, args...)
	return &MySQLRow{row: row}
}

func (t *MySQLTransaction) Exec(ctx context.Context, query string, args ...interface{}) (Result, error) {
	result, err := t.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &MySQLResult{result: result}, nil
}

func (t *MySQLTransaction) Select(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return t.tx.SelectContext(ctx, dest, query, args...)
}

func (t *MySQLTransaction) Get(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return t.tx.GetContext(ctx, dest, query, args...)
}

func (t *MySQLTransaction) NamedExec(ctx context.Context, query string, arg interface{}) (Result, error) {
	result, err := t.tx.NamedExecContext(ctx, query, arg)
	if err != nil {
		return nil, err
	}
	return &MySQLResult{result: result}, nil
}

func (t *MySQLTransaction) NamedQuery(ctx context.Context, query string, arg interface{}) (Rows, error) {
	rows, err := t.tx.NamedQuery(query, arg)
	if err != nil {
		return nil, err
	}
	return &MySQLRows{rows: rows}, nil
}

func (t *MySQLTransaction) Commit() error {
	return t.tx.Commit()
}

func (t *MySQLTransaction) Rollback() error {
	return t.tx.Rollback()
}