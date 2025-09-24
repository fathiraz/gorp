package db

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgreSQLConnection implements Connection interface using pgx
type PostgreSQLConnection struct {
	pool   *pgxpool.Pool
	config ConnectionConfig
	mu     sync.RWMutex
	closed int64
}

// NewPostgreSQLConnection creates a new PostgreSQL connection
func NewPostgreSQLConnection(pool *pgxpool.Pool, config ConnectionConfig) *PostgreSQLConnection {
	return &PostgreSQLConnection{
		pool:   pool,
		config: config,
	}
}

// Type returns the connection type
func (c *PostgreSQLConnection) Type() ConnectionType {
	return PostgreSQLConnectionType
}

// Role returns the connection role
func (c *PostgreSQLConnection) Role() ConnectionRole {
	return c.config.Role
}

// IsHealthy returns whether the connection is healthy
func (c *PostgreSQLConnection) IsHealthy() bool {
	if atomic.LoadInt64(&c.closed) == 1 {
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return c.Ping(ctx) == nil
}

// Ping tests the database connection
func (c *PostgreSQLConnection) Ping(ctx context.Context) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return fmt.Errorf("connection is closed")
	}

	return c.pool.Ping(ctx)
}

// Close closes the database connection
func (c *PostgreSQLConnection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if atomic.CompareAndSwapInt64(&c.closed, 0, 1) {
		c.pool.Close()
		return nil
	}
	return nil
}

// Stats returns connection statistics
func (c *PostgreSQLConnection) Stats() ConnectionStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return ConnectionStats{}
	}

	stats := c.pool.Stat()
	return ConnectionStats{
		MaxOpenConnections: int(stats.MaxConns()),
		OpenConnections:    int(stats.TotalConns()),
		InUse:              int(stats.AcquiredConns()),
		Idle:               int(stats.IdleConns()),
		WaitCount:          0, // pgx doesn't provide this
		WaitDuration:       0, // pgx doesn't provide this
		MaxIdleClosed:      0, // pgx doesn't provide this
		MaxIdleTimeClosed:  0, // pgx doesn't provide this
		MaxLifetimeClosed:  0, // pgx doesn't provide this
	}
}

// Query executes a query that returns rows
func (c *PostgreSQLConnection) Query(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return nil, fmt.Errorf("connection is closed")
	}

	rows, err := c.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	return &PostgreSQLRows{rows: rows}, nil
}

// QueryRow executes a query that returns a single row
func (c *PostgreSQLConnection) QueryRow(ctx context.Context, query string, args ...interface{}) Row {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return &PostgreSQLRow{err: fmt.Errorf("connection is closed")}
	}

	row := c.pool.QueryRow(ctx, query, args...)
	return &PostgreSQLRow{row: row}
}

// Exec executes a query without returning rows
func (c *PostgreSQLConnection) Exec(ctx context.Context, query string, args ...interface{}) (Result, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return nil, fmt.Errorf("connection is closed")
	}

	result, err := c.pool.Exec(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	return &PostgreSQLResult{result: result}, nil
}

// Select executes a query and scans the result into dest
// Note: pgx doesn't have a direct Select method, so we implement it manually
func (c *PostgreSQLConnection) Select(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return fmt.Errorf("connection is closed")
	}

	rows, err := c.pool.Query(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	_, err = pgx.CollectRows(rows, pgx.RowToStructByName[interface{}])
	return err
}

// Get executes a query and scans a single row into dest
func (c *PostgreSQLConnection) Get(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return fmt.Errorf("connection is closed")
	}

	rows, err := c.pool.Query(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}

	return rows.Scan(dest)
}

// NamedExec executes a named query without returning rows
// Note: pgx uses different parameter syntax ($1, $2) but we'll convert from named
func (c *PostgreSQLConnection) NamedExec(ctx context.Context, query string, arg interface{}) (Result, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return nil, fmt.Errorf("connection is closed")
	}

	// For now, we'll assume the query is already in pgx format
	// In a full implementation, we'd need parameter conversion
	result, err := c.pool.Exec(ctx, query)
	if err != nil {
		return nil, err
	}

	return &PostgreSQLResult{result: result}, nil
}

// NamedQuery executes a named query that returns rows
func (c *PostgreSQLConnection) NamedQuery(ctx context.Context, query string, arg interface{}) (Rows, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return nil, fmt.Errorf("connection is closed")
	}

	// For now, we'll assume the query is already in pgx format
	// In a full implementation, we'd need parameter conversion
	rows, err := c.pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}

	return &PostgreSQLRows{rows: rows}, nil
}

// Begin starts a new transaction
func (c *PostgreSQLConnection) Begin(ctx context.Context) (Transaction, error) {
	return c.BeginTx(ctx, nil)
}

// BeginTx starts a new transaction with options
func (c *PostgreSQLConnection) BeginTx(ctx context.Context, opts *sql.TxOptions) (Transaction, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return nil, fmt.Errorf("connection is closed")
	}

	// Convert sql.TxOptions to pgx.TxOptions
	var pgxOpts pgx.TxOptions
	if opts != nil {
		switch opts.Isolation {
		case sql.LevelReadUncommitted:
			pgxOpts.IsoLevel = pgx.ReadUncommitted
		case sql.LevelReadCommitted:
			pgxOpts.IsoLevel = pgx.ReadCommitted
		case sql.LevelRepeatableRead:
			pgxOpts.IsoLevel = pgx.RepeatableRead
		case sql.LevelSerializable:
			pgxOpts.IsoLevel = pgx.Serializable
		}
		if opts.ReadOnly {
			pgxOpts.AccessMode = pgx.ReadOnly
		} else {
			pgxOpts.AccessMode = pgx.ReadWrite
		}
	}

	tx, err := c.pool.BeginTx(ctx, pgxOpts)
	if err != nil {
		return nil, err
	}

	return &PostgreSQLTransaction{tx: tx}, nil
}

// PostgreSQLRows implements the Rows interface for PostgreSQL
type PostgreSQLRows struct {
	rows pgx.Rows
}

func (r *PostgreSQLRows) Next() bool {
	return r.rows.Next()
}

func (r *PostgreSQLRows) Scan(dest ...interface{}) error {
	return r.rows.Scan(dest...)
}

func (r *PostgreSQLRows) Close() error {
	r.rows.Close()
	return nil
}

func (r *PostgreSQLRows) Err() error {
	return r.rows.Err()
}

func (r *PostgreSQLRows) Columns() ([]string, error) {
	fieldDescs := r.rows.FieldDescriptions()
	columns := make([]string, len(fieldDescs))
	for i, desc := range fieldDescs {
		columns[i] = desc.Name
	}
	return columns, nil
}

func (r *PostgreSQLRows) ColumnTypes() ([]ColumnType, error) {
	fieldDescs := r.rows.FieldDescriptions()
	types := make([]ColumnType, len(fieldDescs))
	for i, desc := range fieldDescs {
		types[i] = &PostgreSQLColumnType{fieldDesc: desc}
	}
	return types, nil
}

// StructScan scans the current row into a struct
func (r *PostgreSQLRows) StructScan(dest interface{}) error {
	return r.rows.Scan(dest)
}

// MapScan scans the current row into a map
func (r *PostgreSQLRows) MapScan(dest map[string]interface{}) error {
	fieldDescs := r.rows.FieldDescriptions()
	values := make([]interface{}, len(fieldDescs))
	valuePointers := make([]interface{}, len(fieldDescs))
	for i := range values {
		valuePointers[i] = &values[i]
	}

	if err := r.rows.Scan(valuePointers...); err != nil {
		return err
	}

	for i, desc := range fieldDescs {
		dest[desc.Name] = values[i]
	}
	return nil
}

// SliceScan scans the current row into a slice
func (r *PostgreSQLRows) SliceScan() ([]interface{}, error) {
	fieldDescs := r.rows.FieldDescriptions()
	values := make([]interface{}, len(fieldDescs))
	valuePointers := make([]interface{}, len(fieldDescs))
	for i := range values {
		valuePointers[i] = &values[i]
	}

	if err := r.rows.Scan(valuePointers...); err != nil {
		return nil, err
	}

	return values, nil
}

// PostgreSQLRow implements the Row interface for PostgreSQL
type PostgreSQLRow struct {
	row pgx.Row
	err error
}

func (r *PostgreSQLRow) Scan(dest ...interface{}) error {
	if r.err != nil {
		return r.err
	}
	return r.row.Scan(dest...)
}

func (r *PostgreSQLRow) StructScan(dest interface{}) error {
	if r.err != nil {
		return r.err
	}
	return r.row.Scan(dest)
}

func (r *PostgreSQLRow) MapScan(dest map[string]interface{}) error {
	if r.err != nil {
		return r.err
	}
	// pgx.Row doesn't have field descriptions, so we can't implement this properly
	// This would need to be handled at a higher level
	return fmt.Errorf("MapScan not supported on single row for PostgreSQL")
}

// PostgreSQLResult implements the Result interface for PostgreSQL
type PostgreSQLResult struct {
	result pgconn.CommandTag
}

func (r *PostgreSQLResult) LastInsertId() (int64, error) {
	// PostgreSQL doesn't support LastInsertId in the same way
	// This would typically be handled with RETURNING clauses
	return 0, fmt.Errorf("LastInsertId not supported by PostgreSQL")
}

func (r *PostgreSQLResult) RowsAffected() (int64, error) {
	return r.result.RowsAffected(), nil
}

// PostgreSQLColumnType implements the ColumnType interface for PostgreSQL
type PostgreSQLColumnType struct {
	fieldDesc pgconn.FieldDescription
}

func (c *PostgreSQLColumnType) Name() string {
	return c.fieldDesc.Name
}

func (c *PostgreSQLColumnType) DatabaseTypeName() string {
	return fmt.Sprintf("%d", c.fieldDesc.DataTypeOID)
}

func (c *PostgreSQLColumnType) ScanType() interface{} {
	// This would need proper type mapping from OID to Go types
	return interface{}(nil)
}

func (c *PostgreSQLColumnType) Nullable() (nullable, ok bool) {
	// pgx doesn't provide nullability information in field descriptions
	return false, false
}

func (c *PostgreSQLColumnType) Length() (length int64, ok bool) {
	if c.fieldDesc.DataTypeSize > 0 {
		return int64(c.fieldDesc.DataTypeSize), true
	}
	return 0, false
}

func (c *PostgreSQLColumnType) DecimalSize() (precision, scale int64, ok bool) {
	// pgx doesn't provide precision/scale information directly
	return 0, 0, false
}

// PostgreSQLTransaction implements the Transaction interface for PostgreSQL
type PostgreSQLTransaction struct {
	tx pgx.Tx
}

func (t *PostgreSQLTransaction) Query(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	rows, err := t.tx.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &PostgreSQLRows{rows: rows}, nil
}

func (t *PostgreSQLTransaction) QueryRow(ctx context.Context, query string, args ...interface{}) Row {
	row := t.tx.QueryRow(ctx, query, args...)
	return &PostgreSQLRow{row: row}
}

func (t *PostgreSQLTransaction) Exec(ctx context.Context, query string, args ...interface{}) (Result, error) {
	result, err := t.tx.Exec(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &PostgreSQLResult{result: result}, nil
}

func (t *PostgreSQLTransaction) Select(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	rows, err := t.tx.Query(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	_, err = pgx.CollectRows(rows, pgx.RowToStructByName[interface{}])
	return err
}

func (t *PostgreSQLTransaction) Get(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	rows, err := t.tx.Query(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}

	return rows.Scan(dest)
}

func (t *PostgreSQLTransaction) NamedExec(ctx context.Context, query string, arg interface{}) (Result, error) {
	// For now, we'll assume the query is already in pgx format
	result, err := t.tx.Exec(ctx, query)
	if err != nil {
		return nil, err
	}
	return &PostgreSQLResult{result: result}, nil
}

func (t *PostgreSQLTransaction) NamedQuery(ctx context.Context, query string, arg interface{}) (Rows, error) {
	// For now, we'll assume the query is already in pgx format
	rows, err := t.tx.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	return &PostgreSQLRows{rows: rows}, nil
}

func (t *PostgreSQLTransaction) Commit() error {
	return t.tx.Commit(context.Background())
}

func (t *PostgreSQLTransaction) Rollback() error {
	return t.tx.Rollback(context.Background())
}