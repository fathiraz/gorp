package db

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgreSQLEnhancedConnection extends PostgreSQLConnection with pgx-specific features
type PostgreSQLEnhancedConnection struct {
	*PostgreSQLConnection
	typeMapper *PostgreSQLTypeMapper
	config     *PostgreSQLConfig
}

// PostgreSQLConfig holds PostgreSQL-specific configuration
type PostgreSQLConfig struct {
	PreferSimpleProtocol bool
	ConnMaxLifetime      time.Duration
	ConnMaxIdleTime      time.Duration
	MaxConnections       int32
	MinConnections       int32
	HealthCheckPeriod    time.Duration
	MaxConnLifetimeJitter time.Duration
}

// NewPostgreSQLEnhancedConnection creates an enhanced PostgreSQL connection
func NewPostgreSQLEnhancedConnection(pool *pgxpool.Pool, config ConnectionConfig, pgConfig *PostgreSQLConfig) *PostgreSQLEnhancedConnection {
	baseConn := NewPostgreSQLConnection(pool, config)

	if pgConfig == nil {
		pgConfig = &PostgreSQLConfig{
			PreferSimpleProtocol:      false,
			ConnMaxLifetime:           time.Hour,
			ConnMaxIdleTime:           30 * time.Minute,
			MaxConnections:            25,
			MinConnections:            2,
			HealthCheckPeriod:         30 * time.Second,
			MaxConnLifetimeJitter:     10 * time.Second,
		}
	}

	return &PostgreSQLEnhancedConnection{
		PostgreSQLConnection: baseConn,
		typeMapper:          NewPostgreSQLTypeMapper(),
		config:              pgConfig,
	}
}

// BulkInsert performs bulk insert using pgx.CopyFrom for high performance
func (c *PostgreSQLEnhancedConnection) BulkInsert(ctx context.Context, tableName string, columns []string, data [][]interface{}) (int64, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return 0, fmt.Errorf("connection is closed")
	}

	// Create CopyFrom source
	rows := make([][]interface{}, len(data))
	copy(rows, data)

	copyCount, err := c.pool.CopyFrom(
		ctx,
		pgx.Identifier{tableName},
		columns,
		pgx.CopyFromRows(rows),
	)

	return copyCount, err
}

// BulkInsertFromStruct performs bulk insert from a slice of structs
func (c *PostgreSQLEnhancedConnection) BulkInsertFromStruct(ctx context.Context, tableName string, structs interface{}) (int64, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return 0, fmt.Errorf("connection is closed")
	}

	v := reflect.ValueOf(structs)
	if v.Kind() != reflect.Slice {
		return 0, fmt.Errorf("structs must be a slice")
	}

	if v.Len() == 0 {
		return 0, nil
	}

	// Get struct type and analyze fields
	elemType := v.Type().Elem()
	if elemType.Kind() == reflect.Ptr {
		elemType = elemType.Elem()
	}

	columns, fieldIndices := c.analyzeStructForBulkInsert(elemType)

	// Convert structs to rows
	rows := make([][]interface{}, v.Len())
	for i := 0; i < v.Len(); i++ {
		elem := v.Index(i)
		if elem.Kind() == reflect.Ptr {
			if elem.IsNil() {
				continue
			}
			elem = elem.Elem()
		}

		row := make([]interface{}, len(fieldIndices))
		for j, fieldIdx := range fieldIndices {
			field := elem.Field(fieldIdx)
			row[j] = field.Interface()
		}
		rows[i] = row
	}

	return c.BulkInsert(ctx, tableName, columns, rows)
}

// ExecuteInTransaction executes multiple operations in a single transaction
func (c *PostgreSQLEnhancedConnection) ExecuteInTransaction(ctx context.Context, operations func(pgx.Tx) error) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return fmt.Errorf("connection is closed")
	}

	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if err := operations(tx); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// CreatePartition creates a table partition for PostgreSQL table partitioning
func (c *PostgreSQLEnhancedConnection) CreatePartition(ctx context.Context, parentTable, partitionName string, partitionSpec string) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return fmt.Errorf("connection is closed")
	}

	query := fmt.Sprintf("CREATE TABLE %s PARTITION OF %s %s", partitionName, parentTable, partitionSpec)
	_, err := c.pool.Exec(ctx, query)
	return err
}

// CreateIndex creates an index with PostgreSQL-specific options
func (c *PostgreSQLEnhancedConnection) CreateIndex(ctx context.Context, indexSpec IndexSpec) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return fmt.Errorf("connection is closed")
	}

	query := c.buildIndexQuery(indexSpec)
	_, err := c.pool.Exec(ctx, query)
	return err
}

// Listen starts listening for PostgreSQL NOTIFY events
func (c *PostgreSQLEnhancedConnection) Listen(ctx context.Context, channel string) (*pgx.Conn, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return nil, fmt.Errorf("connection is closed")
	}

	conn, err := c.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}

	_, err = conn.Exec(ctx, "LISTEN "+channel)
	if err != nil {
		conn.Release()
		return nil, err
	}

	return conn.Conn(), nil
}

// Notify sends a PostgreSQL NOTIFY event
func (c *PostgreSQLEnhancedConnection) Notify(ctx context.Context, channel, payload string) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return fmt.Errorf("connection is closed")
	}

	query := "SELECT pg_notify($1, $2)"
	_, err := c.pool.Exec(ctx, query, channel, payload)
	return err
}

// GetConnectionConfig returns the current connection configuration
func (c *PostgreSQLEnhancedConnection) GetConnectionConfig() *PostgreSQLConfig {
	return c.config
}

// SetConnectionConfig updates the connection configuration
func (c *PostgreSQLEnhancedConnection) SetConnectionConfig(config *PostgreSQLConfig) {
	c.config = config
}

// GetPoolStats returns detailed connection pool statistics
func (c *PostgreSQLEnhancedConnection) GetPoolStats() *pgxpool.Stat {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return nil
	}

	stat := c.pool.Stat()
	return stat
}

// PrepareStatement prepares a statement for reuse
func (c *PostgreSQLEnhancedConnection) PrepareStatement(ctx context.Context, name, query string) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return fmt.Errorf("connection is closed")
	}

	_, err := c.pool.Exec(ctx, fmt.Sprintf("PREPARE %s AS %s", name, query))
	return err
}

// ExecutePreparedStatement executes a prepared statement
func (c *PostgreSQLEnhancedConnection) ExecutePreparedStatement(ctx context.Context, name string, args ...interface{}) (pgx.Rows, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if atomic.LoadInt64(&c.closed) == 1 {
		return nil, fmt.Errorf("connection is closed")
	}

	placeholders := make([]string, len(args))
	for i := range args {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	query := fmt.Sprintf("EXECUTE %s(%s)", name, strings.Join(placeholders, ","))
	return c.pool.Query(ctx, query, args...)
}

// IndexSpec represents PostgreSQL index specification
type IndexSpec struct {
	Name        string
	Table       string
	Columns     []string
	IndexType   IndexType
	Unique      bool
	Concurrent  bool
	Where       string
	Include     []string
	With        map[string]string
	Tablespace  string
}

// IndexType represents PostgreSQL index types
type IndexType string

const (
	BTreeIndex IndexType = "BTREE"
	HashIndex  IndexType = "HASH"
	GINIndex   IndexType = "GIN"
	GiSTIndex  IndexType = "GIST"
	SPGiSTIndex IndexType = "SPGIST"
	BRINIndex  IndexType = "BRIN"
)

// buildIndexQuery constructs the CREATE INDEX SQL statement
func (c *PostgreSQLEnhancedConnection) buildIndexQuery(spec IndexSpec) string {
	var parts []string

	// Base CREATE INDEX
	if spec.Unique {
		parts = append(parts, "CREATE UNIQUE INDEX")
	} else {
		parts = append(parts, "CREATE INDEX")
	}

	if spec.Concurrent {
		parts = append(parts, "CONCURRENTLY")
	}

	// Index name
	if spec.Name != "" {
		parts = append(parts, spec.Name)
	}

	// Table
	parts = append(parts, "ON", spec.Table)

	// Index type
	if spec.IndexType != "" {
		parts = append(parts, "USING", string(spec.IndexType))
	}

	// Columns
	columnSpec := "(" + strings.Join(spec.Columns, ", ") + ")"
	parts = append(parts, columnSpec)

	// INCLUDE columns (PostgreSQL 11+)
	if len(spec.Include) > 0 {
		includeSpec := "INCLUDE (" + strings.Join(spec.Include, ", ") + ")"
		parts = append(parts, includeSpec)
	}

	// WITH options
	if len(spec.With) > 0 {
		var withOptions []string
		for key, value := range spec.With {
			withOptions = append(withOptions, fmt.Sprintf("%s = %s", key, value))
		}
		parts = append(parts, "WITH ("+strings.Join(withOptions, ", ")+")")
	}

	// TABLESPACE
	if spec.Tablespace != "" {
		parts = append(parts, "TABLESPACE", spec.Tablespace)
	}

	// WHERE clause for partial indexes
	if spec.Where != "" {
		parts = append(parts, "WHERE", spec.Where)
	}

	return strings.Join(parts, " ")
}

// analyzeStructForBulkInsert analyzes a struct type to determine columns and field indices
func (c *PostgreSQLEnhancedConnection) analyzeStructForBulkInsert(structType reflect.Type) ([]string, []int) {
	var columns []string
	var fieldIndices []int

	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)

		// Skip unexported fields
		if !field.IsExported() {
			continue
		}

		// Check for db tag
		tag := field.Tag.Get("db")
		if tag == "-" {
			continue
		}

		columnName := tag
		if columnName == "" {
			columnName = strings.ToLower(field.Name)
		}

		columns = append(columns, columnName)
		fieldIndices = append(fieldIndices, i)
	}

	return columns, fieldIndices
}

// PostgreSQLBulkOperations provides utilities for bulk database operations
type PostgreSQLBulkOperations struct {
	conn *PostgreSQLEnhancedConnection
}

// NewPostgreSQLBulkOperations creates a new bulk operations helper
func NewPostgreSQLBulkOperations(conn *PostgreSQLEnhancedConnection) *PostgreSQLBulkOperations {
	return &PostgreSQLBulkOperations{conn: conn}
}

// BatchInsert performs batch insert with automatic batching
func (bo *PostgreSQLBulkOperations) BatchInsert(ctx context.Context, tableName string, columns []string, data [][]interface{}, batchSize int) (int64, error) {
	if batchSize <= 0 {
		batchSize = 1000 // Default batch size
	}

	var totalInserted int64

	for i := 0; i < len(data); i += batchSize {
		end := i + batchSize
		if end > len(data) {
			end = len(data)
		}

		batch := data[i:end]
		inserted, err := bo.conn.BulkInsert(ctx, tableName, columns, batch)
		if err != nil {
			return totalInserted, err
		}

		totalInserted += inserted
	}

	return totalInserted, nil
}

// BulkUpdate performs bulk update using PostgreSQL-specific techniques
func (bo *PostgreSQLBulkOperations) BulkUpdate(ctx context.Context, tableName string, updates []BulkUpdateSpec) error {
	if len(updates) == 0 {
		return nil
	}

	// Use temporary table approach for bulk updates
	tempTableName := fmt.Sprintf("temp_bulk_update_%d", time.Now().UnixNano())

	return bo.conn.ExecuteInTransaction(ctx, func(tx pgx.Tx) error {
		// Create temporary table
		createTempTable := fmt.Sprintf("CREATE TEMP TABLE %s AS SELECT * FROM %s WHERE FALSE", tempTableName, tableName)
		_, err := tx.Exec(ctx, createTempTable)
		if err != nil {
			return err
		}

		// Insert update data into temp table
		for _, update := range updates {
			_, err := tx.Exec(ctx, update.InsertSQL, update.Args...)
			if err != nil {
				return err
			}
		}

		// Perform bulk update
		updateQuery := fmt.Sprintf("UPDATE %s SET (%s) = (%s) FROM %s WHERE %s.%s = %s.%s",
			tableName,
			strings.Join(updates[0].UpdateColumns, ", "),
			strings.Join(updates[0].UpdateValues, ", "),
			tempTableName,
			tableName,
			updates[0].KeyColumn,
			tempTableName,
			updates[0].KeyColumn,
		)

		_, err = tx.Exec(ctx, updateQuery)
		return err
	})
}

// BulkUpdateSpec represents a bulk update specification
type BulkUpdateSpec struct {
	KeyColumn     string
	UpdateColumns []string
	UpdateValues  []string
	InsertSQL     string
	Args          []interface{}
}