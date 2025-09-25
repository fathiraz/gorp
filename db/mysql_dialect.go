package db

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

// MySQLDialect provides MySQL-specific enhancements and optimizations
type MySQLDialect struct {
	conn *MySQLConnection
}

// NewMySQLDialect creates a new MySQL dialect instance
func NewMySQLDialect(conn *MySQLConnection) *MySQLDialect {
	return &MySQLDialect{conn: conn}
}

// JSONColumn represents a MySQL JSON column type with enhanced functionality
type JSONColumn[T any] struct {
	Value T
	Valid bool
}

// Scan implements the sql.Scanner interface for JSON columns
func (j *JSONColumn[T]) Scan(value interface{}) error {
	if value == nil {
		j.Valid = false
		return nil
	}

	bytes, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("cannot scan %T into JSONColumn", value)
	}

	if err := json.Unmarshal(bytes, &j.Value); err != nil {
		return fmt.Errorf("failed to unmarshal JSON: %w", err)
	}

	j.Valid = true
	return nil
}

// Value implements the driver.Valuer interface for JSON columns
func (j JSONColumn[T]) Value() (driver.Value, error) {
	if !j.Valid {
		return nil, nil
	}

	bytes, err := json.Marshal(j.Value)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal JSON: %w", err)
	}

	return bytes, nil
}

// GeneratedColumn represents a MySQL generated column
type GeneratedColumn struct {
	Expression string
	Stored     bool // true for STORED, false for VIRTUAL
}

// MySQLBulkInsertOptions holds options for bulk insert operations
type MySQLBulkInsertOptions struct {
	OnDuplicateKeyUpdate bool
	UpdateColumns        []string
	BatchSize            int
	IgnoreErrors         bool
}

// DefaultBulkInsertOptions returns sensible defaults for bulk insert
func DefaultBulkInsertOptions() *MySQLBulkInsertOptions {
	return &MySQLBulkInsertOptions{
		OnDuplicateKeyUpdate: false,
		UpdateColumns:        nil,
		BatchSize:            1000,
		IgnoreErrors:         false,
	}
}

// BulkInsert performs optimized bulk insert with ON DUPLICATE KEY UPDATE support
func (d *MySQLDialect) BulkInsert(ctx context.Context, table string, rows []interface{}, opts *MySQLBulkInsertOptions) error {
	if len(rows) == 0 {
		return nil
	}

	if opts == nil {
		opts = DefaultBulkInsertOptions()
	}

	// Process in batches
	for i := 0; i < len(rows); i += opts.BatchSize {
		end := i + opts.BatchSize
		if end > len(rows) {
			end = len(rows)
		}

		batch := rows[i:end]
		if err := d.executeBulkInsertBatch(ctx, table, batch, opts); err != nil {
			return fmt.Errorf("bulk insert batch failed at row %d: %w", i, err)
		}
	}

	return nil
}

// executeBulkInsertBatch executes a single batch of bulk insert
func (d *MySQLDialect) executeBulkInsertBatch(ctx context.Context, table string, batch []interface{}, opts *MySQLBulkInsertOptions) error {
	if len(batch) == 0 {
		return nil
	}

	// Get column names from the first row
	columns, err := d.extractColumns(batch[0])
	if err != nil {
		return fmt.Errorf("failed to extract columns: %w", err)
	}

	// Build the INSERT query
	query := d.buildBulkInsertQuery(table, columns, len(batch), opts)

	// Prepare arguments
	args := make([]interface{}, 0, len(batch)*len(columns))
	for _, row := range batch {
		rowArgs, err := d.extractValues(row, columns)
		if err != nil {
			return fmt.Errorf("failed to extract row values: %w", err)
		}
		args = append(args, rowArgs...)
	}

	// Execute the query
	_, err = d.conn.Exec(ctx, query, args...)
	return err
}

// buildBulkInsertQuery constructs the bulk insert query with optional ON DUPLICATE KEY UPDATE
func (d *MySQLDialect) buildBulkInsertQuery(table string, columns []string, rowCount int, opts *MySQLBulkInsertOptions) string {
	var query strings.Builder

	// INSERT or INSERT IGNORE
	if opts.IgnoreErrors {
		query.WriteString("INSERT IGNORE INTO ")
	} else {
		query.WriteString("INSERT INTO ")
	}

	query.WriteString(table)
	query.WriteString(" (")
	query.WriteString(strings.Join(columns, ", "))
	query.WriteString(") VALUES ")

	// Build VALUES clause
	placeholderRow := "(" + strings.Repeat("?, ", len(columns)-1) + "?)"
	for i := 0; i < rowCount; i++ {
		if i > 0 {
			query.WriteString(", ")
		}
		query.WriteString(placeholderRow)
	}

	// Add ON DUPLICATE KEY UPDATE if specified
	if opts.OnDuplicateKeyUpdate {
		query.WriteString(" ON DUPLICATE KEY UPDATE ")

		updateColumns := opts.UpdateColumns
		if len(updateColumns) == 0 {
			// Update all columns except the primary key (assumes 'id')
			updateColumns = make([]string, 0, len(columns))
			for _, col := range columns {
				if col != "id" {
					updateColumns = append(updateColumns, col)
				}
			}
		}

		updateClauses := make([]string, len(updateColumns))
		for i, col := range updateColumns {
			updateClauses[i] = fmt.Sprintf("%s = VALUES(%s)", col, col)
		}
		query.WriteString(strings.Join(updateClauses, ", "))
	}

	return query.String()
}

// extractColumns extracts column names from a struct using reflection
func (d *MySQLDialect) extractColumns(row interface{}) ([]string, error) {
	val := reflect.ValueOf(row)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return nil, fmt.Errorf("row must be a struct or pointer to struct")
	}

	typ := val.Type()
	columns := make([]string, 0, val.NumField())

	for i := 0; i < val.NumField(); i++ {
		field := typ.Field(i)

		// Skip unexported fields
		if !field.IsExported() {
			continue
		}

		// Get column name from db tag or field name
		columnName := field.Tag.Get("db")
		if columnName == "" {
			columnName = strings.ToLower(field.Name)
		}

		// Skip fields marked with db:"-"
		if columnName == "-" {
			continue
		}

		columns = append(columns, columnName)
	}

	return columns, nil
}

// extractValues extracts field values from a struct in column order
func (d *MySQLDialect) extractValues(row interface{}, columns []string) ([]interface{}, error) {
	val := reflect.ValueOf(row)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return nil, fmt.Errorf("row must be a struct or pointer to struct")
	}

	typ := val.Type()
	values := make([]interface{}, 0, len(columns))

	// Create a map of field names to values for efficient lookup
	fieldMap := make(map[string]interface{})
	for i := 0; i < val.NumField(); i++ {
		field := typ.Field(i)

		if !field.IsExported() {
			continue
		}

		columnName := field.Tag.Get("db")
		if columnName == "" {
			columnName = strings.ToLower(field.Name)
		}

		if columnName == "-" {
			continue
		}

		fieldMap[columnName] = val.Field(i).Interface()
	}

	// Extract values in column order
	for _, column := range columns {
		value, exists := fieldMap[column]
		if !exists {
			return nil, fmt.Errorf("column %s not found in struct", column)
		}
		values = append(values, value)
	}

	return values, nil
}

// UpsertQuery builds an optimized UPSERT query using ON DUPLICATE KEY UPDATE
func (d *MySQLDialect) UpsertQuery(table string, columns []string, conflictColumns []string) string {
	var query strings.Builder

	query.WriteString("INSERT INTO ")
	query.WriteString(table)
	query.WriteString(" (")
	query.WriteString(strings.Join(columns, ", "))
	query.WriteString(") VALUES (")
	query.WriteString(strings.Repeat("?, ", len(columns)-1))
	query.WriteString("?) ON DUPLICATE KEY UPDATE ")

	// Update all non-conflict columns
	updateClauses := make([]string, 0)
	for _, col := range columns {
		// Skip conflict columns (typically primary key or unique key columns)
		isConflictColumn := false
		for _, conflictCol := range conflictColumns {
			if col == conflictCol {
				isConflictColumn = true
				break
			}
		}

		if !isConflictColumn {
			updateClauses = append(updateClauses, fmt.Sprintf("%s = VALUES(%s)", col, col))
		}
	}

	query.WriteString(strings.Join(updateClauses, ", "))
	return query.String()
}

// CreateJSONIndex creates an optimized index on JSON column paths
func (d *MySQLDialect) CreateJSONIndex(ctx context.Context, table, column, path, indexName string) error {
	query := fmt.Sprintf(
		"CREATE INDEX %s ON %s ((%s->>'$.%s'))",
		indexName, table, column, path,
	)

	_, err := d.conn.Exec(ctx, query)
	return err
}

// JSONExtract extracts a value from a JSON column using MySQL's JSON_EXTRACT function
func (d *MySQLDialect) JSONExtract(column, path string) string {
	return fmt.Sprintf("JSON_EXTRACT(%s, '$.%s')", column, path)
}

// JSONSet updates a value in a JSON column using MySQL's JSON_SET function
func (d *MySQLDialect) JSONSet(column, path string, value interface{}) string {
	return fmt.Sprintf("JSON_SET(%s, '$.%s', ?)", column, path)
}

// MySQLConnectionPool provides enhanced connection pooling for MySQL
type MySQLConnectionPool struct {
	primary  *MySQLConnection
	replicas []*MySQLConnection
	config   *MySQLPoolConfig
}

// MySQLPoolConfig holds MySQL-specific pool configuration
type MySQLPoolConfig struct {
	ReadTimeout        time.Duration
	WriteTimeout       time.Duration
	AllowNativePasswords bool
	Charset            string
	Collation          string
	ParseTime          bool
	MultiStatements    bool
}

// DefaultMySQLPoolConfig returns sensible defaults for MySQL connection pool
func DefaultMySQLPoolConfig() *MySQLPoolConfig {
	return &MySQLPoolConfig{
		ReadTimeout:          30 * time.Second,
		WriteTimeout:         30 * time.Second,
		AllowNativePasswords: true,
		Charset:             "utf8mb4",
		Collation:           "utf8mb4_unicode_ci",
		ParseTime:           true,
		MultiStatements:     false,
	}
}

// GetReadConnection returns a connection optimized for read operations
func (p *MySQLConnectionPool) GetReadConnection() *MySQLConnection {
	if len(p.replicas) > 0 {
		// Simple round-robin selection for replicas
		// In production, this could be enhanced with health checks and load balancing
		index := time.Now().UnixNano() % int64(len(p.replicas))
		return p.replicas[index]
	}

	// Fall back to primary if no replicas available
	return p.primary
}

// GetWriteConnection returns a connection optimized for write operations
func (p *MySQLConnectionPool) GetWriteConnection() *MySQLConnection {
	return p.primary
}

// MySQLHealthChecker provides MySQL-specific health checking
type MySQLHealthChecker struct {
	conn *MySQLConnection
}

// NewMySQLHealthChecker creates a MySQL health checker
func NewMySQLHealthChecker(conn *MySQLConnection) *MySQLHealthChecker {
	return &MySQLHealthChecker{conn: conn}
}

// CheckHealth performs comprehensive MySQL health check
func (h *MySQLHealthChecker) CheckHealth(ctx context.Context) (*MySQLHealthStatus, error) {
	status := &MySQLHealthStatus{
		Timestamp: time.Now(),
	}

	// Basic connectivity check
	if err := h.conn.Ping(ctx); err != nil {
		status.Connected = false
		status.Error = err.Error()
		return status, err
	}
	status.Connected = true

	// Get server variables
	variables, err := h.getServerVariables(ctx)
	if err != nil {
		status.Error = fmt.Sprintf("failed to get server variables: %v", err)
		return status, err
	}
	status.Variables = variables

	// Check replication status if applicable
	replicationStatus, err := h.getReplicationStatus(ctx)
	if err != nil {
		// Replication status check is optional - don't fail if not available
		status.ReplicationError = err.Error()
	} else {
		status.ReplicationStatus = replicationStatus
	}

	// Get connection stats
	status.ConnectionStats = h.conn.Stats()

	return status, nil
}

// MySQLHealthStatus represents MySQL health check results
type MySQLHealthStatus struct {
	Timestamp         time.Time         `json:"timestamp"`
	Connected         bool              `json:"connected"`
	Error             string            `json:"error,omitempty"`
	Variables         map[string]string `json:"variables,omitempty"`
	ReplicationStatus map[string]string `json:"replication_status,omitempty"`
	ReplicationError  string            `json:"replication_error,omitempty"`
	ConnectionStats   ConnectionStats   `json:"connection_stats"`
}

// getServerVariables retrieves important MySQL server variables
func (h *MySQLHealthChecker) getServerVariables(ctx context.Context) (map[string]string, error) {
	query := `
		SHOW VARIABLES WHERE Variable_name IN (
			'version', 'version_comment', 'innodb_version',
			'max_connections', 'max_allowed_packet',
			'innodb_buffer_pool_size', 'query_cache_size'
		)
	`

	rows, err := h.conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	variables := make(map[string]string)
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			return nil, err
		}
		variables[name] = value
	}

	return variables, rows.Err()
}

// getReplicationStatus retrieves MySQL replication status
func (h *MySQLHealthChecker) getReplicationStatus(ctx context.Context) (map[string]string, error) {
	query := "SHOW SLAVE STATUS"

	rows, err := h.conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, fmt.Errorf("no replication status available")
	}

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	if err := rows.Scan(valuePtrs...); err != nil {
		return nil, err
	}

	status := make(map[string]string)
	for i, col := range columns {
		if values[i] != nil {
			status[col] = fmt.Sprintf("%v", values[i])
		}
	}

	return status, nil
}