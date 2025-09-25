package db

import (
	"context"
	"encoding/xml"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/microsoft/go-mssqldb"
	"github.com/jmoiron/sqlx"
)

// SQLServerDialect provides SQL Server-specific enhancements and optimizations
type SQLServerDialect struct {
	conn   *SQLServerConnection
	config *SQLServerConfig
	mu     sync.RWMutex
}

// NewSQLServerDialect creates a new SQL Server dialect instance
func NewSQLServerDialect(conn *SQLServerConnection) *SQLServerDialect {
	return &SQLServerDialect{
		conn:   conn,
		config: DefaultSQLServerConfig(),
	}
}

// SQLServerConfig holds SQL Server-specific configuration
type SQLServerConfig struct {
	// Authentication
	UseAzureAD       bool          `json:"use_azure_ad"`
	TenantID         string        `json:"tenant_id"`
	ClientID         string        `json:"client_id"`
	ClientSecret     string        `json:"client_secret"`
	AuthTimeout      time.Duration `json:"auth_timeout"`

	// Connection settings
	ApplicationName  string        `json:"application_name"`
	Database         string        `json:"database"`
	Encrypt          string        `json:"encrypt"` // "true", "false", "disable"
	TrustServerCert  bool          `json:"trust_server_cert"`
	ConnectionTimeout time.Duration `json:"connection_timeout"`
	LoginTimeout     time.Duration `json:"login_timeout"`

	// Performance settings
	PacketSize       int           `json:"packet_size"`
	KeepAlive        int           `json:"keep_alive"`

	// Bulk operations
	BulkCopyTimeout  time.Duration `json:"bulk_copy_timeout"`
	BulkCopyBatchSize int          `json:"bulk_copy_batch_size"`
}

// DefaultSQLServerConfig returns sensible defaults for SQL Server
func DefaultSQLServerConfig() *SQLServerConfig {
	return &SQLServerConfig{
		UseAzureAD:        false,
		AuthTimeout:       30 * time.Second,
		ApplicationName:   "gorp-sqlserver",
		Encrypt:          "true",
		TrustServerCert:  false,
		ConnectionTimeout: 30 * time.Second,
		LoginTimeout:     30 * time.Second,
		PacketSize:       4096,
		KeepAlive:        30,
		BulkCopyTimeout:  30 * time.Second,
		BulkCopyBatchSize: 10000,
	}
}

// TableValuedParameter represents a SQL Server table-valued parameter
type TableValuedParameter struct {
	TypeName string
	Columns  []TVPColumn
	Rows     [][]interface{}
}

// TVPColumn represents a column in a table-valued parameter
type TVPColumn struct {
	Name     string
	Type     string
	Nullable bool
}

// NewTableValuedParameter creates a new table-valued parameter
func NewTableValuedParameter(typeName string, columns []TVPColumn) *TableValuedParameter {
	return &TableValuedParameter{
		TypeName: typeName,
		Columns:  columns,
		Rows:     make([][]interface{}, 0),
	}
}

// AddRow adds a row to the table-valued parameter
func (tvp *TableValuedParameter) AddRow(values ...interface{}) error {
	if len(values) != len(tvp.Columns) {
		return fmt.Errorf("expected %d values, got %d", len(tvp.Columns), len(values))
	}

	tvp.Rows = append(tvp.Rows, values)
	return nil
}

// AddRowsFromSlice adds multiple rows from a slice of structs
func (tvp *TableValuedParameter) AddRowsFromSlice(slice interface{}) error {
	v := reflect.ValueOf(slice)
	if v.Kind() != reflect.Slice {
		return fmt.Errorf("expected slice, got %T", slice)
	}

	for i := 0; i < v.Len(); i++ {
		item := v.Index(i)
		values, err := tvp.extractValuesFromStruct(item)
		if err != nil {
			return fmt.Errorf("failed to extract values from row %d: %w", i, err)
		}
		tvp.Rows = append(tvp.Rows, values)
	}

	return nil
}

// extractValuesFromStruct extracts field values from a struct in column order
func (tvp *TableValuedParameter) extractValuesFromStruct(item reflect.Value) ([]interface{}, error) {
	if item.Kind() == reflect.Ptr {
		item = item.Elem()
	}

	if item.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected struct, got %s", item.Kind())
	}

	values := make([]interface{}, len(tvp.Columns))
	itemType := item.Type()

	for i, col := range tvp.Columns {
		var fieldValue interface{}
		found := false

		// Try to find field by name or db tag
		for j := 0; j < item.NumField(); j++ {
			field := itemType.Field(j)

			if !field.IsExported() {
				continue
			}

			fieldName := field.Tag.Get("db")
			if fieldName == "" {
				fieldName = strings.ToLower(field.Name)
			}

			if fieldName == strings.ToLower(col.Name) {
				fieldValue = item.Field(j).Interface()
				found = true
				break
			}
		}

		if !found && !col.Nullable {
			return nil, fmt.Errorf("required column %s not found in struct", col.Name)
		}

		values[i] = fieldValue
	}

	return values, nil
}

// ExecuteWithTVP executes a stored procedure with table-valued parameters
func (d *SQLServerDialect) ExecuteWithTVP(ctx context.Context, proc string, params map[string]interface{}, tvps map[string]*TableValuedParameter) (Result, error) {
	// Build the procedure call
	var args []interface{}
	var paramNames []string

	// Add regular parameters
	for name, value := range params {
		paramNames = append(paramNames, fmt.Sprintf("@%s = ?", name))
		args = append(args, value)
	}

	// Add table-valued parameters
	for name, tvp := range tvps {
		paramNames = append(paramNames, fmt.Sprintf("@%s = ?", name))

		// Convert TVP to mssql.TVP
		mssqlTVP := d.convertToMSSQLTVP(tvp)
		args = append(args, mssqlTVP)
	}

	// Build and execute query
	query := fmt.Sprintf("EXEC %s %s", proc, strings.Join(paramNames, ", "))
	return d.conn.Exec(ctx, query, args...)
}

// convertToMSSQLTVP converts our TVP to mssql driver TVP
func (d *SQLServerDialect) convertToMSSQLTVP(tvp *TableValuedParameter) mssql.TVP {
	// Build column definitions
	columns := make([]mssql.ColumnDefinition, len(tvp.Columns))
	for i, col := range tvp.Columns {
		columns[i] = mssql.ColumnDefinition{
			Name:     col.Name,
			TypeName: col.Type,
			Flags:    0, // Add flags based on col.Nullable if needed
		}
	}

	return mssql.TVP{
		TypeName: tvp.TypeName,
		Columns:  columns,
		Rows:     tvp.Rows,
	}
}

// BulkInsert performs optimized bulk insert using SQL Server's BULK INSERT capabilities
func (d *SQLServerDialect) BulkInsert(ctx context.Context, table string, columns []string, rows [][]interface{}) error {
	if len(rows) == 0 {
		return nil
	}

	// Create a TVP for bulk insert
	tvpColumns := make([]TVPColumn, len(columns))
	for i, col := range columns {
		tvpColumns[i] = TVPColumn{
			Name:     col,
			Type:     "sql_variant", // Use sql_variant for flexibility
			Nullable: true,
		}
	}

	tvp := NewTableValuedParameter("BulkInsertType", tvpColumns)
	for _, row := range rows {
		if err := tvp.AddRow(row...); err != nil {
			return fmt.Errorf("failed to add row to TVP: %w", err)
		}
	}

	// Execute bulk insert using TVP
	query := fmt.Sprintf(`
		INSERT INTO %s (%s)
		SELECT %s FROM @bulk_data
	`, table, strings.Join(columns, ", "), strings.Join(columns, ", "))

	_, err := d.conn.Exec(ctx, query, d.convertToMSSQLTVP(tvp))
	return err
}

// MergeUpsert performs MERGE-based upsert operation
func (d *SQLServerDialect) MergeUpsert(ctx context.Context, table string, columns []string, keyColumns []string, rows [][]interface{}) error {
	if len(rows) == 0 {
		return nil
	}

	// Create TVP for source data
	tvpColumns := make([]TVPColumn, len(columns))
	for i, col := range columns {
		tvpColumns[i] = TVPColumn{
			Name:     col,
			Type:     "sql_variant",
			Nullable: true,
		}
	}

	tvp := NewTableValuedParameter("MergeSourceType", tvpColumns)
	for _, row := range rows {
		if err := tvp.AddRow(row...); err != nil {
			return fmt.Errorf("failed to add row to TVP: %w", err)
		}
	}

	// Build MERGE statement
	var query strings.Builder
	query.WriteString(fmt.Sprintf("MERGE %s AS target ", table))
	query.WriteString("USING @source_data AS source ")

	// ON clause for matching
	onClauses := make([]string, len(keyColumns))
	for i, key := range keyColumns {
		onClauses[i] = fmt.Sprintf("target.%s = source.%s", key, key)
	}
	query.WriteString("ON (" + strings.Join(onClauses, " AND ") + ") ")

	// WHEN MATCHED - UPDATE
	query.WriteString("WHEN MATCHED THEN UPDATE SET ")
	updateClauses := make([]string, 0)
	for _, col := range columns {
		// Skip key columns in UPDATE
		isKeyColumn := false
		for _, key := range keyColumns {
			if col == key {
				isKeyColumn = true
				break
			}
		}
		if !isKeyColumn {
			updateClauses = append(updateClauses, fmt.Sprintf("%s = source.%s", col, col))
		}
	}
	query.WriteString(strings.Join(updateClauses, ", ") + " ")

	// WHEN NOT MATCHED - INSERT
	query.WriteString("WHEN NOT MATCHED THEN INSERT (")
	query.WriteString(strings.Join(columns, ", "))
	query.WriteString(") VALUES (")
	sourceCols := make([]string, len(columns))
	for i, col := range columns {
		sourceCols[i] = "source." + col
	}
	query.WriteString(strings.Join(sourceCols, ", ") + ");")

	_, err := d.conn.Exec(ctx, query.String(), d.convertToMSSQLTVP(tvp))
	return err
}

// AzureADAuthenticator handles Azure Active Directory authentication
type AzureADAuthenticator struct {
	config *SQLServerConfig
}

// NewAzureADAuthenticator creates a new Azure AD authenticator
func NewAzureADAuthenticator(config *SQLServerConfig) *AzureADAuthenticator {
	return &AzureADAuthenticator{config: config}
}

// BuildConnectionString builds a connection string with Azure AD authentication
func (auth *AzureADAuthenticator) BuildConnectionString(server, database string) string {
	var parts []string

	parts = append(parts, fmt.Sprintf("server=%s", server))
	parts = append(parts, fmt.Sprintf("database=%s", database))

	if auth.config.UseAzureAD {
		// Azure AD Service Principal authentication
		if auth.config.ClientID != "" && auth.config.ClientSecret != "" {
			parts = append(parts, "authenticator=ActiveDirectoryServicePrincipal")
			parts = append(parts, fmt.Sprintf("user id=%s", auth.config.ClientID))
			parts = append(parts, fmt.Sprintf("password=%s", auth.config.ClientSecret))
			if auth.config.TenantID != "" {
				parts = append(parts, fmt.Sprintf("tenant id=%s", auth.config.TenantID))
			}
		} else {
			// Azure AD Interactive/Integrated authentication
			parts = append(parts, "authenticator=ActiveDirectoryIntegrated")
		}
	}

	// Connection settings
	parts = append(parts, fmt.Sprintf("encrypt=%s", auth.config.Encrypt))
	parts = append(parts, fmt.Sprintf("trust server certificate=%t", auth.config.TrustServerCert))
	parts = append(parts, fmt.Sprintf("connection timeout=%d", int(auth.config.ConnectionTimeout.Seconds())))
	parts = append(parts, fmt.Sprintf("log=%d", 0)) // Disable driver logging by default

	if auth.config.ApplicationName != "" {
		parts = append(parts, fmt.Sprintf("app name=%s", auth.config.ApplicationName))
	}

	return strings.Join(parts, ";")
}

// SQLServerSchemaInspector provides schema inspection capabilities
type SQLServerSchemaInspector struct {
	conn *SQLServerConnection
}

// NewSQLServerSchemaInspector creates a schema inspector
func NewSQLServerSchemaInspector(conn *SQLServerConnection) *SQLServerSchemaInspector {
	return &SQLServerSchemaInspector{conn: conn}
}

// GetTableInfo retrieves comprehensive table information
func (si *SQLServerSchemaInspector) GetTableInfo(ctx context.Context, schema, table string) (*SQLServerTableInfo, error) {
	info := &SQLServerTableInfo{
		Schema: schema,
		Name:   table,
	}

	// Get columns
	columns, err := si.getTableColumns(ctx, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}
	info.Columns = columns

	// Get indexes
	indexes, err := si.getTableIndexes(ctx, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get indexes: %w", err)
	}
	info.Indexes = indexes

	// Get foreign keys
	foreignKeys, err := si.getTableForeignKeys(ctx, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get foreign keys: %w", err)
	}
	info.ForeignKeys = foreignKeys

	return info, nil
}

// SQLServerTableInfo contains comprehensive table information
type SQLServerTableInfo struct {
	Schema      string                     `json:"schema"`
	Name        string                     `json:"name"`
	Columns     []SQLServerColumnInfo      `json:"columns"`
	Indexes     []SQLServerIndexInfo       `json:"indexes"`
	ForeignKeys []SQLServerForeignKeyInfo  `json:"foreign_keys"`
}

// SQLServerColumnInfo contains column information
type SQLServerColumnInfo struct {
	Name               string  `db:"COLUMN_NAME" json:"name"`
	DataType           string  `db:"DATA_TYPE" json:"data_type"`
	MaxLength          *int    `db:"CHARACTER_MAXIMUM_LENGTH" json:"max_length"`
	NumericPrecision   *int    `db:"NUMERIC_PRECISION" json:"numeric_precision"`
	NumericScale       *int    `db:"NUMERIC_SCALE" json:"numeric_scale"`
	IsNullable         string  `db:"IS_NULLABLE" json:"is_nullable"`
	ColumnDefault      *string `db:"COLUMN_DEFAULT" json:"column_default"`
	IsIdentity         bool    `db:"IS_IDENTITY" json:"is_identity"`
	IsComputed         bool    `db:"IS_COMPUTED" json:"is_computed"`
}

// SQLServerIndexInfo contains index information
type SQLServerIndexInfo struct {
	Name         string `db:"index_name" json:"name"`
	IsUnique     bool   `db:"is_unique" json:"is_unique"`
	IsPrimaryKey bool   `db:"is_primary_key" json:"is_primary_key"`
	Type         string `db:"type_desc" json:"type"`
	Columns      string `db:"columns" json:"columns"`
}

// SQLServerForeignKeyInfo contains foreign key information
type SQLServerForeignKeyInfo struct {
	Name               string `db:"CONSTRAINT_NAME" json:"name"`
	Column             string `db:"COLUMN_NAME" json:"column"`
	ReferencedTable    string `db:"REFERENCED_TABLE_NAME" json:"referenced_table"`
	ReferencedColumn   string `db:"REFERENCED_COLUMN_NAME" json:"referenced_column"`
	UpdateRule         string `db:"UPDATE_RULE" json:"update_rule"`
	DeleteRule         string `db:"DELETE_RULE" json:"delete_rule"`
}

// getTableColumns retrieves column information for a table
func (si *SQLServerSchemaInspector) getTableColumns(ctx context.Context, schema, table string) ([]SQLServerColumnInfo, error) {
	query := `
		SELECT
			c.COLUMN_NAME,
			c.DATA_TYPE,
			c.CHARACTER_MAXIMUM_LENGTH,
			c.NUMERIC_PRECISION,
			c.NUMERIC_SCALE,
			c.IS_NULLABLE,
			c.COLUMN_DEFAULT,
			COLUMNPROPERTY(OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') AS IS_IDENTITY,
			COLUMNPROPERTY(OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME), c.COLUMN_NAME, 'IsComputed') AS IS_COMPUTED
		FROM INFORMATION_SCHEMA.COLUMNS c
		WHERE c.TABLE_SCHEMA = @schema AND c.TABLE_NAME = @table
		ORDER BY c.ORDINAL_POSITION
	`

	var columns []SQLServerColumnInfo
	err := si.conn.Select(ctx, &columns, query,
		sql.Named("schema", schema),
		sql.Named("table", table))

	return columns, err
}

// getTableIndexes retrieves index information for a table
func (si *SQLServerSchemaInspector) getTableIndexes(ctx context.Context, schema, table string) ([]SQLServerIndexInfo, error) {
	query := `
		SELECT
			i.name AS index_name,
			i.is_unique,
			i.is_primary_key,
			i.type_desc,
			STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.index_column_id) AS columns
		FROM sys.indexes i
		INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
		INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
		INNER JOIN sys.tables t ON i.object_id = t.object_id
		INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
		WHERE s.name = @schema AND t.name = @table
		GROUP BY i.name, i.is_unique, i.is_primary_key, i.type_desc
		ORDER BY i.name
	`

	var indexes []SQLServerIndexInfo
	err := si.conn.Select(ctx, &indexes, query,
		sql.Named("schema", schema),
		sql.Named("table", table))

	return indexes, err
}

// getTableForeignKeys retrieves foreign key information for a table
func (si *SQLServerSchemaInspector) getTableForeignKeys(ctx context.Context, schema, table string) ([]SQLServerForeignKeyInfo, error) {
	query := `
		SELECT
			rc.CONSTRAINT_NAME,
			kcu.COLUMN_NAME,
			kcu.REFERENCED_TABLE_NAME,
			kcu.REFERENCED_COLUMN_NAME,
			rc.UPDATE_RULE,
			rc.DELETE_RULE
		FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
		INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
			ON rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
		WHERE kcu.TABLE_SCHEMA = @schema AND kcu.TABLE_NAME = @table
		ORDER BY rc.CONSTRAINT_NAME
	`

	var foreignKeys []SQLServerForeignKeyInfo
	err := si.conn.Select(ctx, &foreignKeys, query,
		sql.Named("schema", schema),
		sql.Named("table", table))

	return foreignKeys, err
}

// SQLServerHealthChecker provides SQL Server-specific health checking
type SQLServerHealthChecker struct {
	conn *SQLServerConnection
}

// NewSQLServerHealthChecker creates a SQL Server health checker
func NewSQLServerHealthChecker(conn *SQLServerConnection) *SQLServerHealthChecker {
	return &SQLServerHealthChecker{conn: conn}
}

// CheckHealth performs comprehensive SQL Server health check
func (h *SQLServerHealthChecker) CheckHealth(ctx context.Context) (*SQLServerHealthStatus, error) {
	status := &SQLServerHealthStatus{
		Timestamp: time.Now(),
	}

	// Basic connectivity check
	if err := h.conn.Ping(ctx); err != nil {
		status.Connected = false
		status.Error = err.Error()
		return status, err
	}
	status.Connected = true

	// Get server properties
	properties, err := h.getServerProperties(ctx)
	if err != nil {
		status.Error = fmt.Sprintf("failed to get server properties: %v", err)
		return status, err
	}
	status.ServerProperties = properties

	// Get database info
	dbInfo, err := h.getDatabaseInfo(ctx)
	if err != nil {
		status.DatabaseError = err.Error()
	} else {
		status.DatabaseInfo = dbInfo
	}

	// Get connection stats
	status.ConnectionStats = h.conn.Stats()

	return status, nil
}

// SQLServerHealthStatus represents SQL Server health check results
type SQLServerHealthStatus struct {
	Timestamp        time.Time                 `json:"timestamp"`
	Connected        bool                      `json:"connected"`
	Error            string                    `json:"error,omitempty"`
	ServerProperties map[string]string         `json:"server_properties,omitempty"`
	DatabaseInfo     *SQLServerDatabaseInfo    `json:"database_info,omitempty"`
	DatabaseError    string                    `json:"database_error,omitempty"`
	ConnectionStats  ConnectionStats           `json:"connection_stats"`
}

// SQLServerDatabaseInfo contains database information
type SQLServerDatabaseInfo struct {
	Name             string `db:"name" json:"name"`
	DatabaseID       int    `db:"database_id" json:"database_id"`
	CollationName    string `db:"collation_name" json:"collation_name"`
	CompatibilityLevel int  `db:"compatibility_level" json:"compatibility_level"`
	State            string `db:"state_desc" json:"state"`
	RecoveryModel    string `db:"recovery_model_desc" json:"recovery_model"`
}

// getServerProperties retrieves important SQL Server properties
func (h *SQLServerHealthChecker) getServerProperties(ctx context.Context) (map[string]string, error) {
	query := `
		SELECT
			SERVERPROPERTY('ProductVersion') AS ProductVersion,
			SERVERPROPERTY('ProductLevel') AS ProductLevel,
			SERVERPROPERTY('Edition') AS Edition,
			SERVERPROPERTY('EngineEdition') AS EngineEdition,
			SERVERPROPERTY('MachineName') AS MachineName,
			SERVERPROPERTY('ServerName') AS ServerName,
			SERVERPROPERTY('IsClustered') AS IsClustered,
			SERVERPROPERTY('IsHadrEnabled') AS IsHadrEnabled
	`

	rows, err := h.conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	properties := make(map[string]string)
	if rows.Next() {
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

		for i, col := range columns {
			if values[i] != nil {
				properties[col] = fmt.Sprintf("%v", values[i])
			}
		}
	}

	return properties, rows.Err()
}

// getDatabaseInfo retrieves current database information
func (h *SQLServerHealthChecker) getDatabaseInfo(ctx context.Context) (*SQLServerDatabaseInfo, error) {
	query := `
		SELECT
			name,
			database_id,
			collation_name,
			compatibility_level,
			state_desc,
			recovery_model_desc
		FROM sys.databases
		WHERE database_id = DB_ID()
	`

	var dbInfo SQLServerDatabaseInfo
	err := h.conn.Get(ctx, &dbInfo, query)
	if err != nil {
		return nil, err
	}

	return &dbInfo, nil
}