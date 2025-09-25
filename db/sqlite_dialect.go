package db

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

// SQLiteDialect provides SQLite-specific enhancements and optimizations
type SQLiteDialect struct {
	conn   *SQLiteConnection
	config *SQLitePragmaConfig
	mu     sync.RWMutex
}

// NewSQLiteDialect creates a new SQLite dialect instance
func NewSQLiteDialect(conn *SQLiteConnection) *SQLiteDialect {
	return &SQLiteDialect{
		conn:   conn,
		config: DefaultSQLitePragmaConfig(),
	}
}

// SQLitePragmaConfig holds SQLite PRAGMA configuration
type SQLitePragmaConfig struct {
	// Performance settings
	CacheSize      int    // PRAGMA cache_size
	TempStore      int    // PRAGMA temp_store (0=default, 1=file, 2=memory)
	MmapSize       int64  // PRAGMA mmap_size
	PageSize       int    // PRAGMA page_size

	// Journal and WAL settings
	JournalMode    string // PRAGMA journal_mode
	Synchronous    int    // PRAGMA synchronous (0=OFF, 1=NORMAL, 2=FULL, 3=EXTRA)
	WALCheckpoint  int    // PRAGMA wal_checkpoint

	// Concurrency settings
	BusyTimeout    int    // PRAGMA busy_timeout (milliseconds)

	// Integrity and optimization
	ForeignKeys    bool   // PRAGMA foreign_keys
	CheckpointFullfsync bool // PRAGMA checkpoint_fullfsync

	// Security
	SecureDelete   bool   // PRAGMA secure_delete

	// Custom settings
	CustomPragmas  map[string]string
}

// DefaultSQLitePragmaConfig returns optimized defaults for SQLite
func DefaultSQLitePragmaConfig() *SQLitePragmaConfig {
	return &SQLitePragmaConfig{
		CacheSize:           -64000, // 64MB cache (negative means KB)
		TempStore:           2,       // Store temp tables in memory
		MmapSize:            268435456, // 256MB memory-mapped I/O
		PageSize:            4096,    // 4KB page size
		JournalMode:         "WAL",   // Write-Ahead Logging for better concurrency
		Synchronous:         1,       // NORMAL synchronous mode (good balance)
		WALCheckpoint:       1000,    // Checkpoint every 1000 WAL frames
		BusyTimeout:         30000,   // 30 second busy timeout
		ForeignKeys:         true,    // Enable foreign key constraints
		CheckpointFullfsync: false,   // Don't force full fsync on checkpoint
		SecureDelete:        false,   // Don't overwrite deleted data (performance)
		CustomPragmas:       make(map[string]string),
	}
}

// OptimizeForWrite returns configuration optimized for write-heavy workloads
func OptimizeForWrite() *SQLitePragmaConfig {
	config := DefaultSQLitePragmaConfig()
	config.JournalMode = "WAL"
	config.Synchronous = 1 // NORMAL
	config.TempStore = 2   // Memory
	config.CacheSize = -128000 // 128MB cache
	config.BusyTimeout = 60000 // 60 seconds
	return config
}

// OptimizeForRead returns configuration optimized for read-heavy workloads
func OptimizeForRead() *SQLitePragmaConfig {
	config := DefaultSQLitePragmaConfig()
	config.JournalMode = "WAL"
	config.Synchronous = 0 // OFF (risky but faster reads)
	config.TempStore = 2   // Memory
	config.CacheSize = -256000 // 256MB cache
	config.MmapSize = 1073741824 // 1GB mmap
	return config
}

// OptimizeForSafety returns configuration optimized for maximum data safety
func OptimizeForSafety() *SQLitePragmaConfig {
	config := DefaultSQLitePragmaConfig()
	config.JournalMode = "DELETE" // Traditional rollback journal
	config.Synchronous = 3        // EXTRA
	config.SecureDelete = true    // Overwrite deleted data
	config.CheckpointFullfsync = true // Force full fsync
	return config
}

// ApplyPragmas applies pragma configuration to the SQLite connection
func (d *SQLiteDialect) ApplyPragmas(ctx context.Context, config *SQLitePragmaConfig) error {
	if config == nil {
		config = d.config
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	pragmas := []struct {
		name  string
		value string
	}{
		{"cache_size", strconv.Itoa(config.CacheSize)},
		{"temp_store", strconv.Itoa(config.TempStore)},
		{"mmap_size", strconv.FormatInt(config.MmapSize, 10)},
		{"page_size", strconv.Itoa(config.PageSize)},
		{"journal_mode", config.JournalMode},
		{"synchronous", strconv.Itoa(config.Synchronous)},
		{"busy_timeout", strconv.Itoa(config.BusyTimeout)},
		{"foreign_keys", boolToString(config.ForeignKeys)},
		{"checkpoint_fullfsync", boolToString(config.CheckpointFullfsync)},
		{"secure_delete", boolToString(config.SecureDelete)},
	}

	// Apply standard pragmas
	for _, pragma := range pragmas {
		query := fmt.Sprintf("PRAGMA %s = %s", pragma.name, pragma.value)
		if _, err := d.conn.Exec(ctx, query); err != nil {
			return fmt.Errorf("failed to apply PRAGMA %s: %w", pragma.name, err)
		}
	}

	// Apply custom pragmas
	for name, value := range config.CustomPragmas {
		query := fmt.Sprintf("PRAGMA %s = %s", name, value)
		if _, err := d.conn.Exec(ctx, query); err != nil {
			return fmt.Errorf("failed to apply custom PRAGMA %s: %w", name, err)
		}
	}

	d.config = config
	return nil
}

// GetPragmaValue retrieves the current value of a pragma
func (d *SQLiteDialect) GetPragmaValue(ctx context.Context, pragma string) (string, error) {
	query := fmt.Sprintf("PRAGMA %s", pragma)

	var value string
	err := d.conn.Get(ctx, &value, query)
	if err != nil {
		return "", fmt.Errorf("failed to get PRAGMA %s: %w", pragma, err)
	}

	return value, nil
}

// UpsertQuery builds an optimized UPSERT query using INSERT OR REPLACE/ON CONFLICT
func (d *SQLiteDialect) UpsertQuery(table string, columns []string, conflictColumns []string, onConflictAction string) string {
	if onConflictAction == "" {
		onConflictAction = "REPLACE"
	}

	var query strings.Builder

	query.WriteString("INSERT INTO ")
	query.WriteString(table)
	query.WriteString(" (")
	query.WriteString(strings.Join(columns, ", "))
	query.WriteString(") VALUES (")
	query.WriteString(strings.Repeat("?, ", len(columns)-1))
	query.WriteString("?)")

	// Use ON CONFLICT clause if conflict columns specified
	if len(conflictColumns) > 0 {
		query.WriteString(" ON CONFLICT (")
		query.WriteString(strings.Join(conflictColumns, ", "))
		query.WriteString(") DO ")

		switch strings.ToUpper(onConflictAction) {
		case "IGNORE":
			query.WriteString("NOTHING")
		case "REPLACE", "UPDATE":
			query.WriteString("UPDATE SET ")
			updateClauses := make([]string, 0)
			for _, col := range columns {
				// Skip conflict columns
				isConflictColumn := false
				for _, conflictCol := range conflictColumns {
					if col == conflictCol {
						isConflictColumn = true
						break
					}
				}

				if !isConflictColumn {
					updateClauses = append(updateClauses, fmt.Sprintf("%s = excluded.%s", col, col))
				}
			}
			query.WriteString(strings.Join(updateClauses, ", "))
		}
	}

	return query.String()
}

// EnableWAL enables Write-Ahead Logging mode for better concurrency
func (d *SQLiteDialect) EnableWAL(ctx context.Context) error {
	_, err := d.conn.Exec(ctx, "PRAGMA journal_mode = WAL")
	if err != nil {
		return fmt.Errorf("failed to enable WAL mode: %w", err)
	}

	// Set reasonable WAL checkpoint interval
	_, err = d.conn.Exec(ctx, "PRAGMA wal_checkpoint(TRUNCATE)")
	return err
}

// CheckpointWAL performs a WAL checkpoint to ensure data is written to main database
func (d *SQLiteDialect) CheckpointWAL(ctx context.Context, mode string) (*WALCheckpointResult, error) {
	if mode == "" {
		mode = "PASSIVE"
	}

	query := fmt.Sprintf("PRAGMA wal_checkpoint(%s)", mode)

	var result WALCheckpointResult
	row := d.conn.QueryRow(ctx, query)
	err := row.Scan(&result.BusyErrors, &result.LogFrames, &result.CheckpointedFrames)
	if err != nil {
		return nil, fmt.Errorf("failed to checkpoint WAL: %w", err)
	}

	return &result, nil
}

// WALCheckpointResult contains results from a WAL checkpoint operation
type WALCheckpointResult struct {
	BusyErrors          int `json:"busy_errors"`
	LogFrames           int `json:"log_frames"`
	CheckpointedFrames  int `json:"checkpointed_frames"`
}

// Vacuum performs database vacuum operation to reclaim space and optimize
func (d *SQLiteDialect) Vacuum(ctx context.Context) error {
	_, err := d.conn.Exec(ctx, "VACUUM")
	if err != nil {
		return fmt.Errorf("failed to vacuum database: %w", err)
	}
	return nil
}

// Analyze performs database analysis to update query optimizer statistics
func (d *SQLiteDialect) Analyze(ctx context.Context, table string) error {
	var query string
	if table == "" {
		query = "ANALYZE"
	} else {
		query = fmt.Sprintf("ANALYZE %s", table)
	}

	_, err := d.conn.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to analyze database/table: %w", err)
	}
	return nil
}

// GetDatabaseInfo retrieves comprehensive database information
func (d *SQLiteDialect) GetDatabaseInfo(ctx context.Context) (*SQLiteDBInfo, error) {
	info := &SQLiteDBInfo{}

	// Get database list
	databases, err := d.getDatabaseList(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get database list: %w", err)
	}
	info.Databases = databases

	// Get schema version
	version, err := d.GetPragmaValue(ctx, "schema_version")
	if err != nil {
		return nil, fmt.Errorf("failed to get schema version: %w", err)
	}
	info.SchemaVersion = version

	// Get page count and size
	pageCount, err := d.GetPragmaValue(ctx, "page_count")
	if err != nil {
		return nil, fmt.Errorf("failed to get page count: %w", err)
	}
	info.PageCount = pageCount

	pageSize, err := d.GetPragmaValue(ctx, "page_size")
	if err != nil {
		return nil, fmt.Errorf("failed to get page size: %w", err)
	}
	info.PageSize = pageSize

	// Get journal mode
	journalMode, err := d.GetPragmaValue(ctx, "journal_mode")
	if err != nil {
		return nil, fmt.Errorf("failed to get journal mode: %w", err)
	}
	info.JournalMode = journalMode

	// Get foreign keys status
	foreignKeys, err := d.GetPragmaValue(ctx, "foreign_keys")
	if err != nil {
		return nil, fmt.Errorf("failed to get foreign keys status: %w", err)
	}
	info.ForeignKeys = foreignKeys

	return info, nil
}

// SQLiteDBInfo contains comprehensive SQLite database information
type SQLiteDBInfo struct {
	Databases     []SQLiteDatabase `json:"databases"`
	SchemaVersion string          `json:"schema_version"`
	PageCount     string          `json:"page_count"`
	PageSize      string          `json:"page_size"`
	JournalMode   string          `json:"journal_mode"`
	ForeignKeys   string          `json:"foreign_keys"`
}

// SQLiteDatabase represents information about a SQLite database
type SQLiteDatabase struct {
	Seq  int    `db:"seq"  json:"seq"`
	Name string `db:"name" json:"name"`
	File string `db:"file" json:"file"`
}

// getDatabaseList retrieves the list of attached databases
func (d *SQLiteDialect) getDatabaseList(ctx context.Context) ([]SQLiteDatabase, error) {
	var databases []SQLiteDatabase

	err := d.conn.Select(ctx, &databases, "PRAGMA database_list")
	if err != nil {
		return nil, err
	}

	return databases, nil
}

// GetTableInfo retrieves detailed information about a table
func (d *SQLiteDialect) GetTableInfo(ctx context.Context, tableName string) (*SQLiteTableInfo, error) {
	info := &SQLiteTableInfo{
		Name: tableName,
	}

	// Get table schema
	columns, err := d.getTableColumns(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get table columns: %w", err)
	}
	info.Columns = columns

	// Get indexes
	indexes, err := d.getTableIndexes(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get table indexes: %w", err)
	}
	info.Indexes = indexes

	// Get foreign keys
	foreignKeys, err := d.getTableForeignKeys(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get table foreign keys: %w", err)
	}
	info.ForeignKeys = foreignKeys

	return info, nil
}

// SQLiteTableInfo contains comprehensive table information
type SQLiteTableInfo struct {
	Name        string                `json:"name"`
	Columns     []SQLiteColumnInfo    `json:"columns"`
	Indexes     []SQLiteIndexInfo     `json:"indexes"`
	ForeignKeys []SQLiteForeignKey    `json:"foreign_keys"`
}

// SQLiteColumnInfo contains column information
type SQLiteColumnInfo struct {
	CID          int    `db:"cid" json:"cid"`
	Name         string `db:"name" json:"name"`
	Type         string `db:"type" json:"type"`
	NotNull      bool   `db:"notnull" json:"not_null"`
	DefaultValue string `db:"dflt_value" json:"default_value"`
	PrimaryKey   bool   `db:"pk" json:"primary_key"`
}

// SQLiteIndexInfo contains index information
type SQLiteIndexInfo struct {
	Seq    int    `db:"seq" json:"seq"`
	Name   string `db:"name" json:"name"`
	Unique bool   `db:"unique" json:"unique"`
	Origin string `db:"origin" json:"origin"`
	Partial bool  `db:"partial" json:"partial"`
}

// SQLiteForeignKey contains foreign key information
type SQLiteForeignKey struct {
	ID    int    `db:"id" json:"id"`
	Seq   int    `db:"seq" json:"seq"`
	Table string `db:"table" json:"table"`
	From  string `db:"from" json:"from"`
	To    string `db:"to" json:"to"`
	On    string `db:"on_update" json:"on_update"`
	Match string `db:"match" json:"match"`
}

// getTableColumns retrieves column information for a table
func (d *SQLiteDialect) getTableColumns(ctx context.Context, tableName string) ([]SQLiteColumnInfo, error) {
	var columns []SQLiteColumnInfo

	query := fmt.Sprintf("PRAGMA table_info(%s)", tableName)
	err := d.conn.Select(ctx, &columns, query)
	if err != nil {
		return nil, err
	}

	return columns, nil
}

// getTableIndexes retrieves index information for a table
func (d *SQLiteDialect) getTableIndexes(ctx context.Context, tableName string) ([]SQLiteIndexInfo, error) {
	var indexes []SQLiteIndexInfo

	query := fmt.Sprintf("PRAGMA index_list(%s)", tableName)
	err := d.conn.Select(ctx, &indexes, query)
	if err != nil {
		return nil, err
	}

	return indexes, nil
}

// getTableForeignKeys retrieves foreign key information for a table
func (d *SQLiteDialect) getTableForeignKeys(ctx context.Context, tableName string) ([]SQLiteForeignKey, error) {
	var foreignKeys []SQLiteForeignKey

	query := fmt.Sprintf("PRAGMA foreign_key_list(%s)", tableName)
	err := d.conn.Select(ctx, &foreignKeys, query)
	if err != nil {
		return nil, err
	}

	return foreignKeys, nil
}

// SQLiteConcurrencyManager handles SQLite-specific concurrency patterns
type SQLiteConcurrencyManager struct {
	conn    *SQLiteConnection
	mu      sync.RWMutex
	readCh  chan struct{}
	writeCh chan struct{}
}

// NewSQLiteConcurrencyManager creates a concurrency manager for SQLite
func NewSQLiteConcurrencyManager(conn *SQLiteConnection, maxReaders int) *SQLiteConcurrencyManager {
	return &SQLiteConcurrencyManager{
		conn:    conn,
		readCh:  make(chan struct{}, maxReaders),
		writeCh: make(chan struct{}, 1), // SQLite allows only one writer
	}
}

// AcquireReadLock acquires a read lock for SQLite operations
func (cm *SQLiteConcurrencyManager) AcquireReadLock(ctx context.Context) error {
	select {
	case cm.readCh <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ReleaseReadLock releases a read lock
func (cm *SQLiteConcurrencyManager) ReleaseReadLock() {
	select {
	case <-cm.readCh:
	default:
		log.Printf("Warning: attempted to release read lock when none was held")
	}
}

// AcquireWriteLock acquires an exclusive write lock for SQLite operations
func (cm *SQLiteConcurrencyManager) AcquireWriteLock(ctx context.Context) error {
	select {
	case cm.writeCh <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ReleaseWriteLock releases the exclusive write lock
func (cm *SQLiteConcurrencyManager) ReleaseWriteLock() {
	select {
	case <-cm.writeCh:
	default:
		log.Printf("Warning: attempted to release write lock when none was held")
	}
}

// Utility functions

// boolToString converts boolean to SQLite-compatible string
func boolToString(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

// SQLiteHealthChecker provides SQLite-specific health checking
type SQLiteHealthChecker struct {
	conn *SQLiteConnection
}

// NewSQLiteHealthChecker creates a SQLite health checker
func NewSQLiteHealthChecker(conn *SQLiteConnection) *SQLiteHealthChecker {
	return &SQLiteHealthChecker{conn: conn}
}

// CheckHealth performs comprehensive SQLite health check
func (h *SQLiteHealthChecker) CheckHealth(ctx context.Context) (*SQLiteHealthStatus, error) {
	status := &SQLiteHealthStatus{
		Timestamp: time.Now(),
	}

	// Basic connectivity check
	if err := h.conn.Ping(ctx); err != nil {
		status.Connected = false
		status.Error = err.Error()
		return status, err
	}
	status.Connected = true

	// Get database info
	dialect := NewSQLiteDialect(h.conn)
	dbInfo, err := dialect.GetDatabaseInfo(ctx)
	if err != nil {
		status.Error = fmt.Sprintf("failed to get database info: %v", err)
		return status, err
	}
	status.DatabaseInfo = dbInfo

	// Check integrity
	integrityResult, err := h.checkIntegrity(ctx)
	if err != nil {
		status.IntegrityError = err.Error()
	} else {
		status.IntegrityOK = integrityResult == "ok"
	}

	// Get connection stats
	status.ConnectionStats = h.conn.Stats()

	return status, nil
}

// SQLiteHealthStatus represents SQLite health check results
type SQLiteHealthStatus struct {
	Timestamp       time.Time         `json:"timestamp"`
	Connected       bool              `json:"connected"`
	Error           string            `json:"error,omitempty"`
	DatabaseInfo    *SQLiteDBInfo     `json:"database_info,omitempty"`
	IntegrityOK     bool              `json:"integrity_ok"`
	IntegrityError  string            `json:"integrity_error,omitempty"`
	ConnectionStats ConnectionStats   `json:"connection_stats"`
}

// checkIntegrity performs PRAGMA integrity_check
func (h *SQLiteHealthChecker) checkIntegrity(ctx context.Context) (string, error) {
	var result string
	err := h.conn.Get(ctx, &result, "PRAGMA integrity_check")
	if err != nil {
		return "", err
	}
	return result, nil
}