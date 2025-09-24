// Package db provides database connection management with sqlx integration
// for MySQL, SQLite, and SQL Server, while maintaining pgx for PostgreSQL
package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jmoiron/sqlx"
)

// ConnectionType represents the type of database connection
type ConnectionType string

const (
	PostgreSQLConnectionType ConnectionType = "postgresql"
	MySQLConnectionType      ConnectionType = "mysql"
	SQLiteConnectionType     ConnectionType = "sqlite"
	SQLServerConnectionType  ConnectionType = "sqlserver"
)

// ConnectionRole represents the role of a database connection
type ConnectionRole string

const (
	PrimaryRole ConnectionRole = "primary"
	ReplicaRole ConnectionRole = "replica"
)

// ConnectionConfig holds database connection configuration
type ConnectionConfig struct {
	Type         ConnectionType
	Role         ConnectionRole
	DSN          string
	MaxOpenConns int
	MaxIdleConns int
	ConnMaxLife  time.Duration
	ConnMaxIdle  time.Duration
	HealthCheck  bool
	RetryAttempts int
	RetryInterval time.Duration
}

// Connection interface abstracts database connections
type Connection interface {
	// Basic operations
	Ping(ctx context.Context) error
	Close() error
	Stats() ConnectionStats

	// Query operations
	Query(ctx context.Context, query string, args ...interface{}) (Rows, error)
	QueryRow(ctx context.Context, query string, args ...interface{}) Row
	Exec(ctx context.Context, query string, args ...interface{}) (Result, error)

	// sqlx-specific operations (for non-PostgreSQL)
	Select(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	Get(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	NamedExec(ctx context.Context, query string, arg interface{}) (Result, error)
	NamedQuery(ctx context.Context, query string, arg interface{}) (Rows, error)

	// Transaction support
	Begin(ctx context.Context) (Transaction, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (Transaction, error)

	// Connection metadata
	Type() ConnectionType
	Role() ConnectionRole
	IsHealthy() bool
}

// Transaction interface abstracts database transactions
type Transaction interface {
	Query(ctx context.Context, query string, args ...interface{}) (Rows, error)
	QueryRow(ctx context.Context, query string, args ...interface{}) Row
	Exec(ctx context.Context, query string, args ...interface{}) (Result, error)

	// sqlx-specific transaction operations
	Select(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	Get(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	NamedExec(ctx context.Context, query string, arg interface{}) (Result, error)
	NamedQuery(ctx context.Context, query string, arg interface{}) (Rows, error)

	Commit() error
	Rollback() error
}

// Rows interface abstracts query result rows
type Rows interface {
	Next() bool
	Scan(dest ...interface{}) error
	Close() error
	Err() error
	Columns() ([]string, error)
	ColumnTypes() ([]ColumnType, error)

	// sqlx-specific methods
	StructScan(dest interface{}) error
	MapScan(dest map[string]interface{}) error
	SliceScan() ([]interface{}, error)
}

// Row interface abstracts single row results
type Row interface {
	Scan(dest ...interface{}) error

	// sqlx-specific methods
	StructScan(dest interface{}) error
	MapScan(dest map[string]interface{}) error
}

// Result interface abstracts command execution results
type Result interface {
	LastInsertId() (int64, error)
	RowsAffected() (int64, error)
}

// ColumnType interface abstracts column metadata
type ColumnType interface {
	Name() string
	DatabaseTypeName() string
	ScanType() interface{}
	Nullable() (nullable, ok bool)
	Length() (length int64, ok bool)
	DecimalSize() (precision, scale int64, ok bool)
}

// ConnectionStats represents connection pool statistics
type ConnectionStats struct {
	MaxOpenConnections int
	OpenConnections    int
	InUse              int
	Idle               int
	WaitCount          int64
	WaitDuration       time.Duration
	MaxIdleClosed      int64
	MaxIdleTimeClosed  int64
	MaxLifetimeClosed  int64
}

// ConnectionManager manages multiple database connections with routing
type ConnectionManager struct {
	connections map[string]Connection
	config      *ManagerConfig
	router      *ConnectionRouter
}

// ManagerConfig holds connection manager configuration
type ManagerConfig struct {
	EnableReadWriteSplitting bool
	HealthCheckInterval      time.Duration
	ConnectionTimeout        time.Duration
	QueryTimeout             time.Duration
	EnableConnectionRetry    bool
	MaxRetryAttempts         int
}

// NewConnectionManager creates a new connection manager
func NewConnectionManager(config *ManagerConfig) *ConnectionManager {
	if config == nil {
		config = &ManagerConfig{
			EnableReadWriteSplitting: false,
			HealthCheckInterval:      30 * time.Second,
			ConnectionTimeout:        10 * time.Second,
			QueryTimeout:             30 * time.Second,
			EnableConnectionRetry:    true,
			MaxRetryAttempts:         3,
		}
	}

	return &ConnectionManager{
		connections: make(map[string]Connection),
		config:      config,
		router:      NewConnectionRouter(),
	}
}

// AddConnection adds a new database connection
func (cm *ConnectionManager) AddConnection(name string, conn Connection) error {
	if conn == nil {
		return fmt.Errorf("connection cannot be nil")
	}

	cm.connections[name] = conn

	// Register connection with router
	if err := cm.router.RegisterConnection(name, conn); err != nil {
		return fmt.Errorf("failed to register connection: %w", err)
	}

	return nil
}

// GetConnection retrieves a connection by name
func (cm *ConnectionManager) GetConnection(name string) (Connection, error) {
	conn, exists := cm.connections[name]
	if !exists {
		return nil, fmt.Errorf("connection '%s' not found", name)
	}
	return conn, nil
}

// GetConnectionForQuery routes queries to appropriate connections
func (cm *ConnectionManager) GetConnectionForQuery(queryType QueryType) (Connection, error) {
	return cm.router.RouteQuery(queryType)
}

// Close closes all managed connections
func (cm *ConnectionManager) Close() error {
	var lastErr error
	for name, conn := range cm.connections {
		if err := conn.Close(); err != nil {
			lastErr = fmt.Errorf("failed to close connection '%s': %w", name, err)
		}
	}
	return lastErr
}

// HealthCheck performs health checks on all connections
func (cm *ConnectionManager) HealthCheck(ctx context.Context) map[string]error {
	results := make(map[string]error)

	for name, conn := range cm.connections {
		if err := conn.Ping(ctx); err != nil {
			results[name] = err
		}
	}

	return results
}

// ConnectionRouter handles query routing for read/write splitting
type ConnectionRouter struct {
	primaryConns map[ConnectionType][]Connection
	replicaConns map[ConnectionType][]Connection
	roundRobin   map[ConnectionType]int
}

// QueryType represents the type of database query
type QueryType string

const (
	ReadQuery  QueryType = "read"
	WriteQuery QueryType = "write"
)

// NewConnectionRouter creates a new connection router
func NewConnectionRouter() *ConnectionRouter {
	return &ConnectionRouter{
		primaryConns: make(map[ConnectionType][]Connection),
		replicaConns: make(map[ConnectionType][]Connection),
		roundRobin:   make(map[ConnectionType]int),
	}
}

// RegisterConnection registers a connection with the router
func (cr *ConnectionRouter) RegisterConnection(name string, conn Connection) error {
	connType := conn.Type()
	role := conn.Role()

	switch role {
	case PrimaryRole:
		cr.primaryConns[connType] = append(cr.primaryConns[connType], conn)
	case ReplicaRole:
		cr.replicaConns[connType] = append(cr.replicaConns[connType], conn)
	default:
		return fmt.Errorf("unsupported connection role: %s", role)
	}

	return nil
}

// RouteQuery routes a query to the appropriate connection
func (cr *ConnectionRouter) RouteQuery(queryType QueryType) (Connection, error) {
	// For write queries, always use primary
	if queryType == WriteQuery {
		return cr.getHealthyConnection(cr.primaryConns)
	}

	// For read queries, prefer replica but fallback to primary
	if conn, err := cr.getHealthyConnection(cr.replicaConns); err == nil {
		return conn, nil
	}

	return cr.getHealthyConnection(cr.primaryConns)
}

// getHealthyConnection returns a healthy connection using round-robin
func (cr *ConnectionRouter) getHealthyConnection(connMap map[ConnectionType][]Connection) (Connection, error) {
	for connType, conns := range connMap {
		if len(conns) == 0 {
			continue
		}

		// Round-robin selection
		index := cr.roundRobin[connType] % len(conns)
		cr.roundRobin[connType] = (cr.roundRobin[connType] + 1) % len(conns)

		conn := conns[index]
		if conn.IsHealthy() {
			return conn, nil
		}
	}

	return nil, fmt.Errorf("no healthy connections available")
}

// DatabaseBuilder provides a fluent interface for creating database connections
type DatabaseBuilder struct {
	config ConnectionConfig
}

// NewDatabaseBuilder creates a new database builder
func NewDatabaseBuilder(connType ConnectionType) *DatabaseBuilder {
	return &DatabaseBuilder{
		config: ConnectionConfig{
			Type:          connType,
			Role:          PrimaryRole,
			MaxOpenConns:  25,
			MaxIdleConns:  5,
			ConnMaxLife:   5 * time.Minute,
			ConnMaxIdle:   5 * time.Minute,
			HealthCheck:   true,
			RetryAttempts: 3,
			RetryInterval: 1 * time.Second,
		},
	}
}

// WithDSN sets the data source name
func (db *DatabaseBuilder) WithDSN(dsn string) *DatabaseBuilder {
	db.config.DSN = dsn
	return db
}

// WithRole sets the connection role
func (db *DatabaseBuilder) WithRole(role ConnectionRole) *DatabaseBuilder {
	db.config.Role = role
	return db
}

// WithMaxOpenConns sets the maximum number of open connections
func (db *DatabaseBuilder) WithMaxOpenConns(max int) *DatabaseBuilder {
	db.config.MaxOpenConns = max
	return db
}

// WithMaxIdleConns sets the maximum number of idle connections
func (db *DatabaseBuilder) WithMaxIdleConns(max int) *DatabaseBuilder {
	db.config.MaxIdleConns = max
	return db
}

// WithConnMaxLifetime sets the maximum connection lifetime
func (db *DatabaseBuilder) WithConnMaxLifetime(duration time.Duration) *DatabaseBuilder {
	db.config.ConnMaxLife = duration
	return db
}

// WithHealthCheck enables or disables health checks
func (db *DatabaseBuilder) WithHealthCheck(enabled bool) *DatabaseBuilder {
	db.config.HealthCheck = enabled
	return db
}

// Build creates the database connection based on the configuration
func (db *DatabaseBuilder) Build(ctx context.Context) (Connection, error) {
	switch db.config.Type {
	case PostgreSQLConnectionType:
		return db.buildPostgreSQLConnection(ctx)
	case MySQLConnectionType:
		return db.buildMySQLConnection(ctx)
	case SQLiteConnectionType:
		return db.buildSQLiteConnection(ctx)
	case SQLServerConnectionType:
		return db.buildSQLServerConnection(ctx)
	default:
		return nil, fmt.Errorf("unsupported database type: %s", db.config.Type)
	}
}

// buildPostgreSQLConnection creates a PostgreSQL connection using pgx
func (db *DatabaseBuilder) buildPostgreSQLConnection(ctx context.Context) (Connection, error) {
	config, err := pgxpool.ParseConfig(db.config.DSN)
	if err != nil {
		return nil, fmt.Errorf("failed to parse PostgreSQL config: %w", err)
	}

	config.MaxConns = int32(db.config.MaxOpenConns)
	config.MinConns = int32(db.config.MaxIdleConns)
	config.MaxConnLifetime = db.config.ConnMaxLife
	config.MaxConnIdleTime = db.config.ConnMaxIdle

	pool, err := pgxpool.New(ctx, config.ConnString())
	if err != nil {
		return nil, fmt.Errorf("failed to create PostgreSQL connection pool: %w", err)
	}

	return NewPostgreSQLConnection(pool, db.config), nil
}

// buildMySQLConnection creates a MySQL connection using sqlx
func (db *DatabaseBuilder) buildMySQLConnection(ctx context.Context) (Connection, error) {
	sqlxDB, err := sqlx.ConnectContext(ctx, "mysql", db.config.DSN)
	if err != nil {
		return nil, fmt.Errorf("failed to create MySQL connection: %w", err)
	}

	sqlxDB.SetMaxOpenConns(db.config.MaxOpenConns)
	sqlxDB.SetMaxIdleConns(db.config.MaxIdleConns)
	sqlxDB.SetConnMaxLifetime(db.config.ConnMaxLife)
	sqlxDB.SetConnMaxIdleTime(db.config.ConnMaxIdle)

	return NewMySQLConnection(sqlxDB, db.config), nil
}

// buildSQLiteConnection creates a SQLite connection using sqlx
func (db *DatabaseBuilder) buildSQLiteConnection(ctx context.Context) (Connection, error) {
	sqlxDB, err := sqlx.ConnectContext(ctx, "sqlite3", db.config.DSN)
	if err != nil {
		return nil, fmt.Errorf("failed to create SQLite connection: %w", err)
	}

	// SQLite-specific configuration
	sqlxDB.SetMaxOpenConns(1) // SQLite doesn't support multiple concurrent writes
	sqlxDB.SetMaxIdleConns(1)
	sqlxDB.SetConnMaxLifetime(db.config.ConnMaxLife)

	return NewSQLiteConnection(sqlxDB, db.config), nil
}

// buildSQLServerConnection creates a SQL Server connection using sqlx
func (db *DatabaseBuilder) buildSQLServerConnection(ctx context.Context) (Connection, error) {
	sqlxDB, err := sqlx.ConnectContext(ctx, "sqlserver", db.config.DSN)
	if err != nil {
		return nil, fmt.Errorf("failed to create SQL Server connection: %w", err)
	}

	sqlxDB.SetMaxOpenConns(db.config.MaxOpenConns)
	sqlxDB.SetMaxIdleConns(db.config.MaxIdleConns)
	sqlxDB.SetConnMaxLifetime(db.config.ConnMaxLife)
	sqlxDB.SetConnMaxIdleTime(db.config.ConnMaxIdle)

	return NewSQLServerConnection(sqlxDB, db.config), nil
}