package performance

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/fathiraz/gorp/db"
)

// ConnectionPoolManager provides optimized connection pooling with monitoring
type ConnectionPoolManager struct {
	pools       map[db.ConnectionType]*ConnectionPool
	mu          sync.RWMutex
	tracer      trace.Tracer
	healthCheck *HealthChecker
}

// ConnectionPool represents an optimized connection pool for a specific database type
type ConnectionPool struct {
	connType          db.ConnectionType
	config            *ConnectionPoolConfig
	connections       chan *PooledConnection
	activeConnections map[*PooledConnection]bool
	mu                sync.RWMutex
	created           int64
	closed            int64
	borrowed          int64
	returned          int64
	errors            int64
	tracer            trace.Tracer
	healthCheck       *HealthChecker
	stopHealthCheck   chan struct{}
}

// PooledConnection wraps a database connection with pool metadata
type PooledConnection struct {
	conn        *sql.DB
	connType    db.ConnectionType
	createdAt   time.Time
	lastUsedAt  time.Time
	borrowCount int64
	isHealthy   bool
	mu          sync.RWMutex
}

// ConnectionPoolConfig holds configuration for connection pools
type ConnectionPoolConfig struct {
	MinConnections      int           // Minimum connections to maintain
	MaxConnections      int           // Maximum connections allowed
	MaxIdleTime         time.Duration // Maximum idle time before connection is closed
	MaxLifetime         time.Duration // Maximum lifetime of a connection
	AcquisitionTimeout  time.Duration // Maximum time to wait for connection
	HealthCheckInterval time.Duration // How often to run health checks
	EnableTracing       bool          // Enable OpenTelemetry tracing
	TracerName          string        // Name for the tracer
	ValidationQuery     string        // Query to validate connections
}

// DefaultConnectionPoolConfig returns sensible defaults
func DefaultConnectionPoolConfig() *ConnectionPoolConfig {
	return &ConnectionPoolConfig{
		MinConnections:      2,
		MaxConnections:      10,
		MaxIdleTime:         30 * time.Minute,
		MaxLifetime:         1 * time.Hour,
		AcquisitionTimeout:  30 * time.Second,
		HealthCheckInterval: 5 * time.Minute,
		EnableTracing:       true,
		TracerName:          "gorp.connection_pool",
		ValidationQuery:     "SELECT 1",
	}
}

// NewConnectionPoolManager creates a new connection pool manager
func NewConnectionPoolManager(config *ConnectionPoolConfig) *ConnectionPoolManager {
	if config == nil {
		config = DefaultConnectionPoolConfig()
	}

	var tracer trace.Tracer
	if config.EnableTracing {
		tracer = otel.Tracer(config.TracerName)
	}

	healthChecker := NewHealthChecker(config.ValidationQuery, config.HealthCheckInterval)

	return &ConnectionPoolManager{
		pools:       make(map[db.ConnectionType]*ConnectionPool),
		tracer:      tracer,
		healthCheck: healthChecker,
	}
}

// GetPool returns the connection pool for the specified database type
func (cpm *ConnectionPoolManager) GetPool(connType db.ConnectionType, config *ConnectionPoolConfig, connectionFactory func() (*sql.DB, error)) (*ConnectionPool, error) {
	cpm.mu.RLock()
	if pool, exists := cpm.pools[connType]; exists {
		cpm.mu.RUnlock()
		return pool, nil
	}
	cpm.mu.RUnlock()

	cpm.mu.Lock()
	defer cpm.mu.Unlock()

	// Double-check pattern
	if pool, exists := cpm.pools[connType]; exists {
		return pool, nil
	}

	// Create new pool
	pool, err := NewConnectionPool(connType, config, connectionFactory)
	if err != nil {
		return nil, err
	}

	pool.tracer = cpm.tracer
	pool.healthCheck = cpm.healthCheck
	cpm.pools[connType] = pool

	return pool, nil
}

// NewConnectionPool creates a new connection pool
func NewConnectionPool(connType db.ConnectionType, config *ConnectionPoolConfig, connectionFactory func() (*sql.DB, error)) (*ConnectionPool, error) {
	if config == nil {
		config = DefaultConnectionPoolConfig()
	}

	pool := &ConnectionPool{
		connType:          connType,
		config:            config,
		connections:       make(chan *PooledConnection, config.MaxConnections),
		activeConnections: make(map[*PooledConnection]bool),
		stopHealthCheck:   make(chan struct{}),
	}

	// Create minimum connections
	for i := 0; i < config.MinConnections; i++ {
		conn, err := connectionFactory()
		if err != nil {
			// Close any connections we've already created
			pool.Close()
			return nil, fmt.Errorf("failed to create initial connection %d: %v", i, err)
		}

		pooledConn := &PooledConnection{
			conn:       conn,
			connType:   connType,
			createdAt:  time.Now(),
			lastUsedAt: time.Now(),
			isHealthy:  true,
		}

		pool.connections <- pooledConn
		pool.created++
	}

	// Start health check worker
	go pool.healthCheckWorker()

	return pool, nil
}

// AcquireConnection gets a connection from the pool
func (cp *ConnectionPool) AcquireConnection(ctx context.Context) (*PooledConnection, error) {
	// Create span for connection acquisition
	var span trace.Span
	if cp.tracer != nil {
		ctx, span = cp.tracer.Start(ctx, "connection_pool.acquire",
			trace.WithAttributes(
				attribute.String("db.connection_type", string(cp.connType)),
			))
		defer span.End()
	}

	start := time.Now()

	// Try to acquire with timeout
	timeout := cp.config.AcquisitionTimeout
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining < timeout {
			timeout = remaining
		}
	}

	select {
	case conn := <-cp.connections:
		// Update connection stats
		conn.mu.Lock()
		conn.lastUsedAt = time.Now()
		conn.borrowCount++
		conn.mu.Unlock()

		// Track active connection
		cp.mu.Lock()
		cp.activeConnections[conn] = true
		cp.borrowed++
		cp.mu.Unlock()

		if span != nil {
			span.SetAttributes(
				attribute.Int64("connection.borrow_count", conn.borrowCount),
				attribute.Int64("connection.acquisition_ms", time.Since(start).Milliseconds()),
				attribute.Bool("connection.from_pool", true),
			)
			span.SetStatus(codes.Ok, "Connection acquired from pool")
		}

		return conn, nil

	case <-time.After(timeout):
		cp.mu.Lock()
		cp.errors++
		cp.mu.Unlock()

		if span != nil {
			span.SetStatus(codes.Error, "Connection acquisition timeout")
		}

		return nil, fmt.Errorf("connection acquisition timeout after %v", timeout)

	case <-ctx.Done():
		cp.mu.Lock()
		cp.errors++
		cp.mu.Unlock()

		if span != nil {
			span.SetStatus(codes.Error, "Context cancelled")
		}

		return nil, ctx.Err()
	}
}

// ReleaseConnection returns a connection to the pool
func (cp *ConnectionPool) ReleaseConnection(conn *PooledConnection) error {
	if conn == nil {
		return fmt.Errorf("cannot release nil connection")
	}

	cp.mu.Lock()
	defer cp.mu.Unlock()

	// Remove from active connections
	delete(cp.activeConnections, conn)
	cp.returned++

	// Check if connection should be discarded
	now := time.Now()
	if cp.shouldDiscardConnection(conn, now) {
		conn.conn.Close()
		cp.closed++
		return nil
	}

	// Return to pool
	select {
	case cp.connections <- conn:
		return nil
	default:
		// Pool is full, close the connection
		conn.conn.Close()
		cp.closed++
		return nil
	}
}

// shouldDiscardConnection determines if a connection should be discarded
func (cp *ConnectionPool) shouldDiscardConnection(conn *PooledConnection, now time.Time) bool {
	// Check max lifetime
	if cp.config.MaxLifetime > 0 && now.Sub(conn.createdAt) > cp.config.MaxLifetime {
		return true
	}

	// Check idle time
	if cp.config.MaxIdleTime > 0 && now.Sub(conn.lastUsedAt) > cp.config.MaxIdleTime {
		return true
	}

	// Check health
	if !conn.isHealthy {
		return true
	}

	return false
}

// Stats returns pool statistics
func (cp *ConnectionPool) Stats() ConnectionPoolStats {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	return ConnectionPoolStats{
		ConnectionType:    cp.connType,
		AvailableConns:    len(cp.connections),
		ActiveConns:       len(cp.activeConnections),
		MaxConnections:    cp.config.MaxConnections,
		MinConnections:    cp.config.MinConnections,
		TotalCreated:      cp.created,
		TotalClosed:       cp.closed,
		TotalBorrowed:     cp.borrowed,
		TotalReturned:     cp.returned,
		TotalErrors:       cp.errors,
		IdleTimeout:       cp.config.MaxIdleTime,
		MaxLifetime:       cp.config.MaxLifetime,
	}
}

// ConnectionPoolStats represents pool statistics
type ConnectionPoolStats struct {
	ConnectionType db.ConnectionType
	AvailableConns int
	ActiveConns    int
	MaxConnections int
	MinConnections int
	TotalCreated   int64
	TotalClosed    int64
	TotalBorrowed  int64
	TotalReturned  int64
	TotalErrors    int64
	IdleTimeout    time.Duration
	MaxLifetime    time.Duration
}

// Close shuts down the connection pool
func (cp *ConnectionPool) Close() error {
	close(cp.stopHealthCheck)

	cp.mu.Lock()
	defer cp.mu.Unlock()

	// Close all connections in pool
	close(cp.connections)
	for conn := range cp.connections {
		conn.conn.Close()
		cp.closed++
	}

	// Close all active connections
	for conn := range cp.activeConnections {
		conn.conn.Close()
		cp.closed++
	}

	cp.activeConnections = make(map[*PooledConnection]bool)
	return nil
}

// healthCheckWorker runs periodic health checks
func (cp *ConnectionPool) healthCheckWorker() {
	if cp.config.HealthCheckInterval <= 0 {
		return
	}

	ticker := time.NewTicker(cp.config.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-cp.stopHealthCheck:
			return
		case <-ticker.C:
			cp.performHealthCheck()
		}
	}
}

// performHealthCheck checks the health of pooled connections
func (cp *ConnectionPool) performHealthCheck() {
	cp.mu.RLock()
	connections := make([]*PooledConnection, 0, len(cp.connections))

	// Temporarily drain the pool to check connections
	for {
		select {
		case conn := <-cp.connections:
			connections = append(connections, conn)
		default:
			goto checkConnections
		}
	}

checkConnections:
	cp.mu.RUnlock()

	// Check each connection
	healthyConnections := make([]*PooledConnection, 0, len(connections))
	for _, conn := range connections {
		if cp.healthCheck.CheckConnection(conn.conn) {
			conn.mu.Lock()
			conn.isHealthy = true
			conn.mu.Unlock()
			healthyConnections = append(healthyConnections, conn)
		} else {
			conn.mu.Lock()
			conn.isHealthy = false
			conn.mu.Unlock()
			// Connection will be discarded on next use
		}
	}

	// Return healthy connections to pool
	cp.mu.Lock()
	for _, conn := range healthyConnections {
		select {
		case cp.connections <- conn:
		default:
			// Pool is full, close excess connections
			conn.conn.Close()
			cp.closed++
		}
	}
	cp.mu.Unlock()
}

// HealthChecker provides health checking for database connections
type HealthChecker struct {
	validationQuery string
	interval        time.Duration
}

// NewHealthChecker creates a new health checker
func NewHealthChecker(validationQuery string, interval time.Duration) *HealthChecker {
	if validationQuery == "" {
		validationQuery = "SELECT 1"
	}

	return &HealthChecker{
		validationQuery: validationQuery,
		interval:        interval,
	}
}

// CheckConnection validates a database connection
func (hc *HealthChecker) CheckConnection(conn *sql.DB) bool {
	if conn == nil {
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var result int
	err := conn.QueryRowContext(ctx, hc.validationQuery).Scan(&result)
	return err == nil
}

// Connection implements the db.Connection interface for pooled connections
func (pc *PooledConnection) Connection() *sql.DB {
	return pc.conn
}

// Type returns the connection type
func (pc *PooledConnection) Type() db.ConnectionType {
	return pc.connType
}

// IsHealthy returns the health status
func (pc *PooledConnection) IsHealthy() bool {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.isHealthy
}

// BorrowCount returns the number of times this connection has been borrowed
func (pc *PooledConnection) BorrowCount() int64 {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.borrowCount
}

// Age returns the age of the connection
func (pc *PooledConnection) Age() time.Duration {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return time.Since(pc.createdAt)
}

// IdleTime returns how long the connection has been idle
func (pc *PooledConnection) IdleTime() time.Duration {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return time.Since(pc.lastUsedAt)
}

// GetAllStats returns statistics for all pools
func (cpm *ConnectionPoolManager) GetAllStats() map[db.ConnectionType]ConnectionPoolStats {
	cpm.mu.RLock()
	defer cpm.mu.RUnlock()

	stats := make(map[db.ConnectionType]ConnectionPoolStats)
	for connType, pool := range cpm.pools {
		stats[connType] = pool.Stats()
	}

	return stats
}

// Close shuts down all connection pools
func (cpm *ConnectionPoolManager) Close() error {
	cpm.mu.Lock()
	defer cpm.mu.Unlock()

	for _, pool := range cpm.pools {
		if err := pool.Close(); err != nil {
			return err
		}
	}

	cpm.pools = make(map[db.ConnectionType]*ConnectionPool)
	return nil
}

// OptimizedConnectionManager provides connection-level optimizations
type OptimizedConnectionManager struct {
	pools         *ConnectionPoolManager
	statementCache *StatementCache
	batchManager   *BatchOperationsManager
}

// NewOptimizedConnectionManager creates a connection manager with all optimizations
func NewOptimizedConnectionManager(poolConfig *ConnectionPoolConfig, cacheConfig *StatementCacheConfig) *OptimizedConnectionManager {
	return &OptimizedConnectionManager{
		pools:         NewConnectionPoolManager(poolConfig),
		statementCache: NewStatementCache(cacheConfig),
		batchManager:  nil, // Will be initialized per connection
	}
}

// GetOptimizedConnection returns a connection with all optimizations enabled
func (ocm *OptimizedConnectionManager) GetOptimizedConnection(ctx context.Context, connType db.ConnectionType, connectionFactory func() (*sql.DB, error)) (*OptimizedConnection, error) {
	pool, err := ocm.pools.GetPool(connType, nil, connectionFactory)
	if err != nil {
		return nil, err
	}

	pooledConn, err := pool.AcquireConnection(ctx)
	if err != nil {
		return nil, err
	}

	// Create a mock connection for batch manager
	mockConn := &poolConnection{db: pooledConn.conn, connType: pooledConn.connType}

	return &OptimizedConnection{
		pooledConn:     pooledConn,
		pool:           pool,
		statementCache: ocm.statementCache,
		batchManager:   NewBatchOperationsManager(mockConn),
	}, nil
}

// OptimizedConnection wraps a pooled connection with performance optimizations
type OptimizedConnection struct {
	pooledConn     *PooledConnection
	pool           *ConnectionPool
	statementCache *StatementCache
	batchManager   *BatchOperationsManager
}

// Release returns the connection to the pool
func (oc *OptimizedConnection) Release() error {
	return oc.pool.ReleaseConnection(oc.pooledConn)
}

// GetDB returns the underlying database connection
func (oc *OptimizedConnection) GetDB() *sql.DB {
	return oc.pooledConn.conn
}

// PrepareStatement gets a prepared statement with caching
func (oc *OptimizedConnection) PrepareStatement(ctx context.Context, query string) (*sql.Stmt, error) {
	// Create a mock connection that implements the StatementPreparer interface
	mockConn := &poolConnection{db: oc.pooledConn.conn, connType: oc.pooledConn.connType}
	return oc.statementCache.Get(ctx, mockConn, query)
}

// GetBatchOperations returns batch operations for a table
func GetBatchOperationsForConn[T any](oc *OptimizedConnection, tableName string) *BatchOperations[T] {
	return GetBatchOperations[T](oc.batchManager, tableName)
}

// poolConnection implements db.Connection for statement caching and batch operations
type poolConnection struct {
	db       *sql.DB
	connType db.ConnectionType
}

func (pc *poolConnection) Type() db.ConnectionType {
	return pc.connType
}

func (pc *poolConnection) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return pc.db.PrepareContext(ctx, query)
}

func (pc *poolConnection) Begin(ctx context.Context) (db.Transaction, error) {
	tx, err := pc.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	// Return a basic transaction wrapper - this would need proper implementation
	return &basicTransaction{tx: tx}, nil
}

func (pc *poolConnection) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return pc.db.QueryContext(ctx, query, args...)
}

func (pc *poolConnection) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return pc.db.QueryRowContext(ctx, query, args...)
}

func (pc *poolConnection) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return pc.db.ExecContext(ctx, query, args...)
}

// basicTransaction is a minimal transaction wrapper
type basicTransaction struct {
	tx *sql.Tx
}

func (bt *basicTransaction) Commit() error {
	return bt.tx.Commit()
}

func (bt *basicTransaction) Rollback() error {
	return bt.tx.Rollback()
}

func (bt *basicTransaction) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return bt.tx.QueryContext(ctx, query, args...)
}

func (bt *basicTransaction) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return bt.tx.QueryRowContext(ctx, query, args...)
}

func (bt *basicTransaction) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return bt.tx.ExecContext(ctx, query, args...)
}

func (bt *basicTransaction) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return bt.tx.PrepareContext(ctx, query)
}

func (bt *basicTransaction) Exec(query string, args ...interface{}) (sql.Result, error) {
	return bt.tx.Exec(query, args...)
}