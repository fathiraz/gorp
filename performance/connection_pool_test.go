package performance

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/go-gorp/gorp/v3/db"
	_ "github.com/mattn/go-sqlite3" // SQLite driver for testing
)

func TestConnectionPool_Basic(t *testing.T) {
	config := &ConnectionPoolConfig{
		MinConnections:     2,
		MaxConnections:     5,
		MaxIdleTime:        10 * time.Second,
		MaxLifetime:        1 * time.Minute,
		AcquisitionTimeout: 5 * time.Second,
		EnableTracing:      false, // Disable for simpler test
	}

	connectionFactory := func() (*sql.DB, error) {
		return sql.Open("sqlite3", ":memory:")
	}

	pool, err := NewConnectionPool(db.SQLiteConnectionType, config, connectionFactory)
	if err != nil {
		t.Fatalf("Failed to create connection pool: %v", err)
	}
	defer pool.Close()

	ctx := context.Background()

	// Test acquiring connection
	conn, err := pool.AcquireConnection(ctx)
	if err != nil {
		t.Fatalf("Failed to acquire connection: %v", err)
	}

	if conn == nil {
		t.Fatal("Expected non-nil connection")
	}

	if conn.Type() != db.SQLiteConnectionType {
		t.Errorf("Expected SQLite connection type, got %v", conn.Type())
	}

	// Test connection is healthy
	if !conn.IsHealthy() {
		t.Error("Expected connection to be healthy")
	}

	// Test releasing connection
	err = pool.ReleaseConnection(conn)
	if err != nil {
		t.Fatalf("Failed to release connection: %v", err)
	}

	// Test stats
	stats := pool.Stats()
	if stats.TotalBorrowed != 1 {
		t.Errorf("Expected 1 borrowed, got %d", stats.TotalBorrowed)
	}
	if stats.TotalReturned != 1 {
		t.Errorf("Expected 1 returned, got %d", stats.TotalReturned)
	}
}

func TestConnectionPool_AcquisitionTimeout(t *testing.T) {
	config := &ConnectionPoolConfig{
		MinConnections:     1,
		MaxConnections:     1, // Very small pool
		MaxIdleTime:        10 * time.Second,
		MaxLifetime:        1 * time.Minute,
		AcquisitionTimeout: 100 * time.Millisecond, // Short timeout
		EnableTracing:      false,
	}

	connectionFactory := func() (*sql.DB, error) {
		return sql.Open("sqlite3", ":memory:")
	}

	pool, err := NewConnectionPool(db.SQLiteConnectionType, config, connectionFactory)
	if err != nil {
		t.Fatalf("Failed to create connection pool: %v", err)
	}
	defer pool.Close()

	ctx := context.Background()

	// Acquire the only connection
	conn1, err := pool.AcquireConnection(ctx)
	if err != nil {
		t.Fatalf("Failed to acquire first connection: %v", err)
	}

	// Try to acquire second connection - should timeout
	start := time.Now()
	conn2, err := pool.AcquireConnection(ctx)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Expected timeout error")
		pool.ReleaseConnection(conn2)
	}

	if elapsed < 90*time.Millisecond || elapsed > 200*time.Millisecond {
		t.Errorf("Expected timeout around 100ms, got %v", elapsed)
	}

	// Release first connection and try again
	pool.ReleaseConnection(conn1)

	conn3, err := pool.AcquireConnection(ctx)
	if err != nil {
		t.Fatalf("Failed to acquire connection after release: %v", err)
	}
	defer pool.ReleaseConnection(conn3)
}

func TestConnectionPool_ConcurrentAccess(t *testing.T) {
	config := &ConnectionPoolConfig{
		MinConnections:     2,
		MaxConnections:     10,
		MaxIdleTime:        10 * time.Second,
		MaxLifetime:        1 * time.Minute,
		AcquisitionTimeout: 5 * time.Second,
		EnableTracing:      false,
	}

	connectionFactory := func() (*sql.DB, error) {
		return sql.Open("sqlite3", ":memory:")
	}

	pool, err := NewConnectionPool(db.SQLiteConnectionType, config, connectionFactory)
	if err != nil {
		t.Fatalf("Failed to create connection pool: %v", err)
	}
	defer pool.Close()

	ctx := context.Background()
	var wg sync.WaitGroup
	errors := make([]error, 20)

	// Launch 20 goroutines to acquire and release connections
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			conn, err := pool.AcquireConnection(ctx)
			if err != nil {
				errors[index] = err
				return
			}

			// Hold connection briefly
			time.Sleep(10 * time.Millisecond)

			err = pool.ReleaseConnection(conn)
			if err != nil {
				errors[index] = err
			}
		}(i)
	}

	wg.Wait()

	// Check for errors
	for i, err := range errors {
		if err != nil {
			t.Errorf("Goroutine %d failed: %v", i, err)
		}
	}

	// Verify final stats
	stats := pool.Stats()
	if stats.TotalBorrowed != 20 {
		t.Errorf("Expected 20 borrowed, got %d", stats.TotalBorrowed)
	}
	if stats.TotalReturned != 20 {
		t.Errorf("Expected 20 returned, got %d", stats.TotalReturned)
	}
	if stats.TotalErrors > 0 {
		t.Errorf("Expected 0 errors, got %d", stats.TotalErrors)
	}
}

func TestConnectionPool_HealthCheck(t *testing.T) {
	config := &ConnectionPoolConfig{
		MinConnections:      1,
		MaxConnections:      3,
		MaxIdleTime:         10 * time.Second,
		MaxLifetime:         1 * time.Minute,
		AcquisitionTimeout:  5 * time.Second,
		HealthCheckInterval: 50 * time.Millisecond, // Fast health check
		EnableTracing:       false,
		ValidationQuery:     "SELECT 1",
	}

	connectionFactory := func() (*sql.DB, error) {
		return sql.Open("sqlite3", ":memory:")
	}

	pool, err := NewConnectionPool(db.SQLiteConnectionType, config, connectionFactory)
	if err != nil {
		t.Fatalf("Failed to create connection pool: %v", err)
	}
	defer pool.Close()

	// Let health check run a few times
	time.Sleep(200 * time.Millisecond)

	stats := pool.Stats()
	if stats.AvailableConns < 1 {
		t.Error("Expected at least 1 available connection after health check")
	}
}

func TestConnectionPool_MaxLifetime(t *testing.T) {
	config := &ConnectionPoolConfig{
		MinConnections:     1,
		MaxConnections:     2,
		MaxIdleTime:        10 * time.Second,
		MaxLifetime:        50 * time.Millisecond, // Very short lifetime
		AcquisitionTimeout: 5 * time.Second,
		EnableTracing:      false,
	}

	connectionFactory := func() (*sql.DB, error) {
		return sql.Open("sqlite3", ":memory:")
	}

	pool, err := NewConnectionPool(db.SQLiteConnectionType, config, connectionFactory)
	if err != nil {
		t.Fatalf("Failed to create connection pool: %v", err)
	}
	defer pool.Close()

	ctx := context.Background()

	// Acquire connection
	conn, err := pool.AcquireConnection(ctx)
	if err != nil {
		t.Fatalf("Failed to acquire connection: %v", err)
	}

	// Wait for connection to exceed max lifetime
	time.Sleep(100 * time.Millisecond)

	// Release connection - should be discarded due to age
	initialClosed := pool.Stats().TotalClosed
	err = pool.ReleaseConnection(conn)
	if err != nil {
		t.Fatalf("Failed to release connection: %v", err)
	}

	finalClosed := pool.Stats().TotalClosed
	if finalClosed <= initialClosed {
		t.Error("Expected connection to be closed due to max lifetime")
	}
}

func TestConnectionPoolManager(t *testing.T) {
	config := DefaultConnectionPoolConfig()
	config.EnableTracing = false

	manager := NewConnectionPoolManager(config)
	defer manager.Close()

	// Connection factories for different types
	sqliteFactory := func() (*sql.DB, error) {
		return sql.Open("sqlite3", ":memory:")
	}

	// Get pools for different connection types
	sqlitePool, err := manager.GetPool(db.SQLiteConnectionType, config, sqliteFactory)
	if err != nil {
		t.Fatalf("Failed to get SQLite pool: %v", err)
	}

	// Should return same pool on second call
	sqlitePool2, err := manager.GetPool(db.SQLiteConnectionType, config, sqliteFactory)
	if err != nil {
		t.Fatalf("Failed to get SQLite pool second time: %v", err)
	}

	if sqlitePool != sqlitePool2 {
		t.Error("Expected same pool instance")
	}

	// Test stats
	allStats := manager.GetAllStats()
	if len(allStats) != 1 {
		t.Errorf("Expected 1 pool, got %d", len(allStats))
	}

	if _, exists := allStats[db.SQLiteConnectionType]; !exists {
		t.Error("Expected SQLite pool in stats")
	}
}

func TestOptimizedConnectionManager(t *testing.T) {
	poolConfig := DefaultConnectionPoolConfig()
	poolConfig.EnableTracing = false

	cacheConfig := DefaultStatementCacheConfig()
	cacheConfig.EnableTracing = false

	manager := NewOptimizedConnectionManager(poolConfig, cacheConfig)

	connectionFactory := func() (*sql.DB, error) {
		return sql.Open("sqlite3", ":memory:")
	}

	ctx := context.Background()
	optimizedConn, err := manager.GetOptimizedConnection(ctx, db.SQLiteConnectionType, connectionFactory)
	if err != nil {
		t.Fatalf("Failed to get optimized connection: %v", err)
	}
	defer optimizedConn.Release()

	// Test basic connection functionality
	db := optimizedConn.GetDB()
	if db == nil {
		t.Fatal("Expected non-nil database connection")
	}

	// Test prepared statement with caching
	stmt, err := optimizedConn.PrepareStatement(ctx, "SELECT ?")
	if err != nil {
		t.Fatalf("Failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	// Test statement execution
	var result int
	err = stmt.QueryRowContext(ctx, 42).Scan(&result)
	if err != nil {
		t.Fatalf("Failed to execute statement: %v", err)
	}

	if result != 42 {
		t.Errorf("Expected 42, got %d", result)
	}
}

func TestPooledConnection_Methods(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	conn := &PooledConnection{
		conn:        db,
		connType:    db.SQLiteConnectionType,
		createdAt:   time.Now().Add(-1 * time.Minute),
		lastUsedAt:  time.Now().Add(-30 * time.Second),
		borrowCount: 5,
		isHealthy:   true,
	}

	// Test Type
	if conn.Type() != db.SQLiteConnectionType {
		t.Errorf("Expected SQLite type, got %v", conn.Type())
	}

	// Test IsHealthy
	if !conn.IsHealthy() {
		t.Error("Expected connection to be healthy")
	}

	// Test BorrowCount
	if conn.BorrowCount() != 5 {
		t.Errorf("Expected borrow count 5, got %d", conn.BorrowCount())
	}

	// Test Age (should be around 1 minute)
	age := conn.Age()
	if age < 50*time.Second || age > 70*time.Second {
		t.Errorf("Expected age around 1 minute, got %v", age)
	}

	// Test IdleTime (should be around 30 seconds)
	idleTime := conn.IdleTime()
	if idleTime < 25*time.Second || idleTime > 35*time.Second {
		t.Errorf("Expected idle time around 30 seconds, got %v", idleTime)
	}

	// Test Connection
	if conn.Connection() != db {
		t.Error("Expected same database instance")
	}
}

func TestHealthChecker(t *testing.T) {
	hc := NewHealthChecker("SELECT 1", 1*time.Minute)

	// Test with valid connection
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	if !hc.CheckConnection(db) {
		t.Error("Expected healthy connection")
	}

	// Test with nil connection
	if hc.CheckConnection(nil) {
		t.Error("Expected nil connection to be unhealthy")
	}

	// Test with closed connection
	db.Close()
	if hc.CheckConnection(db) {
		t.Error("Expected closed connection to be unhealthy")
	}
}

func BenchmarkConnectionPool_AcquireRelease(b *testing.B) {
	config := &ConnectionPoolConfig{
		MinConnections:     5,
		MaxConnections:     10,
		MaxIdleTime:        10 * time.Second,
		MaxLifetime:        1 * time.Minute,
		AcquisitionTimeout: 5 * time.Second,
		EnableTracing:      false,
	}

	connectionFactory := func() (*sql.DB, error) {
		return sql.Open("sqlite3", ":memory:")
	}

	pool, err := NewConnectionPool(db.SQLiteConnectionType, config, connectionFactory)
	if err != nil {
		b.Fatalf("Failed to create connection pool: %v", err)
	}
	defer pool.Close()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn, err := pool.AcquireConnection(ctx)
		if err != nil {
			b.Fatalf("Failed to acquire connection: %v", err)
		}
		pool.ReleaseConnection(conn)
	}
}

func BenchmarkConnectionPool_Concurrent(b *testing.B) {
	config := &ConnectionPoolConfig{
		MinConnections:     10,
		MaxConnections:     20,
		MaxIdleTime:        10 * time.Second,
		MaxLifetime:        1 * time.Minute,
		AcquisitionTimeout: 5 * time.Second,
		EnableTracing:      false,
	}

	connectionFactory := func() (*sql.DB, error) {
		return sql.Open("sqlite3", ":memory:")
	}

	pool, err := NewConnectionPool(db.SQLiteConnectionType, config, connectionFactory)
	if err != nil {
		b.Fatalf("Failed to create connection pool: %v", err)
	}
	defer pool.Close()

	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			conn, err := pool.AcquireConnection(ctx)
			if err != nil {
				b.Fatalf("Failed to acquire connection: %v", err)
			}
			time.Sleep(1 * time.Microsecond) // Simulate work
			pool.ReleaseConnection(conn)
		}
	})
}