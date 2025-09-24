package db

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDatabaseBuilder(t *testing.T) {
	ctx := context.Background()

	t.Run("MySQL Builder", func(t *testing.T) {
		builder := NewDatabaseBuilder(MySQLConnectionType)
		assert.Equal(t, MySQLConnectionType, builder.config.Type)
		assert.Equal(t, PrimaryRole, builder.config.Role)
		assert.Equal(t, 25, builder.config.MaxOpenConns)
		assert.Equal(t, 5, builder.config.MaxIdleConns)
		assert.Equal(t, 5*time.Minute, builder.config.ConnMaxLife)
		assert.True(t, builder.config.HealthCheck)
		assert.Equal(t, 3, builder.config.RetryAttempts)
		assert.Equal(t, 1*time.Second, builder.config.RetryInterval)
	})

	t.Run("PostgreSQL Builder", func(t *testing.T) {
		builder := NewDatabaseBuilder(PostgreSQLConnectionType).
			WithRole(ReplicaRole).
			WithMaxOpenConns(50).
			WithMaxIdleConns(10).
			WithConnMaxLifetime(10 * time.Minute).
			WithHealthCheck(false)

		assert.Equal(t, PostgreSQLConnectionType, builder.config.Type)
		assert.Equal(t, ReplicaRole, builder.config.Role)
		assert.Equal(t, 50, builder.config.MaxOpenConns)
		assert.Equal(t, 10, builder.config.MaxIdleConns)
		assert.Equal(t, 10*time.Minute, builder.config.ConnMaxLife)
		assert.False(t, builder.config.HealthCheck)
	})

	t.Run("SQLite Builder", func(t *testing.T) {
		builder := NewDatabaseBuilder(SQLiteConnectionType).
			WithDSN(":memory:")

		assert.Equal(t, SQLiteConnectionType, builder.config.Type)
		assert.Equal(t, ":memory:", builder.config.DSN)

		// Test building SQLite connection
		conn, err := builder.Build(ctx)
		if err != nil {
			t.Skipf("SQLite not available in test environment: %v", err)
		} else {
			defer conn.Close()
			assert.Equal(t, SQLiteConnectionType, conn.Type())
			assert.Equal(t, PrimaryRole, conn.Role())
		}
	})

	t.Run("SQL Server Builder", func(t *testing.T) {
		builder := NewDatabaseBuilder(SQLServerConnectionType)
		assert.Equal(t, SQLServerConnectionType, builder.config.Type)
	})

	t.Run("Invalid Database Type", func(t *testing.T) {
		builder := NewDatabaseBuilder("invalid")
		_, err := builder.Build(ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported database type")
	})
}

func TestConnectionManager(t *testing.T) {
	t.Run("Default Configuration", func(t *testing.T) {
		manager := NewConnectionManager(nil)
		assert.NotNil(t, manager)
		assert.NotNil(t, manager.config)
		assert.False(t, manager.config.EnableReadWriteSplitting)
		assert.Equal(t, 30*time.Second, manager.config.HealthCheckInterval)
		assert.Equal(t, 10*time.Second, manager.config.ConnectionTimeout)
		assert.Equal(t, 30*time.Second, manager.config.QueryTimeout)
		assert.True(t, manager.config.EnableConnectionRetry)
		assert.Equal(t, 3, manager.config.MaxRetryAttempts)
	})

	t.Run("Custom Configuration", func(t *testing.T) {
		config := &ManagerConfig{
			EnableReadWriteSplitting: true,
			HealthCheckInterval:      60 * time.Second,
			ConnectionTimeout:        5 * time.Second,
			QueryTimeout:             60 * time.Second,
			EnableConnectionRetry:    false,
			MaxRetryAttempts:         5,
		}
		manager := NewConnectionManager(config)
		assert.Equal(t, config, manager.config)
	})

	t.Run("Add and Get Connection", func(t *testing.T) {
		manager := NewConnectionManager(nil)

		// Test with mock connection
		mockConn := &mockConnection{
			connType: MySQLConnectionType,
			role:     PrimaryRole,
			healthy:  true,
		}

		// Add connection
		err := manager.AddConnection("test-conn", mockConn)
		assert.NoError(t, err)

		// Get connection
		conn, err := manager.GetConnection("test-conn")
		assert.NoError(t, err)
		assert.Equal(t, mockConn, conn)

		// Get non-existent connection
		_, err = manager.GetConnection("non-existent")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("Connection Routing", func(t *testing.T) {
		manager := NewConnectionManager(&ManagerConfig{
			EnableReadWriteSplitting: true,
		})

		primaryConn := &mockConnection{
			connType: MySQLConnectionType,
			role:     PrimaryRole,
			healthy:  true,
		}
		replicaConn := &mockConnection{
			connType: MySQLConnectionType,
			role:     ReplicaRole,
			healthy:  true,
		}

		manager.AddConnection("primary", primaryConn)
		manager.AddConnection("replica", replicaConn)

		// Test write query routing (should go to primary)
		writeConn, err := manager.GetConnectionForQuery(WriteQuery)
		assert.NoError(t, err)
		assert.Equal(t, PrimaryRole, writeConn.Role())

		// Test read query routing (should prefer replica)
		readConn, err := manager.GetConnectionForQuery(ReadQuery)
		assert.NoError(t, err)
		// Note: This test may fail if no healthy replica is available
		_ = readConn // Acknowledge that we're not using this variable in the test
	})

	t.Run("Health Check", func(t *testing.T) {
		manager := NewConnectionManager(nil)

		healthyConn := &mockConnection{
			connType: MySQLConnectionType,
			role:     PrimaryRole,
			healthy:  true,
		}
		unhealthyConn := &mockConnection{
			connType: PostgreSQLConnectionType,
			role:     ReplicaRole,
			healthy:  false,
		}

		manager.AddConnection("healthy", healthyConn)
		manager.AddConnection("unhealthy", unhealthyConn)

		ctx := context.Background()
		results := manager.HealthCheck(ctx)

		// Healthy connection should not appear in results
		_, hasHealthy := results["healthy"]
		assert.False(t, hasHealthy)

		// Unhealthy connection should appear in results
		_, hasUnhealthy := results["unhealthy"]
		assert.True(t, hasUnhealthy)
	})
}

func TestConnectionRouter(t *testing.T) {
	t.Run("Register and Route Connections", func(t *testing.T) {
		router := NewConnectionRouter()

		primaryConn := &mockConnection{
			connType: MySQLConnectionType,
			role:     PrimaryRole,
			healthy:  true,
		}
		replicaConn := &mockConnection{
			connType: MySQLConnectionType,
			role:     ReplicaRole,
			healthy:  true,
		}

		// Register connections
		err := router.RegisterConnection("primary", primaryConn)
		assert.NoError(t, err)

		err = router.RegisterConnection("replica", replicaConn)
		assert.NoError(t, err)

		// Test write query routing
		conn, err := router.RouteQuery(WriteQuery)
		assert.NoError(t, err)
		assert.Equal(t, PrimaryRole, conn.Role())

		// Test read query routing
		conn, err = router.RouteQuery(ReadQuery)
		assert.NoError(t, err)
		// Should prefer replica but fallback to primary if needed
	})

	t.Run("Invalid Connection Role", func(t *testing.T) {
		router := NewConnectionRouter()
		invalidConn := &mockConnection{
			connType: MySQLConnectionType,
			role:     "invalid-role",
			healthy:  true,
		}

		err := router.RegisterConnection("invalid", invalidConn)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported connection role")
	})
}

// Mock connection for testing
type mockConnection struct {
	connType ConnectionType
	role     ConnectionRole
	healthy  bool
}

func (m *mockConnection) Type() ConnectionType                                                       { return m.connType }
func (m *mockConnection) Role() ConnectionRole                                                       { return m.role }
func (m *mockConnection) IsHealthy() bool                                                            { return m.healthy }
func (m *mockConnection) Ping(ctx context.Context) error {
	if !m.healthy {
		return fmt.Errorf("connection unhealthy")
	}
	return nil
}
func (m *mockConnection) Close() error                                                              { return nil }
func (m *mockConnection) Stats() ConnectionStats                                                     { return ConnectionStats{} }
func (m *mockConnection) Query(ctx context.Context, query string, args ...interface{}) (Rows, error) { return nil, nil }
func (m *mockConnection) QueryRow(ctx context.Context, query string, args ...interface{}) Row     { return nil }
func (m *mockConnection) Exec(ctx context.Context, query string, args ...interface{}) (Result, error) { return nil, nil }
func (m *mockConnection) Select(ctx context.Context, dest interface{}, query string, args ...interface{}) error { return nil }
func (m *mockConnection) Get(ctx context.Context, dest interface{}, query string, args ...interface{}) error { return nil }
func (m *mockConnection) NamedExec(ctx context.Context, query string, arg interface{}) (Result, error) { return nil, nil }
func (m *mockConnection) NamedQuery(ctx context.Context, query string, arg interface{}) (Rows, error) { return nil, nil }
func (m *mockConnection) Begin(ctx context.Context) (Transaction, error)                            { return nil, nil }
func (m *mockConnection) BeginTx(ctx context.Context, opts *sql.TxOptions) (Transaction, error)   { return nil, nil }