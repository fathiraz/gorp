// Package testing provides Docker-based test utilities for comprehensive database testing
package testing

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"

	"github.com/fathiraz/gorp/db"
)

// DockerConfig contains configuration for Docker test containers
type DockerConfig struct {
	PostgreSQL PostgreSQLConfig `json:"postgresql"`
	MySQL      MySQLConfig      `json:"mysql"`
	SQLServer  SQLServerConfig  `json:"sqlserver"`
	Timeout    time.Duration    `json:"timeout"`
}

// PostgreSQLConfig contains PostgreSQL-specific Docker configuration
type PostgreSQLConfig struct {
	Image    string `json:"image"`
	Port     string `json:"port"`
	Database string `json:"database"`
	Username string `json:"username"`
	Password string `json:"password"`
}

// MySQLConfig contains MySQL-specific Docker configuration
type MySQLConfig struct {
	Image    string `json:"image"`
	Port     string `json:"port"`
	Database string `json:"database"`
	Username string `json:"username"`
	Password string `json:"password"`
}

// SQLServerConfig contains SQL Server-specific Docker configuration
type SQLServerConfig struct {
	Image    string `json:"image"`
	Port     string `json:"port"`
	Database string `json:"database"`
	Password string `json:"password"`
}

// DefaultDockerConfig returns sensible defaults for Docker containers
func DefaultDockerConfig() DockerConfig {
	return DockerConfig{
		PostgreSQL: PostgreSQLConfig{
			Image:    "postgres:16-alpine",
			Port:     "5432",
			Database: "gorp_test",
			Username: "postgres",
			Password: "test123",
		},
		MySQL: MySQLConfig{
			Image:    "mysql:8.0",
			Port:     "3306",
			Database: "gorp_test",
			Username: "root",
			Password: "test123",
		},
		SQLServer: SQLServerConfig{
			Image:    "mcr.microsoft.com/mssql/server:2022-latest",
			Port:     "1433",
			Database: "gorp_test",
			Password: "Test123!",
		},
		Timeout: 60 * time.Second,
	}
}

// TestContainer represents a running test database container
type TestContainer struct {
	ContainerID string
	Type        db.ConnectionType
	Config      interface{}
	Connection  db.Connection
	DSN         string
	cleanup     func() error
}

// Cleanup stops and removes the container
func (tc *TestContainer) Cleanup() error {
	if tc.cleanup != nil {
		return tc.cleanup()
	}
	return nil
}

// ContainerManager manages Docker test containers
type ContainerManager struct {
	config     DockerConfig
	containers map[string]*TestContainer
}

// NewContainerManager creates a new container manager
func NewContainerManager(config DockerConfig) *ContainerManager {
	return &ContainerManager{
		config:     config,
		containers: make(map[string]*TestContainer),
	}
}

// StartPostgreSQL starts a PostgreSQL test container
func (cm *ContainerManager) StartPostgreSQL(t *testing.T) *TestContainer {
	t.Helper()

	// Check if Docker is available
	if !isDockerAvailable() {
		t.Skip("Docker not available, skipping PostgreSQL container test")
	}

	cfg := cm.config.PostgreSQL
	containerName := fmt.Sprintf("gorp-test-postgres-%d", time.Now().Unix())

	// Start container
	containerID, err := startDockerContainer(dockerContainerOptions{
		Name:  containerName,
		Image: cfg.Image,
		Ports: map[string]string{cfg.Port: "5432"},
		Env: []string{
			fmt.Sprintf("POSTGRES_DB=%s", cfg.Database),
			fmt.Sprintf("POSTGRES_USER=%s", cfg.Username),
			fmt.Sprintf("POSTGRES_PASSWORD=%s", cfg.Password),
		},
		HealthCheck: func() error {
			dsn := fmt.Sprintf("postgres://%s:%s@localhost:%s/%s?sslmode=disable",
				cfg.Username, cfg.Password, cfg.Port, cfg.Database)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			pool, err := pgxpool.New(ctx, dsn)
			if err != nil {
				return err
			}
			defer pool.Close()

			return pool.Ping(ctx)
		},
		Timeout: cm.config.Timeout,
	})
	require.NoError(t, err, "Failed to start PostgreSQL container")

	// Create connection
	dsn := fmt.Sprintf("postgres://%s:%s@localhost:%s/%s?sslmode=disable",
		cfg.Username, cfg.Password, cfg.Port, cfg.Database)

	connection, err := db.NewPostgreSQLConnection(context.Background(), dsn)
	require.NoError(t, err, "Failed to create PostgreSQL connection")

	container := &TestContainer{
		ContainerID: containerID,
		Type:        db.PostgreSQLConnectionType,
		Config:      cfg,
		Connection:  connection,
		DSN:         dsn,
		cleanup: func() error {
			connection.Close()
			return stopDockerContainer(containerID)
		},
	}

	cm.containers[containerID] = container
	return container
}

// StartMySQL starts a MySQL test container
func (cm *ContainerManager) StartMySQL(t *testing.T) *TestContainer {
	t.Helper()

	if !isDockerAvailable() {
		t.Skip("Docker not available, skipping MySQL container test")
	}

	cfg := cm.config.MySQL
	containerName := fmt.Sprintf("gorp-test-mysql-%d", time.Now().Unix())

	containerID, err := startDockerContainer(dockerContainerOptions{
		Name:  containerName,
		Image: cfg.Image,
		Ports: map[string]string{cfg.Port: "3306"},
		Env: []string{
			fmt.Sprintf("MYSQL_DATABASE=%s", cfg.Database),
			fmt.Sprintf("MYSQL_ROOT_PASSWORD=%s", cfg.Password),
		},
		HealthCheck: func() error {
			dsn := fmt.Sprintf("%s:%s@tcp(localhost:%s)/%s?parseTime=true",
				cfg.Username, cfg.Password, cfg.Port, cfg.Database)

			sqlxDB, err := sqlx.Connect("mysql", dsn)
			if err != nil {
				return err
			}
			defer sqlxDB.Close()

			return sqlxDB.Ping()
		},
		Timeout: cm.config.Timeout,
	})
	require.NoError(t, err, "Failed to start MySQL container")

	dsn := fmt.Sprintf("%s:%s@tcp(localhost:%s)/%s?parseTime=true",
		cfg.Username, cfg.Password, cfg.Port, cfg.Database)

	connection, err := db.NewMySQLConnection(context.Background(), dsn)
	require.NoError(t, err, "Failed to create MySQL connection")

	container := &TestContainer{
		ContainerID: containerID,
		Type:        db.MySQLConnectionType,
		Config:      cfg,
		Connection:  connection,
		DSN:         dsn,
		cleanup: func() error {
			connection.Close()
			return stopDockerContainer(containerID)
		},
	}

	cm.containers[containerID] = container
	return container
}

// StartSQLServer starts a SQL Server test container
func (cm *ContainerManager) StartSQLServer(t *testing.T) *TestContainer {
	t.Helper()

	if !isDockerAvailable() {
		t.Skip("Docker not available, skipping SQL Server container test")
	}

	cfg := cm.config.SQLServer
	containerName := fmt.Sprintf("gorp-test-sqlserver-%d", time.Now().Unix())

	containerID, err := startDockerContainer(dockerContainerOptions{
		Name:  containerName,
		Image: cfg.Image,
		Ports: map[string]string{cfg.Port: "1433"},
		Env: []string{
			"ACCEPT_EULA=Y",
			fmt.Sprintf("SA_PASSWORD=%s", cfg.Password),
		},
		HealthCheck: func() error {
			dsn := fmt.Sprintf("server=localhost;port=%s;database=%s;user id=sa;password=%s;encrypt=disable",
				cfg.Port, cfg.Database, cfg.Password)

			sqlxDB, err := sqlx.Connect("sqlserver", dsn)
			if err != nil {
				return err
			}
			defer sqlxDB.Close()

			return sqlxDB.Ping()
		},
		Timeout: cm.config.Timeout,
	})
	require.NoError(t, err, "Failed to start SQL Server container")

	dsn := fmt.Sprintf("server=localhost;port=%s;database=%s;user id=sa;password=%s;encrypt=disable",
		cfg.Port, cfg.Database, cfg.Password)

	connection, err := db.NewSQLServerConnection(context.Background(), dsn)
	require.NoError(t, err, "Failed to create SQL Server connection")

	container := &TestContainer{
		ContainerID: containerID,
		Type:        db.SQLServerConnectionType,
		Config:      cfg,
		Connection:  connection,
		DSN:         dsn,
		cleanup: func() error {
			connection.Close()
			return stopDockerContainer(containerID)
		},
	}

	cm.containers[containerID] = container
	return container
}

// StartSQLite creates an in-memory SQLite connection (no Docker needed)
func (cm *ContainerManager) StartSQLite(t *testing.T) *TestContainer {
	t.Helper()

	dsn := ":memory:"
	connection, err := db.NewSQLiteConnection(context.Background(), dsn)
	require.NoError(t, err, "Failed to create SQLite connection")

	container := &TestContainer{
		ContainerID: "sqlite-memory",
		Type:        db.SQLiteConnectionType,
		Config:      nil,
		Connection:  connection,
		DSN:         dsn,
		cleanup: func() error {
			return connection.Close()
		},
	}

	return container
}

// CleanupAll stops and removes all managed containers
func (cm *ContainerManager) CleanupAll() error {
	var errors []error
	for _, container := range cm.containers {
		if err := container.Cleanup(); err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("cleanup errors: %v", errors)
	}
	return nil
}

// TestSuite provides a testing suite with multiple database backends
type TestSuite struct {
	t                *testing.T
	containerManager *ContainerManager
	PostgreSQL       *TestContainer
	MySQL            *TestContainer
	SQLServer        *TestContainer
	SQLite           *TestContainer
}

// NewTestSuite creates a new test suite with all database backends
func NewTestSuite(t *testing.T, config DockerConfig) *TestSuite {
	t.Helper()

	cm := NewContainerManager(config)

	suite := &TestSuite{
		t:                t,
		containerManager: cm,
	}

	// Start containers in parallel for speed
	done := make(chan struct{}, 4)

	go func() {
		suite.PostgreSQL = cm.StartPostgreSQL(t)
		done <- struct{}{}
	}()

	go func() {
		suite.MySQL = cm.StartMySQL(t)
		done <- struct{}{}
	}()

	go func() {
		suite.SQLServer = cm.StartSQLServer(t)
		done <- struct{}{}
	}()

	go func() {
		suite.SQLite = cm.StartSQLite(t)
		done <- struct{}{}
	}()

	// Wait for all containers to start
	for i := 0; i < 4; i++ {
		<-done
	}

	// Cleanup on test completion
	t.Cleanup(func() {
		if err := cm.CleanupAll(); err != nil {
			t.Logf("Cleanup errors: %v", err)
		}
	})

	return suite
}

// RunOnAllDatabases runs a test function on all available databases
func (ts *TestSuite) RunOnAllDatabases(testFunc func(t *testing.T, conn db.Connection, dbType db.ConnectionType)) {
	databases := map[string]struct {
		container *TestContainer
		dbType    db.ConnectionType
	}{
		"PostgreSQL": {ts.PostgreSQL, db.PostgreSQLConnectionType},
		"MySQL":      {ts.MySQL, db.MySQLConnectionType},
		"SQLServer":  {ts.SQLServer, db.SQLServerConnectionType},
		"SQLite":     {ts.SQLite, db.SQLiteConnectionType},
	}

	for name, db := range databases {
		if db.container != nil {
			ts.t.Run(name, func(t *testing.T) {
				testFunc(t, db.container.Connection, db.dbType)
			})
		}
	}
}

// RunOnDatabase runs a test function on a specific database type
func (ts *TestSuite) RunOnDatabase(dbType db.ConnectionType, testFunc func(t *testing.T, conn db.Connection)) {
	var container *TestContainer

	switch dbType {
	case db.PostgreSQLConnectionType:
		container = ts.PostgreSQL
	case db.MySQLConnectionType:
		container = ts.MySQL
	case db.SQLServerConnectionType:
		container = ts.SQLServer
	case db.SQLiteConnectionType:
		container = ts.SQLite
	}

	if container != nil {
		testFunc(ts.t, container.Connection)
	} else {
		ts.t.Skipf("Database type %s not available", dbType)
	}
}

// Helper functions for environment-based testing
func GetTestDSN(dbType db.ConnectionType) (string, bool) {
	switch dbType {
	case db.PostgreSQLConnectionType:
		if dsn := os.Getenv("GORP_TEST_POSTGRES_DSN"); dsn != "" {
			return dsn, true
		}
	case db.MySQLConnectionType:
		if dsn := os.Getenv("GORP_TEST_MYSQL_DSN"); dsn != "" {
			return dsn, true
		}
	case db.SQLServerConnectionType:
		if dsn := os.Getenv("GORP_TEST_SQLSERVER_DSN"); dsn != "" {
			return dsn, true
		}
	case db.SQLiteConnectionType:
		return ":memory:", true
	}
	return "", false
}

// CreateTestConnection creates a test connection using environment variables or Docker
func CreateTestConnection(t *testing.T, dbType db.ConnectionType) db.Connection {
	t.Helper()

	ctx := context.Background()

	// Try environment variable first
	if dsn, ok := GetTestDSN(dbType); ok {
		var conn db.Connection
		var err error

		switch dbType {
		case db.PostgreSQLConnectionType:
			conn, err = db.NewPostgreSQLConnection(ctx, dsn)
		case db.MySQLConnectionType:
			conn, err = db.NewMySQLConnection(ctx, dsn)
		case db.SQLServerConnectionType:
			conn, err = db.NewSQLServerConnection(ctx, dsn)
		case db.SQLiteConnectionType:
			conn, err = db.NewSQLiteConnection(ctx, dsn)
		default:
			t.Fatalf("Unsupported database type: %s", dbType)
		}

		if err == nil {
			t.Cleanup(func() { conn.Close() })
			return conn
		}
		t.Logf("Failed to connect using environment DSN: %v", err)
	}

	// Fall back to Docker
	cm := NewContainerManager(DefaultDockerConfig())
	var container *TestContainer

	switch dbType {
	case db.PostgreSQLConnectionType:
		container = cm.StartPostgreSQL(t)
	case db.MySQLConnectionType:
		container = cm.StartMySQL(t)
	case db.SQLServerConnectionType:
		container = cm.StartSQLServer(t)
	case db.SQLiteConnectionType:
		container = cm.StartSQLite(t)
	default:
		t.Fatalf("Unsupported database type: %s", dbType)
	}

	return container.Connection
}