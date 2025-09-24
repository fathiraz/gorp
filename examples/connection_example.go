package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/go-gorp/gorp/v3/db"
)

func main() {
	ctx := context.Background()

	// Example 1: MySQL Connection
	fmt.Println("=== MySQL Connection Example ===")
	mysqlConn, err := db.NewDatabaseBuilder(db.MySQLConnectionType).
		WithDSN("user:password@tcp(localhost:3306)/testdb").
		WithRole(db.PrimaryRole).
		WithMaxOpenConns(10).
		WithMaxIdleConns(5).
		WithConnMaxLifetime(5 * time.Minute).
		WithHealthCheck(true).
		Build(ctx)
	if err != nil {
		log.Printf("Failed to create MySQL connection: %v", err)
	} else {
		fmt.Printf("MySQL connection type: %s, role: %s\n", mysqlConn.Type(), mysqlConn.Role())
		fmt.Printf("MySQL connection is healthy: %t\n", mysqlConn.IsHealthy())
		mysqlConn.Close()
	}

	// Example 2: PostgreSQL Connection
	fmt.Println("\n=== PostgreSQL Connection Example ===")
	pgConn, err := db.NewDatabaseBuilder(db.PostgreSQLConnectionType).
		WithDSN("postgres://user:password@localhost:5432/testdb?sslmode=disable").
		WithRole(db.ReplicaRole).
		WithMaxOpenConns(15).
		WithMaxIdleConns(3).
		WithConnMaxLifetime(10 * time.Minute).
		Build(ctx)
	if err != nil {
		log.Printf("Failed to create PostgreSQL connection: %v", err)
	} else {
		fmt.Printf("PostgreSQL connection type: %s, role: %s\n", pgConn.Type(), pgConn.Role())
		fmt.Printf("PostgreSQL connection is healthy: %t\n", pgConn.IsHealthy())
		pgConn.Close()
	}

	// Example 3: SQLite Connection
	fmt.Println("\n=== SQLite Connection Example ===")
	sqliteConn, err := db.NewDatabaseBuilder(db.SQLiteConnectionType).
		WithDSN("./test.db").
		WithRole(db.PrimaryRole).
		Build(ctx)
	if err != nil {
		log.Printf("Failed to create SQLite connection: %v", err)
	} else {
		fmt.Printf("SQLite connection type: %s, role: %s\n", sqliteConn.Type(), sqliteConn.Role())
		fmt.Printf("SQLite connection is healthy: %t\n", sqliteConn.IsHealthy())
		sqliteConn.Close()
	}

	// Example 4: SQL Server Connection
	fmt.Println("\n=== SQL Server Connection Example ===")
	sqlServerConn, err := db.NewDatabaseBuilder(db.SQLServerConnectionType).
		WithDSN("server=localhost;user id=sa;password=Password123;database=testdb").
		WithRole(db.PrimaryRole).
		WithMaxOpenConns(20).
		WithMaxIdleConns(5).
		Build(ctx)
	if err != nil {
		log.Printf("Failed to create SQL Server connection: %v", err)
	} else {
		fmt.Printf("SQL Server connection type: %s, role: %s\n", sqlServerConn.Type(), sqlServerConn.Role())
		fmt.Printf("SQL Server connection is healthy: %t\n", sqlServerConn.IsHealthy())
		sqlServerConn.Close()
	}

	// Example 5: Connection Manager with Multiple Connections
	fmt.Println("\n=== Connection Manager Example ===")
	manager := db.NewConnectionManager(&db.ManagerConfig{
		EnableReadWriteSplitting: true,
		HealthCheckInterval:      30 * time.Second,
		ConnectionTimeout:        10 * time.Second,
		QueryTimeout:             30 * time.Second,
		EnableConnectionRetry:    true,
		MaxRetryAttempts:         3,
	})

	// Add primary MySQL connection
	primaryMySQL, err := db.NewDatabaseBuilder(db.MySQLConnectionType).
		WithDSN("user:password@tcp(localhost:3306)/testdb").
		WithRole(db.PrimaryRole).
		Build(ctx)
	if err == nil {
		manager.AddConnection("primary-mysql", primaryMySQL)
		fmt.Println("Added primary MySQL connection to manager")
	}

	// Add replica MySQL connection
	replicaMySQL, err := db.NewDatabaseBuilder(db.MySQLConnectionType).
		WithDSN("user:password@tcp(localhost:3307)/testdb").
		WithRole(db.ReplicaRole).
		Build(ctx)
	if err == nil {
		manager.AddConnection("replica-mysql", replicaMySQL)
		fmt.Println("Added replica MySQL connection to manager")
	}

	// Get connection for read query
	readConn, err := manager.GetConnectionForQuery(db.ReadQuery)
	if err != nil {
		log.Printf("Failed to get read connection: %v", err)
	} else {
		fmt.Printf("Got read connection: type=%s, role=%s\n", readConn.Type(), readConn.Role())
	}

	// Get connection for write query
	writeConn, err := manager.GetConnectionForQuery(db.WriteQuery)
	if err != nil {
		log.Printf("Failed to get write connection: %v", err)
	} else {
		fmt.Printf("Got write connection: type=%s, role=%s\n", writeConn.Type(), writeConn.Role())
	}

	// Perform health check
	fmt.Println("\n=== Health Check Example ===")
	healthResults := manager.HealthCheck(ctx)
	if len(healthResults) == 0 {
		fmt.Println("All connections are healthy!")
	} else {
		fmt.Println("Connection health issues:")
		for name, err := range healthResults {
			fmt.Printf("- %s: %v\n", name, err)
		}
	}

	// Clean up
	manager.Close()
	fmt.Println("\nAll connections closed successfully!")
}