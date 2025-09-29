// Package main demonstrates comprehensive OpenTelemetry tracing integration with GORP
// This example shows how to use all tracing features including distributed transactions,
// business logic instrumentation, and custom span attributes.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/fathiraz/gorp/instrumentation"
	"go.opentelemetry.io/otel/attribute"
)

func main() {
	// Create a context for the application
	ctx := context.Background()

	// Configure OpenTelemetry tracing
	tracingConfig := instrumentation.TracingConfig{
		ServiceName:                  "gorp-tracing-example",
		ServiceVersion:               "1.0.0",
		Environment:                  "development",
		SamplingRatio:                1.0, // Sample all traces for demo
		BatchTimeout:                 5 * time.Second,
		MaxBatchSize:                 512,
		EnableMetrics:                true,
		DatabaseName:                 "example_db",
		DisableQuerySanitization:     false,
		MaxQueryLength:               1000,
	}

	// Initialize tracing instrumentation
	tracer, err := instrumentation.NewTracingInstrumentation(tracingConfig)
	if err != nil {
		log.Fatalf("Failed to initialize tracing: %v", err)
	}

	// Ensure proper shutdown
	defer func() {
		if err := tracer.Shutdown(context.Background()); err != nil {
			log.Printf("Error shutting down tracer: %v", err)
		}
	}()

	// Open database connection
	db, err := sql.Open("postgres", "postgres://user:password@localhost/testdb?sslmode=disable")
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Wrap database with tracing
	tracedDB := tracer.WrapDatabase("postgres", db)

	// Example 1: Basic traced query
	fmt.Println("=== Example 1: Basic Traced Query ===")
	if err := basicTracedQuery(ctx, tracedDB); err != nil {
		log.Printf("Basic query error: %v", err)
	}

	// Example 2: Traced transaction
	fmt.Println("\n=== Example 2: Traced Transaction ===")
	if err := tracedTransaction(ctx, tracedDB); err != nil {
		log.Printf("Transaction error: %v", err)
	}

	// Example 3: Distributed transaction
	fmt.Println("\n=== Example 3: Distributed Transaction ===")
	if err := distributedTransaction(ctx, tracedDB, tracer); err != nil {
		log.Printf("Distributed transaction error: %v", err)
	}

	// Example 4: Business logic instrumentation
	fmt.Println("\n=== Example 4: Business Logic Instrumentation ===")
	if err := businessLogicExample(ctx, tracedDB, tracer); err != nil {
		log.Printf("Business logic error: %v", err)
	}

	// Example 5: Cache operation tracing
	fmt.Println("\n=== Example 5: Cache Operation Tracing ===")
	cacheExample(ctx, tracer)

	fmt.Println("\n=== Tracing Example Complete ===")
	fmt.Println("Check your OpenTelemetry collector or tracing backend for trace data")
}

// basicTracedQuery demonstrates basic database query tracing
func basicTracedQuery(ctx context.Context, db *instrumentation.TracedDB) error {
	// This query will be automatically traced with spans
	rows, err := db.QueryContext(ctx, "SELECT 1 as test_value")
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	// Process results
	for rows.Next() {
		var value int
		if err := rows.Scan(&value); err != nil {
			return fmt.Errorf("scan failed: %w", err)
		}
		fmt.Printf("Query result: %d\n", value)
	}

	return rows.Err()
}

// tracedTransaction demonstrates transaction tracing
func tracedTransaction(ctx context.Context, db *instrumentation.TracedDB) error {
	// Begin traced transaction
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction failed: %w", err)
	}

	// Ensure transaction cleanup
	defer func() {
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				log.Printf("Rollback failed: %v", rollbackErr)
			}
		}
	}()

	// Execute operations within transaction (these will be traced)
	_, err = tx.ExecContext(ctx, "CREATE TEMP TABLE test_table (id SERIAL, name TEXT)")
	if err != nil {
		return fmt.Errorf("create table failed: %w", err)
	}

	_, err = tx.ExecContext(ctx, "INSERT INTO test_table (name) VALUES ($1)", "test_name")
	if err != nil {
		return fmt.Errorf("insert failed: %w", err)
	}

	// Commit transaction
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit failed: %w", err)
	}

	fmt.Println("Transaction completed successfully")
	return nil
}

// distributedTransaction demonstrates distributed transaction tracing
func distributedTransaction(ctx context.Context, db *instrumentation.TracedDB, tracer *instrumentation.TracingInstrumentation) error {
	// Create distributed transaction manager
	dtm := instrumentation.NewDistributedTransactionManager(tracer)

	// Begin distributed transaction
	tx, err := dtm.BeginDistributedTransaction(ctx, db, nil, "dist-tx-001")
	if err != nil {
		return fmt.Errorf("begin distributed transaction failed: %w", err)
	}

	defer func() {
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				log.Printf("Distributed transaction rollback failed: %v", rollbackErr)
			}
		}
	}()

	// Simulate propagating context to another service
	propagatedCtx, headers := dtm.PropagateTransactionContext(ctx, "remote-service-call")
	fmt.Printf("Propagated trace headers: %v\n", headers)

	// Simulate receiving context from another service
	receivedCtx := dtm.ReceiveTransactionContext(propagatedCtx, headers)

	// Use the received context for operations
	_, err = tx.ExecContext(receivedCtx, "CREATE TEMP TABLE dist_test (id SERIAL, data TEXT)")
	if err != nil {
		return fmt.Errorf("distributed operation failed: %w", err)
	}

	// Commit distributed transaction
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("distributed transaction commit failed: %w", err)
	}

	fmt.Println("Distributed transaction completed successfully")
	return nil
}

// businessLogicExample demonstrates business logic instrumentation
func businessLogicExample(ctx context.Context, db *instrumentation.TracedDB, tracer *instrumentation.TracingInstrumentation) error {
	// Create instrumentation hooks
	hooks := instrumentation.NewInstrumentationHooks(tracer)

	// Trace business operation
	businessCtx, businessCleanup := hooks.TraceBusinessOperation(ctx, "user-registration",
		attribute.String("user.type", "premium"),
		attribute.String("registration.source", "web"),
	)
	defer businessCleanup()

	// Trace data access operation
	dataCtx, dataCleanup := hooks.TraceDataAccess(businessCtx, "user", "create",
		attribute.String("table", "users"),
		attribute.Int("batch_size", 1),
	)

	// Simulate data operation
	_, err := db.ExecContext(dataCtx, "CREATE TEMP TABLE users_example (id SERIAL, email TEXT)")
	dataCleanup(err) // Pass error to data cleanup

	if err != nil {
		return fmt.Errorf("data access failed: %w", err)
	}

	fmt.Println("Business logic instrumentation completed")
	return nil
}

// cacheExample demonstrates cache operation tracing
func cacheExample(ctx context.Context, tracer *instrumentation.TracingInstrumentation) {
	hooks := instrumentation.NewInstrumentationHooks(tracer)

	// Trace cache miss
	cacheCtx, cacheCleanup := hooks.TraceCacheOperation(ctx, "redis", "get", "user:123", false)
	defer cacheCleanup()

	// Simulate cache operation
	time.Sleep(10 * time.Millisecond) // Simulate cache lookup time

	fmt.Println("Cache operation traced")

	// Trace cache set operation
	_, setCacheCleanup := hooks.TraceCacheOperation(cacheCtx, "redis", "set", "user:123", true)
	defer setCacheCleanup()

	// Simulate cache set
	time.Sleep(5 * time.Millisecond)

	fmt.Println("Cache set operation traced")
}