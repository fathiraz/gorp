package transaction

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-gorp/gorp/v3/db"
)

// TransactionManager provides advanced transaction management with context support
type TransactionManager[T Transactional] struct {
	conn          db.Connection
	hooks         *TransactionHooks[T]
	config        *TransactionConfig
	tracer        trace.Tracer
	connManager   *TransactionScopedConnectionManager[T]
	mu            sync.RWMutex
	activeTxns    map[string]*ManagedTransaction[T]
}

// Transactional constrains types that can be used in transactions
type Transactional interface {
	~struct{}
}

// TransactionConfig holds transaction manager configuration
type TransactionConfig struct {
	DefaultTimeout    time.Duration
	MaxRetryAttempts  int
	RetryBackoffBase  time.Duration
	RetryBackoffMax   time.Duration
	EnableSavepoints  bool
	EnableTracing     bool
	TracerName        string
	MaxNestedLevels   int
}

// DefaultTransactionConfig returns sensible defaults
func DefaultTransactionConfig() *TransactionConfig {
	return &TransactionConfig{
		DefaultTimeout:    30 * time.Second,
		MaxRetryAttempts:  3,
		RetryBackoffBase:  100 * time.Millisecond,
		RetryBackoffMax:   10 * time.Second,
		EnableSavepoints:  true,
		EnableTracing:     true,
		TracerName:        "gorp.transaction",
		MaxNestedLevels:   10,
	}
}

// NewTransactionManager creates a new transaction manager
func NewTransactionManager[T Transactional](conn db.Connection, config *TransactionConfig) *TransactionManager[T] {
	if config == nil {
		config = DefaultTransactionConfig()
	}

	var tracer trace.Tracer
	if config.EnableTracing {
		tracer = otel.Tracer(config.TracerName)
	}

	// Create connection manager
	connManagerConfig := DefaultConnectionManagerConfig()
	connManagerConfig.EnableTracing = config.EnableTracing
	connManager := NewTransactionScopedConnectionManager[T](conn, tracer, connManagerConfig)

	tm := &TransactionManager[T]{
		conn:        conn,
		hooks:       NewTransactionHooks[T](),
		config:      config,
		tracer:      tracer,
		connManager: connManager,
		activeTxns:  make(map[string]*ManagedTransaction[T]),
	}

	return tm
}

// GetHooks returns the transaction hooks manager
func (tm *TransactionManager[T]) GetHooks() *TransactionHooks[T] {
	return tm.hooks
}

// GetActiveTransactions returns statistics for all active transactions
func (tm *TransactionManager[T]) GetActiveTransactions() []TransactionInfo {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	info := make([]TransactionInfo, 0, len(tm.activeTxns))
	for txID, tx := range tm.activeTxns {
		info = append(info, TransactionInfo{
			ID:           txID,
			NestLevel:    tx.GetNestLevel(),
			CreatedAt:    time.Now(), // In a real implementation, track creation time
			ConnectionStats: tx.GetConnectionStats(),
		})
	}
	return info
}

// GetConnectionManagerStats returns connection manager statistics
func (tm *TransactionManager[T]) GetConnectionManagerStats() ManagerStats {
	return tm.connManager.GetManagerStats()
}

// StartBackgroundCleanup starts background cleanup for idle connections
func (tm *TransactionManager[T]) StartBackgroundCleanup(ctx context.Context) {
	go tm.connManager.StartCleanupWorker(ctx, time.Minute)
}

// TransactionInfo represents information about an active transaction
type TransactionInfo struct {
	ID              string
	NestLevel       int
	CreatedAt       time.Time
	ConnectionStats ConnectionStats
}

// ExecuteInTransaction executes a function within a transaction context
func (tm *TransactionManager[T]) ExecuteInTransaction(ctx context.Context, fn TransactionFunc[T]) error {
	return tm.ExecuteInTransactionWithOptions(ctx, nil, fn)
}

// ExecuteInTransactionWithOptions executes a function with custom transaction options
func (tm *TransactionManager[T]) ExecuteInTransactionWithOptions(ctx context.Context, opts *sql.TxOptions, fn TransactionFunc[T]) error {
	// Add timeout to context if not already set
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, tm.config.DefaultTimeout)
		defer cancel()
	}

	// Start tracing span if enabled
	var span trace.Span
	if tm.tracer != nil {
		ctx, span = tm.tracer.Start(ctx, "transaction.execute",
			trace.WithAttributes(
				attribute.String("transaction.type", "managed"),
				attribute.Bool("transaction.nested", isNestedTransaction(ctx)),
				attribute.String("connection.type", string(tm.conn.Type())),
				attribute.String("connection.role", string(tm.conn.Role())),
			))
		defer func() {
			if r := recover(); r != nil {
				span.RecordError(fmt.Errorf("transaction panic: %v", r))
				span.SetStatus(codes.Error, "Transaction panicked")
				span.End()
				panic(r) // re-panic after recording
			}
			span.End()
		}()
	}

	// Execute with retry logic
	return tm.executeWithRetry(ctx, opts, fn, span)
}

// executeWithRetry implements retry logic with exponential backoff
func (tm *TransactionManager[T]) executeWithRetry(ctx context.Context, opts *sql.TxOptions, fn TransactionFunc[T], span trace.Span) error {
	var lastErr error
	backoff := tm.config.RetryBackoffBase

	for attempt := 0; attempt <= tm.config.MaxRetryAttempts; attempt++ {
		if attempt > 0 {
			// Wait with exponential backoff
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
				backoff *= 2
				if backoff > tm.config.RetryBackoffMax {
					backoff = tm.config.RetryBackoffMax
				}
			}
		}

		// Update span attributes for retry
		if span != nil {
			span.SetAttributes(
				attribute.Int("transaction.attempt", attempt+1),
				attribute.Int("transaction.max_attempts", tm.config.MaxRetryAttempts+1),
			)
			if attempt > 0 {
				span.AddEvent("transaction.retry", trace.WithAttributes(
					attribute.Int("retry.attempt", attempt),
					attribute.String("retry.backoff", backoff.String()),
				))
			}
		}

		lastErr = tm.executeSingleTransaction(ctx, opts, fn, span)
		if lastErr == nil {
			return nil
		}

		// Check if error is retryable
		if !isRetryableError(lastErr) {
			return lastErr
		}

		// Check if we should continue retrying
		if attempt >= tm.config.MaxRetryAttempts {
			break
		}
	}

	if span != nil {
		span.RecordError(lastErr)
		span.SetStatus(codes.Error, "Transaction failed after retries")
		span.SetAttributes(
			attribute.String("transaction.final_error", lastErr.Error()),
			attribute.Int("transaction.total_attempts", tm.config.MaxRetryAttempts+1),
		)
	}

	return fmt.Errorf("transaction failed after %d attempts: %w", tm.config.MaxRetryAttempts+1, lastErr)
}

// executeSingleTransaction executes a single transaction attempt
func (tm *TransactionManager[T]) executeSingleTransaction(ctx context.Context, opts *sql.TxOptions, fn TransactionFunc[T], span trace.Span) error {
	// Create child span for transaction execution
	var txSpan trace.Span
	if span != nil {
		_, txSpan = tm.tracer.Start(ctx, "transaction.execute_single",
			trace.WithAttributes(
				attribute.String("db.operation", "transaction"),
				attribute.String("db.connection_type", string(tm.conn.Type())),
			))
		defer txSpan.End()
	}

	// Generate transaction ID
	txnID := generateTransactionID()
	nestLevel := getNestingLevel(ctx)

	// Acquire scoped connection
	if txSpan != nil {
		txSpan.AddEvent("transaction.connection.acquire")
	}
	scopedConn, err := tm.connManager.AcquireConnection(ctx, txnID, nestLevel)
	if err != nil {
		if txSpan != nil {
			txSpan.RecordError(err)
			txSpan.SetStatus(codes.Error, "Failed to acquire connection")
		}
		return fmt.Errorf("failed to acquire connection: %w", err)
	}

	// Begin transaction
	if txSpan != nil {
		txSpan.AddEvent("transaction.begin")
	}
	tx, err := scopedConn.GetConnection().BeginTx(ctx, opts)
	if err != nil {
		// Release connection on failure
		tm.connManager.ReleaseConnection(ctx, txnID)
		if txSpan != nil {
			txSpan.RecordError(err)
			txSpan.SetStatus(codes.Error, "Failed to begin transaction")
		}
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Create managed transaction
	managedTx := &ManagedTransaction[T]{
		tx:         tx,
		manager:    tm,
		ctx:        ctx,
		savepoints: make(map[string]bool),
		nestLevel:  nestLevel,
		span:       txSpan, // Use the single transaction span
		scopedConn: scopedConn,
		txID:       txnID,
	}

	// Register active transaction
	tm.mu.Lock()
	tm.activeTxns[txnID] = managedTx
	tm.mu.Unlock()

	// Clean up on exit
	defer func() {
		tm.mu.Lock()
		delete(tm.activeTxns, txnID)
		tm.mu.Unlock()

		// Release scoped connection
		if err := tm.connManager.ReleaseConnection(ctx, txnID); err != nil {
			// Log error but don't fail the transaction
			if txSpan != nil {
				txSpan.AddEvent("transaction.connection.release_failed",
					trace.WithAttributes(attribute.String("error", err.Error())))
			}
		}
	}()

	// Add transaction to context
	txCtx := context.WithValue(ctx, transactionContextKey{}, managedTx)
	txCtx = context.WithValue(txCtx, nestingLevelKey{}, managedTx.nestLevel)

	// Execute pre-transaction hooks
	if txSpan != nil {
		txSpan.AddEvent("transaction.hooks.before_begin")
	}
	if err := tm.hooks.ExecuteBeforeBegin(txCtx, managedTx); err != nil {
		if txSpan != nil {
			txSpan.RecordError(err)
			txSpan.AddEvent("transaction.hooks.before_begin_failed")
		}
		tx.Rollback()
		return fmt.Errorf("before begin hook failed: %w", err)
	}

	// Execute the transaction function
	if txSpan != nil {
		txSpan.AddEvent("transaction.function.start")
	}
	txErr := fn(txCtx, managedTx)
	if txSpan != nil {
		if txErr != nil {
			txSpan.AddEvent("transaction.function.error")
		} else {
			txSpan.AddEvent("transaction.function.success")
		}
	}

	// Handle transaction completion
	if txErr != nil {
		// Execute before rollback hooks
		if txSpan != nil {
			txSpan.AddEvent("transaction.hooks.before_rollback")
		}
		if hookErr := tm.hooks.ExecuteBeforeRollback(txCtx, managedTx, txErr); hookErr != nil {
			// Log hook error but don't override original error
			if txSpan != nil {
				txSpan.RecordError(hookErr)
				txSpan.SetAttributes(attribute.String("transaction.hook_error", hookErr.Error()))
				txSpan.AddEvent("transaction.hooks.before_rollback_failed")
			}
		}

		// Rollback transaction
		if txSpan != nil {
			txSpan.AddEvent("transaction.rollback.start")
		}
		if rbErr := tx.Rollback(); rbErr != nil {
			if txSpan != nil {
				txSpan.RecordError(rbErr)
				txSpan.AddEvent("transaction.rollback.failed")
			}
			txErr = fmt.Errorf("rollback failed: %v (original error: %w)", rbErr, txErr)
		} else if txSpan != nil {
			txSpan.AddEvent("transaction.rollback.success")
		}

		// Execute after rollback hooks
		if txSpan != nil {
			txSpan.AddEvent("transaction.hooks.after_rollback")
		}
		tm.hooks.ExecuteAfterRollback(txCtx, managedTx, txErr)

		if txSpan != nil {
			txSpan.RecordError(txErr)
			txSpan.SetStatus(codes.Error, "Transaction rolled back")
			txSpan.SetAttributes(
				attribute.Bool("transaction.rolled_back", true),
				attribute.String("transaction.error", txErr.Error()),
				attribute.Int("transaction.nesting_level", managedTx.nestLevel),
			)
		}

		return txErr
	}

	// Execute before commit hooks
	if txSpan != nil {
		txSpan.AddEvent("transaction.hooks.before_commit")
	}
	if err := tm.hooks.ExecuteBeforeCommit(txCtx, managedTx); err != nil {
		// Rollback on hook failure
		if txSpan != nil {
			txSpan.RecordError(err)
			txSpan.AddEvent("transaction.hooks.before_commit_failed")
			txSpan.AddEvent("transaction.rollback.start")
		}
		tx.Rollback()
		tm.hooks.ExecuteAfterRollback(txCtx, managedTx, err)
		if txSpan != nil {
			txSpan.SetStatus(codes.Error, "Before commit hook failed")
		}
		return fmt.Errorf("before commit hook failed: %w", err)
	}

	// Commit transaction
	if txSpan != nil {
		txSpan.AddEvent("transaction.commit.start")
	}
	if err := tx.Commit(); err != nil {
		// Execute after rollback hooks on commit failure
		if txSpan != nil {
			txSpan.RecordError(err)
			txSpan.AddEvent("transaction.commit.failed")
			txSpan.AddEvent("transaction.hooks.after_rollback")
		}
		tm.hooks.ExecuteAfterRollback(txCtx, managedTx, err)
		if txSpan != nil {
			txSpan.SetStatus(codes.Error, "Transaction commit failed")
		}
		return fmt.Errorf("commit failed: %w", err)
	}

	if txSpan != nil {
		txSpan.AddEvent("transaction.commit.success")
		txSpan.AddEvent("transaction.hooks.after_commit")
	}

	// Execute after commit hooks
	tm.hooks.ExecuteAfterCommit(txCtx, managedTx)

	if txSpan != nil {
		txSpan.SetStatus(codes.Ok, "Transaction committed successfully")
		txSpan.SetAttributes(
			attribute.Bool("transaction.committed", true),
			attribute.Int("transaction.nesting_level", managedTx.nestLevel),
		)
	}

	return nil
}

// ManagedTransaction wraps a database transaction with additional management features
type ManagedTransaction[T Transactional] struct {
	tx         db.Transaction
	manager    *TransactionManager[T]
	ctx        context.Context
	savepoints map[string]bool
	nestLevel  int
	span       trace.Span
	scopedConn *ScopedConnection[T]
	txID       string
	mu         sync.RWMutex
}

// CreateSavepoint creates a named savepoint
func (mt *ManagedTransaction[T]) CreateSavepoint(ctx context.Context, name string) error {
	if !mt.manager.config.EnableSavepoints {
		return fmt.Errorf("savepoints are disabled")
	}

	// Create span for savepoint operation
	var span trace.Span
	if mt.span != nil && mt.manager.tracer != nil {
		ctx, span = mt.manager.tracer.Start(ctx, "transaction.savepoint.create",
			trace.WithAttributes(
				attribute.String("savepoint.name", name),
				attribute.Int("transaction.nesting_level", mt.nestLevel),
			))
		defer span.End()
	}

	mt.mu.Lock()
	defer mt.mu.Unlock()

	if mt.savepoints[name] {
		if span != nil {
			span.SetStatus(codes.Error, "Savepoint already exists")
		}
		return fmt.Errorf("savepoint %s already exists", name)
	}

	query := fmt.Sprintf("SAVEPOINT %s", name)
	_, err := mt.tx.Exec(ctx, query)
	if err != nil {
		if span != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "Failed to create savepoint")
		}
		return fmt.Errorf("failed to create savepoint %s: %w", name, err)
	}

	mt.savepoints[name] = true
	if span != nil {
		span.SetStatus(codes.Ok, "Savepoint created successfully")
		span.SetAttributes(attribute.Int("savepoint.total_count", len(mt.savepoints)))
	}
	return nil
}

// RollbackToSavepoint rolls back to a named savepoint
func (mt *ManagedTransaction[T]) RollbackToSavepoint(ctx context.Context, name string) error {
	if !mt.manager.config.EnableSavepoints {
		return fmt.Errorf("savepoints are disabled")
	}

	mt.mu.RLock()
	exists := mt.savepoints[name]
	mt.mu.RUnlock()

	if !exists {
		return fmt.Errorf("savepoint %s does not exist", name)
	}

	query := fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", name)
	_, err := mt.tx.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to rollback to savepoint %s: %w", name, err)
	}

	return nil
}

// ReleaseSavepoint releases a named savepoint
func (mt *ManagedTransaction[T]) ReleaseSavepoint(ctx context.Context, name string) error {
	if !mt.manager.config.EnableSavepoints {
		return fmt.Errorf("savepoints are disabled")
	}

	mt.mu.Lock()
	defer mt.mu.Unlock()

	if !mt.savepoints[name] {
		return fmt.Errorf("savepoint %s does not exist", name)
	}

	query := fmt.Sprintf("RELEASE SAVEPOINT %s", name)
	_, err := mt.tx.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to release savepoint %s: %w", name, err)
	}

	delete(mt.savepoints, name)
	return nil
}

// GetTransaction returns the underlying database transaction
func (mt *ManagedTransaction[T]) GetTransaction() db.Transaction {
	return mt.tx
}

// GetContext returns the transaction context
func (mt *ManagedTransaction[T]) GetContext() context.Context {
	return mt.ctx
}

// GetNestLevel returns the nesting level of this transaction
func (mt *ManagedTransaction[T]) GetNestLevel() int {
	return mt.nestLevel
}

// GetTransactionID returns the transaction ID
func (mt *ManagedTransaction[T]) GetTransactionID() string {
	return mt.txID
}

// GetScopedConnection returns the scoped connection for this transaction
func (mt *ManagedTransaction[T]) GetScopedConnection() *ScopedConnection[T] {
	return mt.scopedConn
}

// GetConnectionStats returns statistics for this transaction's connection
func (mt *ManagedTransaction[T]) GetConnectionStats() ConnectionStats {
	if mt.scopedConn != nil {
		return mt.scopedConn.GetStats()
	}
	return ConnectionStats{}
}

// TransactionFunc represents a function that executes within a transaction
type TransactionFunc[T Transactional] func(ctx context.Context, tx *ManagedTransaction[T]) error

// Context keys for transaction management
type transactionContextKey struct{}
type nestingLevelKey struct{}

// GetTransactionFromContext retrieves the current transaction from context
func GetTransactionFromContext[T Transactional](ctx context.Context) (*ManagedTransaction[T], bool) {
	tx, ok := ctx.Value(transactionContextKey{}).(*ManagedTransaction[T])
	return tx, ok
}

// Helper functions
func isNestedTransaction(ctx context.Context) bool {
	val := ctx.Value(transactionContextKey{})
	return val != nil
}

func getNestingLevel(ctx context.Context) int {
	if level := ctx.Value(nestingLevelKey{}); level != nil {
		if lvl, ok := level.(int); ok {
			return lvl + 1
		}
	}
	return 0
}

func generateTransactionID() string {
	return fmt.Sprintf("txn_%d", time.Now().UnixNano())
}

func isRetryableError(err error) bool {
	// Check for common retryable database errors
	// This is database-specific and could be expanded
	if err == nil {
		return false
	}

	errorStr := err.Error()
	retryablePatterns := []string{
		"connection reset",
		"connection refused",
		"timeout",
		"deadlock",
		"serialization failure",
		"could not serialize access",
	}

	for _, pattern := range retryablePatterns {
		if containsIgnoreCase(errorStr, pattern) {
			return true
		}
	}

	return false
}

func containsIgnoreCase(s, substr string) bool {
	return len(s) >= len(substr) &&
		   len(s) > 0 &&
		   len(substr) > 0 &&
		   (s == substr ||
			strings.Contains(strings.ToLower(s), strings.ToLower(substr)))
}