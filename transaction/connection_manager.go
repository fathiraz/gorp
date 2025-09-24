package transaction

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-gorp/gorp/v3/db"
)

// TransactionScopedConnectionManager manages connections within transaction scopes
type TransactionScopedConnectionManager[T Transactional] struct {
	baseConn      db.Connection
	tracer        trace.Tracer
	config        *ConnectionManagerConfig
	mu            sync.RWMutex
	txConnections map[string]*ScopedConnection[T]
}

// ConnectionManagerConfig holds configuration for transaction-scoped connection management
type ConnectionManagerConfig struct {
	// MaxTxConnections limits the number of concurrent transaction connections
	MaxTxConnections int
	// ConnectionTimeout for acquiring connections
	ConnectionTimeout time.Duration
	// IdleTimeout for releasing idle connections
	IdleTimeout time.Duration
	// EnableConnectionReuse allows reusing connections across transactions
	EnableConnectionReuse bool
	// EnableTracing for connection operations
	EnableTracing bool
}

// DefaultConnectionManagerConfig returns sensible defaults
func DefaultConnectionManagerConfig() *ConnectionManagerConfig {
	return &ConnectionManagerConfig{
		MaxTxConnections:      100,
		ConnectionTimeout:     5 * time.Second,
		IdleTimeout:          30 * time.Second,
		EnableConnectionReuse: true,
		EnableTracing:        true,
	}
}

// ScopedConnection wraps a connection with transaction scope information
type ScopedConnection[T Transactional] struct {
	conn         db.Connection
	txID         string
	nestLevel    int
	createdAt    time.Time
	lastUsed     time.Time
	isExclusive  bool
	refCount     int
	mu           sync.RWMutex
}

// NewTransactionScopedConnectionManager creates a new connection manager
func NewTransactionScopedConnectionManager[T Transactional](
	baseConn db.Connection,
	tracer trace.Tracer,
	config *ConnectionManagerConfig,
) *TransactionScopedConnectionManager[T] {
	if config == nil {
		config = DefaultConnectionManagerConfig()
	}

	return &TransactionScopedConnectionManager[T]{
		baseConn:      baseConn,
		tracer:        tracer,
		config:        config,
		txConnections: make(map[string]*ScopedConnection[T]),
	}
}

// AcquireConnection acquires a connection for a transaction scope
func (cm *TransactionScopedConnectionManager[T]) AcquireConnection(
	ctx context.Context,
	txID string,
	nestLevel int,
) (*ScopedConnection[T], error) {
	// Create span for connection acquisition
	var span trace.Span
	if cm.tracer != nil && cm.config.EnableTracing {
		ctx, span = cm.tracer.Start(ctx, "transaction.connection.acquire",
			trace.WithAttributes(
				attribute.String("transaction.id", txID),
				attribute.Int("transaction.nesting_level", nestLevel),
			))
		defer span.End()
	}

	// Add timeout to context
	acquireCtx, cancel := context.WithTimeout(ctx, cm.config.ConnectionTimeout)
	defer cancel()

	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Check if connection already exists for this transaction
	if existing, exists := cm.txConnections[txID]; exists {
		existing.mu.Lock()
		existing.refCount++
		existing.lastUsed = time.Now()
		existing.mu.Unlock()

		if span != nil {
			span.SetAttributes(
				attribute.Bool("connection.reused", true),
				attribute.Int("connection.ref_count", existing.refCount),
			)
			span.SetStatus(codes.Ok, "Connection reused")
		}
		return existing, nil
	}

	// Check connection limit
	if len(cm.txConnections) >= cm.config.MaxTxConnections {
		err := fmt.Errorf("maximum transaction connections reached: %d", cm.config.MaxTxConnections)
		if span != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "Connection limit reached")
		}
		return nil, err
	}

	// Create new scoped connection
	scopedConn := &ScopedConnection[T]{
		conn:        cm.baseConn, // In a real implementation, this might be a new connection from a pool
		txID:        txID,
		nestLevel:   nestLevel,
		createdAt:   time.Now(),
		lastUsed:    time.Now(),
		isExclusive: nestLevel == 0, // Only top-level transactions are exclusive
		refCount:    1,
	}

	// Register the connection
	cm.txConnections[txID] = scopedConn

	if span != nil {
		span.SetAttributes(
			attribute.Bool("connection.reused", false),
			attribute.Bool("connection.exclusive", scopedConn.isExclusive),
			attribute.String("connection.type", string(cm.baseConn.Type())),
			attribute.Int("connection.total_active", len(cm.txConnections)),
		)
		span.SetStatus(codes.Ok, "Connection acquired")
	}

	// Handle context cancellation
	go func() {
		<-acquireCtx.Done()
		if acquireCtx.Err() == context.DeadlineExceeded {
			// Log timeout but don't release connection immediately
			// as it might still be in use
		}
	}()

	return scopedConn, nil
}

// ReleaseConnection releases a connection from transaction scope
func (cm *TransactionScopedConnectionManager[T]) ReleaseConnection(
	ctx context.Context,
	txID string,
) error {
	// Create span for connection release
	var span trace.Span
	if cm.tracer != nil && cm.config.EnableTracing {
		ctx, span = cm.tracer.Start(ctx, "transaction.connection.release",
			trace.WithAttributes(
				attribute.String("transaction.id", txID),
			))
		defer span.End()
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	scopedConn, exists := cm.txConnections[txID]
	if !exists {
		err := fmt.Errorf("no connection found for transaction: %s", txID)
		if span != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "Connection not found")
		}
		return err
	}

	scopedConn.mu.Lock()
	scopedConn.refCount--
	refCount := scopedConn.refCount
	scopedConn.mu.Unlock()

	if span != nil {
		span.SetAttributes(
			attribute.Int("connection.ref_count", refCount),
			attribute.Bool("connection.will_close", refCount <= 0),
		)
	}

	// Only remove connection when reference count reaches zero
	if refCount <= 0 {
		delete(cm.txConnections, txID)

		// In a real implementation, we might return the connection to a pool
		// For now, we just mark it as released
		if span != nil {
			span.SetStatus(codes.Ok, "Connection released")
		}
	}

	return nil
}

// GetConnection returns the underlying connection for a scoped connection
func (sc *ScopedConnection[T]) GetConnection() db.Connection {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.conn
}

// GetTransactionID returns the transaction ID for this connection
func (sc *ScopedConnection[T]) GetTransactionID() string {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.txID
}

// GetNestLevel returns the nesting level for this connection
func (sc *ScopedConnection[T]) GetNestLevel() int {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.nestLevel
}

// IsExclusive returns whether this connection is exclusive to its transaction
func (sc *ScopedConnection[T]) IsExclusive() bool {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.isExclusive
}

// UpdateLastUsed updates the last used timestamp
func (sc *ScopedConnection[T]) UpdateLastUsed() {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.lastUsed = time.Now()
}

// GetStats returns connection statistics
func (sc *ScopedConnection[T]) GetStats() ConnectionStats {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	return ConnectionStats{
		TransactionID: sc.txID,
		NestLevel:     sc.nestLevel,
		CreatedAt:     sc.createdAt,
		LastUsed:      sc.lastUsed,
		RefCount:      sc.refCount,
		IsExclusive:   sc.isExclusive,
		Age:           time.Since(sc.createdAt),
		IdleTime:      time.Since(sc.lastUsed),
	}
}

// ConnectionStats represents connection statistics
type ConnectionStats struct {
	TransactionID string
	NestLevel     int
	CreatedAt     time.Time
	LastUsed      time.Time
	RefCount      int
	IsExclusive   bool
	Age           time.Duration
	IdleTime      time.Duration
}

// GetActiveConnections returns statistics for all active connections
func (cm *TransactionScopedConnectionManager[T]) GetActiveConnections() []ConnectionStats {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	stats := make([]ConnectionStats, 0, len(cm.txConnections))
	for _, conn := range cm.txConnections {
		stats = append(stats, conn.GetStats())
	}

	return stats
}

// CleanupIdleConnections removes connections that have been idle too long
func (cm *TransactionScopedConnectionManager[T]) CleanupIdleConnections(ctx context.Context) int {
	var span trace.Span
	if cm.tracer != nil && cm.config.EnableTracing {
		ctx, span = cm.tracer.Start(ctx, "transaction.connection.cleanup")
		defer span.End()
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	var cleaned int
	cutoff := time.Now().Add(-cm.config.IdleTimeout)

	for txID, conn := range cm.txConnections {
		conn.mu.RLock()
		isIdle := conn.lastUsed.Before(cutoff) && conn.refCount <= 0
		conn.mu.RUnlock()

		if isIdle {
			delete(cm.txConnections, txID)
			cleaned++
		}
	}

	if span != nil {
		span.SetAttributes(
			attribute.Int("connections.cleaned", cleaned),
			attribute.Int("connections.remaining", len(cm.txConnections)),
		)
		span.SetStatus(codes.Ok, "Cleanup completed")
	}

	return cleaned
}

// GetManagerStats returns overall connection manager statistics
func (cm *TransactionScopedConnectionManager[T]) GetManagerStats() ManagerStats {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	stats := ManagerStats{
		MaxConnections:    cm.config.MaxTxConnections,
		ActiveConnections: len(cm.txConnections),
		ConnectionTimeout: cm.config.ConnectionTimeout,
		IdleTimeout:      cm.config.IdleTimeout,
		ReuseEnabled:     cm.config.EnableConnectionReuse,
	}

	return stats
}

// ManagerStats represents connection manager statistics
type ManagerStats struct {
	MaxConnections    int
	ActiveConnections int
	ConnectionTimeout time.Duration
	IdleTimeout      time.Duration
	ReuseEnabled     bool
}

// StartCleanupWorker starts a background worker to clean up idle connections
func (cm *TransactionScopedConnectionManager[T]) StartCleanupWorker(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = cm.config.IdleTimeout / 2 // Default to half the idle timeout
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cm.CleanupIdleConnections(ctx)
		}
	}
}