package transaction

import (
	"context"
	"fmt"
	"sync"
)

// TransactionHooks manages transaction lifecycle hooks
type TransactionHooks[T Transactional] struct {
	beforeBegin    []BeforeBeginHook[T]
	beforeCommit   []BeforeCommitHook[T]
	afterCommit    []AfterCommitHook[T]
	beforeRollback []BeforeRollbackHook[T]
	afterRollback  []AfterRollbackHook[T]
	mu             sync.RWMutex
}

// Hook function types
type BeforeBeginHook[T Transactional] func(ctx context.Context, tx *ManagedTransaction[T]) error
type BeforeCommitHook[T Transactional] func(ctx context.Context, tx *ManagedTransaction[T]) error
type AfterCommitHook[T Transactional] func(ctx context.Context, tx *ManagedTransaction[T])
type BeforeRollbackHook[T Transactional] func(ctx context.Context, tx *ManagedTransaction[T], err error) error
type AfterRollbackHook[T Transactional] func(ctx context.Context, tx *ManagedTransaction[T], err error)

// NewTransactionHooks creates a new transaction hooks manager
func NewTransactionHooks[T Transactional]() *TransactionHooks[T] {
	return &TransactionHooks[T]{
		beforeBegin:    make([]BeforeBeginHook[T], 0),
		beforeCommit:   make([]BeforeCommitHook[T], 0),
		afterCommit:    make([]AfterCommitHook[T], 0),
		beforeRollback: make([]BeforeRollbackHook[T], 0),
		afterRollback:  make([]AfterRollbackHook[T], 0),
	}
}

// AddBeforeBeginHook adds a hook that executes before transaction begins
func (th *TransactionHooks[T]) AddBeforeBeginHook(hook BeforeBeginHook[T]) {
	th.mu.Lock()
	defer th.mu.Unlock()
	th.beforeBegin = append(th.beforeBegin, hook)
}

// AddBeforeCommitHook adds a hook that executes before transaction commits
func (th *TransactionHooks[T]) AddBeforeCommitHook(hook BeforeCommitHook[T]) {
	th.mu.Lock()
	defer th.mu.Unlock()
	th.beforeCommit = append(th.beforeCommit, hook)
}

// AddAfterCommitHook adds a hook that executes after transaction commits
func (th *TransactionHooks[T]) AddAfterCommitHook(hook AfterCommitHook[T]) {
	th.mu.Lock()
	defer th.mu.Unlock()
	th.afterCommit = append(th.afterCommit, hook)
}

// AddBeforeRollbackHook adds a hook that executes before transaction rolls back
func (th *TransactionHooks[T]) AddBeforeRollbackHook(hook BeforeRollbackHook[T]) {
	th.mu.Lock()
	defer th.mu.Unlock()
	th.beforeRollback = append(th.beforeRollback, hook)
}

// AddAfterRollbackHook adds a hook that executes after transaction rolls back
func (th *TransactionHooks[T]) AddAfterRollbackHook(hook AfterRollbackHook[T]) {
	th.mu.Lock()
	defer th.mu.Unlock()
	th.afterRollback = append(th.afterRollback, hook)
}

// ExecuteBeforeBegin executes all before begin hooks
func (th *TransactionHooks[T]) ExecuteBeforeBegin(ctx context.Context, tx *ManagedTransaction[T]) error {
	th.mu.RLock()
	hooks := make([]BeforeBeginHook[T], len(th.beforeBegin))
	copy(hooks, th.beforeBegin)
	th.mu.RUnlock()

	for i, hook := range hooks {
		if err := hook(ctx, tx); err != nil {
			return fmt.Errorf("before begin hook %d failed: %w", i, err)
		}
	}
	return nil
}

// ExecuteBeforeCommit executes all before commit hooks
func (th *TransactionHooks[T]) ExecuteBeforeCommit(ctx context.Context, tx *ManagedTransaction[T]) error {
	th.mu.RLock()
	hooks := make([]BeforeCommitHook[T], len(th.beforeCommit))
	copy(hooks, th.beforeCommit)
	th.mu.RUnlock()

	for i, hook := range hooks {
		if err := hook(ctx, tx); err != nil {
			return fmt.Errorf("before commit hook %d failed: %w", i, err)
		}
	}
	return nil
}

// ExecuteAfterCommit executes all after commit hooks
func (th *TransactionHooks[T]) ExecuteAfterCommit(ctx context.Context, tx *ManagedTransaction[T]) {
	th.mu.RLock()
	hooks := make([]AfterCommitHook[T], len(th.afterCommit))
	copy(hooks, th.afterCommit)
	th.mu.RUnlock()

	for _, hook := range hooks {
		// After commit hooks should not fail the transaction
		// They run in a "fire and forget" manner
		func() {
			defer func() {
				if r := recover(); r != nil {
					// Log the panic but don't propagate it
					// In a production system, this would use a proper logger
				}
			}()
			hook(ctx, tx)
		}()
	}
}

// ExecuteBeforeRollback executes all before rollback hooks
func (th *TransactionHooks[T]) ExecuteBeforeRollback(ctx context.Context, tx *ManagedTransaction[T], originalErr error) error {
	th.mu.RLock()
	hooks := make([]BeforeRollbackHook[T], len(th.beforeRollback))
	copy(hooks, th.beforeRollback)
	th.mu.RUnlock()

	for i, hook := range hooks {
		if err := hook(ctx, tx, originalErr); err != nil {
			return fmt.Errorf("before rollback hook %d failed: %w", i, err)
		}
	}
	return nil
}

// ExecuteAfterRollback executes all after rollback hooks
func (th *TransactionHooks[T]) ExecuteAfterRollback(ctx context.Context, tx *ManagedTransaction[T], originalErr error) {
	th.mu.RLock()
	hooks := make([]AfterRollbackHook[T], len(th.afterRollback))
	copy(hooks, th.afterRollback)
	th.mu.RUnlock()

	for _, hook := range hooks {
		// After rollback hooks should not fail
		func() {
			defer func() {
				if r := recover(); r != nil {
					// Log the panic but don't propagate it
				}
			}()
			hook(ctx, tx, originalErr)
		}()
	}
}

// ClearHooks removes all registered hooks
func (th *TransactionHooks[T]) ClearHooks() {
	th.mu.Lock()
	defer th.mu.Unlock()

	th.beforeBegin = th.beforeBegin[:0]
	th.beforeCommit = th.beforeCommit[:0]
	th.afterCommit = th.afterCommit[:0]
	th.beforeRollback = th.beforeRollback[:0]
	th.afterRollback = th.afterRollback[:0]
}

// GetHookCounts returns the number of registered hooks of each type
func (th *TransactionHooks[T]) GetHookCounts() HookCounts {
	th.mu.RLock()
	defer th.mu.RUnlock()

	return HookCounts{
		BeforeBegin:    len(th.beforeBegin),
		BeforeCommit:   len(th.beforeCommit),
		AfterCommit:    len(th.afterCommit),
		BeforeRollback: len(th.beforeRollback),
		AfterRollback:  len(th.afterRollback),
	}
}

// HookCounts represents the count of each hook type
type HookCounts struct {
	BeforeBegin    int
	BeforeCommit   int
	AfterCommit    int
	BeforeRollback int
	AfterRollback  int
}

// Common hook implementations

// LoggingHooks provides common logging hooks
type LoggingHooks[T Transactional] struct {
	logger Logger
}

// Logger interface for hook logging
type Logger interface {
	Debug(msg string, args ...interface{})
	Info(msg string, args ...interface{})
	Warn(msg string, args ...interface{})
	Error(msg string, args ...interface{})
}

// NewLoggingHooks creates logging hooks
func NewLoggingHooks[T Transactional](logger Logger) *LoggingHooks[T] {
	return &LoggingHooks[T]{logger: logger}
}

// BeforeBegin logs transaction start
func (lh *LoggingHooks[T]) BeforeBegin(ctx context.Context, tx *ManagedTransaction[T]) error {
	lh.logger.Debug("Transaction starting at nesting level %d", tx.GetNestLevel())
	return nil
}

// BeforeCommit logs transaction commit
func (lh *LoggingHooks[T]) BeforeCommit(ctx context.Context, tx *ManagedTransaction[T]) error {
	lh.logger.Debug("Transaction committing at nesting level %d", tx.GetNestLevel())
	return nil
}

// AfterCommit logs successful commit
func (lh *LoggingHooks[T]) AfterCommit(ctx context.Context, tx *ManagedTransaction[T]) {
	lh.logger.Debug("Transaction committed successfully at nesting level %d", tx.GetNestLevel())
}

// BeforeRollback logs transaction rollback
func (lh *LoggingHooks[T]) BeforeRollback(ctx context.Context, tx *ManagedTransaction[T], err error) error {
	lh.logger.Warn("Transaction rolling back at nesting level %d due to error: %v", tx.GetNestLevel(), err)
	return nil
}

// AfterRollback logs completed rollback
func (lh *LoggingHooks[T]) AfterRollback(ctx context.Context, tx *ManagedTransaction[T], err error) {
	lh.logger.Debug("Transaction rolled back at nesting level %d", tx.GetNestLevel())
}

// MetricsHooks provides hooks for collecting transaction metrics
type MetricsHooks[T Transactional] struct {
	registry MetricsRegistry
}

// MetricsRegistry interface for collecting metrics
type MetricsRegistry interface {
	IncrementCounter(name string, tags map[string]string)
	RecordDuration(name string, duration float64, tags map[string]string)
	SetGauge(name string, value float64, tags map[string]string)
}

// NewMetricsHooks creates metrics collection hooks
func NewMetricsHooks[T Transactional](registry MetricsRegistry) *MetricsHooks[T] {
	return &MetricsHooks[T]{registry: registry}
}

// BeforeBegin records transaction start
func (mh *MetricsHooks[T]) BeforeBegin(ctx context.Context, tx *ManagedTransaction[T]) error {
	tags := map[string]string{
		"nesting_level": fmt.Sprintf("%d", tx.GetNestLevel()),
		"nested":        fmt.Sprintf("%t", tx.GetNestLevel() > 0),
	}
	mh.registry.IncrementCounter("transaction.started", tags)
	return nil
}

// AfterCommit records successful commits
func (mh *MetricsHooks[T]) AfterCommit(ctx context.Context, tx *ManagedTransaction[T]) {
	tags := map[string]string{
		"nesting_level": fmt.Sprintf("%d", tx.GetNestLevel()),
		"result":        "committed",
	}
	mh.registry.IncrementCounter("transaction.completed", tags)
}

// AfterRollback records rollbacks
func (mh *MetricsHooks[T]) AfterRollback(ctx context.Context, tx *ManagedTransaction[T], err error) {
	tags := map[string]string{
		"nesting_level": fmt.Sprintf("%d", tx.GetNestLevel()),
		"result":        "rolled_back",
	}
	mh.registry.IncrementCounter("transaction.completed", tags)
}

// ValidationHooks provides hooks for transaction validation
type ValidationHooks[T Transactional] struct {
	validators []TransactionValidator[T]
}

// TransactionValidator validates transaction state
type TransactionValidator[T Transactional] func(ctx context.Context, tx *ManagedTransaction[T]) error

// NewValidationHooks creates validation hooks
func NewValidationHooks[T Transactional](validators ...TransactionValidator[T]) *ValidationHooks[T] {
	return &ValidationHooks[T]{validators: validators}
}

// BeforeCommit validates transaction before commit
func (vh *ValidationHooks[T]) BeforeCommit(ctx context.Context, tx *ManagedTransaction[T]) error {
	for i, validator := range vh.validators {
		if err := validator(ctx, tx); err != nil {
			return fmt.Errorf("transaction validation %d failed: %w", i, err)
		}
	}
	return nil
}

// AddValidator adds a new validator
func (vh *ValidationHooks[T]) AddValidator(validator TransactionValidator[T]) {
	vh.validators = append(vh.validators, validator)
}