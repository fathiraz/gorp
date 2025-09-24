package transaction

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"

	"github.com/go-gorp/gorp/v3/db"
)

// TestEntity for testing
type TestEntity struct{}

// Mock implementations
type MockConnection struct {
	mock.Mock
}

func (m *MockConnection) Begin(ctx context.Context) (db.Transaction, error) {
	args := m.Called(ctx)
	return args.Get(0).(db.Transaction), args.Error(1)
}

func (m *MockConnection) BeginTx(ctx context.Context, opts *sql.TxOptions) (db.Transaction, error) {
	args := m.Called(ctx, opts)
	return args.Get(0).(db.Transaction), args.Error(1)
}

func (m *MockConnection) Query(ctx context.Context, query string, args ...interface{}) (db.Rows, error) {
	mockArgs := m.Called(ctx, query, args)
	return mockArgs.Get(0).(db.Rows), mockArgs.Error(1)
}

func (m *MockConnection) QueryRow(ctx context.Context, query string, args ...interface{}) db.Row {
	mockArgs := m.Called(ctx, query, args)
	return mockArgs.Get(0).(db.Row)
}

func (m *MockConnection) Exec(ctx context.Context, query string, args ...interface{}) (db.Result, error) {
	mockArgs := m.Called(ctx, query, args)
	return mockArgs.Get(0).(db.Result), mockArgs.Error(1)
}

func (m *MockConnection) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockConnection) Type() db.ConnectionType {
	args := m.Called()
	return args.Get(0).(db.ConnectionType)
}

func (m *MockConnection) Role() db.ConnectionRole {
	args := m.Called()
	return args.Get(0).(db.ConnectionRole)
}

func (m *MockConnection) Stats() db.ConnectionStats {
	args := m.Called()
	return args.Get(0).(db.ConnectionStats)
}

func (m *MockConnection) Select(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	mockArgs := m.Called(ctx, dest, query, args)
	return mockArgs.Error(0)
}

func (m *MockConnection) Get(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	mockArgs := m.Called(ctx, dest, query, args)
	return mockArgs.Error(0)
}

func (m *MockConnection) NamedExec(ctx context.Context, query string, arg interface{}) (db.Result, error) {
	mockArgs := m.Called(ctx, query, arg)
	return mockArgs.Get(0).(db.Result), mockArgs.Error(1)
}

func (m *MockConnection) NamedQuery(ctx context.Context, query string, arg interface{}) (db.Rows, error) {
	mockArgs := m.Called(ctx, query, arg)
	return mockArgs.Get(0).(db.Rows), mockArgs.Error(1)
}

func (m *MockConnection) IsHealthy() bool {
	args := m.Called()
	return args.Bool(0)
}

func (m *MockConnection) Ping(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

type MockTransaction struct {
	mock.Mock
}

func (m *MockTransaction) Query(ctx context.Context, query string, args ...interface{}) (db.Rows, error) {
	mockArgs := m.Called(ctx, query, args)
	return mockArgs.Get(0).(db.Rows), mockArgs.Error(1)
}

func (m *MockTransaction) QueryRow(ctx context.Context, query string, args ...interface{}) db.Row {
	mockArgs := m.Called(ctx, query, args)
	return mockArgs.Get(0).(db.Row)
}

func (m *MockTransaction) Exec(ctx context.Context, query string, args ...interface{}) (db.Result, error) {
	mockArgs := m.Called(ctx, query, args)
	return mockArgs.Get(0).(db.Result), mockArgs.Error(1)
}

func (m *MockTransaction) Commit() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockTransaction) Rollback() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockTransaction) Select(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	mockArgs := m.Called(ctx, dest, query, args)
	return mockArgs.Error(0)
}

func (m *MockTransaction) Get(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	mockArgs := m.Called(ctx, dest, query, args)
	return mockArgs.Error(0)
}

func (m *MockTransaction) NamedExec(ctx context.Context, query string, arg interface{}) (db.Result, error) {
	mockArgs := m.Called(ctx, query, arg)
	return mockArgs.Get(0).(db.Result), mockArgs.Error(1)
}

func (m *MockTransaction) NamedQuery(ctx context.Context, query string, arg interface{}) (db.Rows, error) {
	mockArgs := m.Called(ctx, query, arg)
	return mockArgs.Get(0).(db.Rows), mockArgs.Error(1)
}

// Test TransactionManager creation and configuration
func TestNewTransactionManager(t *testing.T) {
	mockConn := &MockConnection{}
	mockConn.On("Type").Return(db.PostgreSQLConnectionType)

	t.Run("WithDefaultConfig", func(t *testing.T) {
		tm := NewTransactionManager[TestEntity](mockConn, nil)

		require.NotNil(t, tm)
		assert.NotNil(t, tm.config)
		assert.NotNil(t, tm.hooks)
		assert.NotNil(t, tm.connManager)
		assert.Equal(t, 30*time.Second, tm.config.DefaultTimeout)
		assert.Equal(t, 3, tm.config.MaxRetryAttempts)
		assert.True(t, tm.config.EnableSavepoints)
		assert.True(t, tm.config.EnableTracing)
	})

	t.Run("WithCustomConfig", func(t *testing.T) {
		config := &TransactionConfig{
			DefaultTimeout:    10 * time.Second,
			MaxRetryAttempts:  5,
			EnableSavepoints:  false,
			EnableTracing:     false,
		}

		tm := NewTransactionManager[TestEntity](mockConn, config)

		require.NotNil(t, tm)
		assert.Equal(t, 10*time.Second, tm.config.DefaultTimeout)
		assert.Equal(t, 5, tm.config.MaxRetryAttempts)
		assert.False(t, tm.config.EnableSavepoints)
		assert.False(t, tm.config.EnableTracing)
		assert.Nil(t, tm.tracer)
	})

	t.Run("WithTracing", func(t *testing.T) {
		config := &TransactionConfig{
			EnableTracing: true,
			TracerName:   "test-tracer",
		}

		tm := NewTransactionManager[TestEntity](mockConn, config)

		require.NotNil(t, tm)
		assert.NotNil(t, tm.tracer)
	})
}

// Test successful transaction execution
func TestTransactionManager_ExecuteInTransaction_Success(t *testing.T) {
	mockConn := &MockConnection{}
	mockTx := &MockTransaction{}

	mockConn.On("Type").Return(db.PostgreSQLConnectionType)
	mockConn.On("BeginTx", mock.Anything, (*sql.TxOptions)(nil)).Return(mockTx, nil)
	mockTx.On("Commit").Return(nil)

	tm := NewTransactionManager[TestEntity](mockConn, nil)

	ctx := context.Background()
	executed := false

	err := tm.ExecuteInTransaction(ctx, func(ctx context.Context, tx *ManagedTransaction[TestEntity]) error {
		executed = true
		assert.NotNil(t, tx)
		assert.Equal(t, 0, tx.GetNestLevel())
		assert.NotEmpty(t, tx.GetTransactionID())
		return nil
	})

	assert.NoError(t, err)
	assert.True(t, executed)
	mockConn.AssertExpectations(t)
	mockTx.AssertExpectations(t)
}

// Test transaction rollback on error
func TestTransactionManager_ExecuteInTransaction_Rollback(t *testing.T) {
	mockConn := &MockConnection{}
	mockTx := &MockTransaction{}

	mockConn.On("Type").Return(db.PostgreSQLConnectionType)
	mockConn.On("BeginTx", mock.Anything, (*sql.TxOptions)(nil)).Return(mockTx, nil)
	mockTx.On("Rollback").Return(nil)

	tm := NewTransactionManager[TestEntity](mockConn, nil)

	ctx := context.Background()
	testError := errors.New("test error")

	err := tm.ExecuteInTransaction(ctx, func(ctx context.Context, tx *ManagedTransaction[TestEntity]) error {
		return testError
	})

	assert.Error(t, err)
	assert.Equal(t, testError, err)
	mockConn.AssertExpectations(t)
	mockTx.AssertExpectations(t)
}

// Test transaction hooks
func TestTransactionManager_Hooks(t *testing.T) {
	mockConn := &MockConnection{}
	mockTx := &MockTransaction{}

	mockConn.On("Type").Return(db.PostgreSQLConnectionType)
	mockConn.On("BeginTx", mock.Anything, (*sql.TxOptions)(nil)).Return(mockTx, nil)
	mockTx.On("Commit").Return(nil)

	tm := NewTransactionManager[TestEntity](mockConn, nil)

	// Track hook execution
	var hookOrder []string
	var hookMutex sync.Mutex

	addHookExecution := func(name string) {
		hookMutex.Lock()
		defer hookMutex.Unlock()
		hookOrder = append(hookOrder, name)
	}

	// Add hooks
	tm.GetHooks().AddBeforeBeginHook(func(ctx context.Context, tx *ManagedTransaction[TestEntity]) error {
		addHookExecution("before_begin")
		return nil
	})

	tm.GetHooks().AddBeforeCommitHook(func(ctx context.Context, tx *ManagedTransaction[TestEntity]) error {
		addHookExecution("before_commit")
		return nil
	})

	tm.GetHooks().AddAfterCommitHook(func(ctx context.Context, tx *ManagedTransaction[TestEntity]) {
		addHookExecution("after_commit")
	})

	// Execute transaction
	ctx := context.Background()
	err := tm.ExecuteInTransaction(ctx, func(ctx context.Context, tx *ManagedTransaction[TestEntity]) error {
		addHookExecution("transaction_function")
		return nil
	})

	assert.NoError(t, err)
	assert.Equal(t, []string{"before_begin", "transaction_function", "before_commit", "after_commit"}, hookOrder)

	mockConn.AssertExpectations(t)
	mockTx.AssertExpectations(t)
}

// Test savepoints
func TestManagedTransaction_Savepoints(t *testing.T) {
	mockConn := &MockConnection{}
	mockTx := &MockTransaction{}

	mockConn.On("Type").Return(db.PostgreSQLConnectionType)
	mockConn.On("BeginTx", mock.Anything, (*sql.TxOptions)(nil)).Return(mockTx, nil)
	mockTx.On("Commit").Return(nil)

	// Mock savepoint operations
	mockTx.On("Exec", mock.Anything, "SAVEPOINT test_savepoint").Return(nil, nil)
	mockTx.On("Exec", mock.Anything, "ROLLBACK TO SAVEPOINT test_savepoint").Return(nil, nil)
	mockTx.On("Exec", mock.Anything, "RELEASE SAVEPOINT test_savepoint").Return(nil, nil)

	tm := NewTransactionManager[TestEntity](mockConn, nil)

	ctx := context.Background()
	err := tm.ExecuteInTransaction(ctx, func(ctx context.Context, tx *ManagedTransaction[TestEntity]) error {
		// Create savepoint
		err := tx.CreateSavepoint(ctx, "test_savepoint")
		assert.NoError(t, err)

		// Rollback to savepoint
		err = tx.RollbackToSavepoint(ctx, "test_savepoint")
		assert.NoError(t, err)

		// Release savepoint
		err = tx.ReleaseSavepoint(ctx, "test_savepoint")
		assert.NoError(t, err)

		return nil
	})

	assert.NoError(t, err)
	mockConn.AssertExpectations(t)
	mockTx.AssertExpectations(t)
}

// Test retry logic
func TestTransactionManager_RetryLogic(t *testing.T) {
	mockConn := &MockConnection{}
	mockTx := &MockTransaction{}

	mockConn.On("Type").Return(db.PostgreSQLConnectionType)

	// First two attempts fail, third succeeds
	mockConn.On("BeginTx", mock.Anything, (*sql.TxOptions)(nil)).Return(mockTx, nil).Times(3)
	mockTx.On("Rollback").Return(nil).Times(2)
	mockTx.On("Commit").Return(nil).Once()

	config := &TransactionConfig{
		MaxRetryAttempts: 3,
		RetryBackoffBase: time.Millisecond, // Fast for testing
	}

	tm := NewTransactionManager[TestEntity](mockConn, config)

	ctx := context.Background()
	attemptCount := 0

	err := tm.ExecuteInTransaction(ctx, func(ctx context.Context, tx *ManagedTransaction[TestEntity]) error {
		attemptCount++
		if attemptCount < 3 {
			return errors.New("connection reset") // Retryable error
		}
		return nil
	})

	assert.NoError(t, err)
	assert.Equal(t, 3, attemptCount)
	mockConn.AssertExpectations(t)
	mockTx.AssertExpectations(t)
}

// Test connection manager
func TestTransactionScopedConnectionManager(t *testing.T) {
	mockConn := &MockConnection{}
	mockConn.On("Type").Return(db.PostgreSQLConnectionType)

	config := DefaultConnectionManagerConfig()
	config.MaxTxConnections = 2

	tracer := otel.Tracer("test")
	cm := NewTransactionScopedConnectionManager[TestEntity](mockConn, tracer, config)

	ctx := context.Background()

	t.Run("AcquireAndReleaseConnection", func(t *testing.T) {
		// Acquire connection
		scopedConn, err := cm.AcquireConnection(ctx, "tx1", 0)
		require.NoError(t, err)
		require.NotNil(t, scopedConn)

		assert.Equal(t, "tx1", scopedConn.GetTransactionID())
		assert.Equal(t, 0, scopedConn.GetNestLevel())
		assert.True(t, scopedConn.IsExclusive())

		stats := scopedConn.GetStats()
		assert.Equal(t, "tx1", stats.TransactionID)
		assert.Equal(t, 1, stats.RefCount)

		// Release connection
		err = cm.ReleaseConnection(ctx, "tx1")
		assert.NoError(t, err)
	})

	t.Run("ConnectionReuse", func(t *testing.T) {
		// Acquire same connection twice
		scopedConn1, err := cm.AcquireConnection(ctx, "tx2", 0)
		require.NoError(t, err)

		scopedConn2, err := cm.AcquireConnection(ctx, "tx2", 0)
		require.NoError(t, err)

		// Should be the same connection
		assert.Equal(t, scopedConn1, scopedConn2)

		stats := scopedConn1.GetStats()
		assert.Equal(t, 2, stats.RefCount)

		// Release connections
		err = cm.ReleaseConnection(ctx, "tx2")
		assert.NoError(t, err)

		// Connection should still exist after first release
		activeConns := cm.GetActiveConnections()
		assert.Len(t, activeConns, 1)

		err = cm.ReleaseConnection(ctx, "tx2")
		assert.NoError(t, err)

		// Now connection should be gone
		activeConns = cm.GetActiveConnections()
		assert.Len(t, activeConns, 0)
	})

	t.Run("ConnectionLimit", func(t *testing.T) {
		// Acquire up to limit
		_, err := cm.AcquireConnection(ctx, "tx3", 0)
		require.NoError(t, err)

		_, err = cm.AcquireConnection(ctx, "tx4", 0)
		require.NoError(t, err)

		// Should fail to acquire beyond limit
		_, err = cm.AcquireConnection(ctx, "tx5", 0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "maximum transaction connections reached")

		// Clean up
		cm.ReleaseConnection(ctx, "tx3")
		cm.ReleaseConnection(ctx, "tx4")
	})

	t.Run("IdleCleanup", func(t *testing.T) {
		// Set very short idle timeout for testing
		cm.config.IdleTimeout = time.Millisecond

		// Acquire and release connection
		_, err := cm.AcquireConnection(ctx, "tx6", 0)
		require.NoError(t, err)

		err = cm.ReleaseConnection(ctx, "tx6")
		require.NoError(t, err)

		// Wait for connection to become idle
		time.Sleep(2 * time.Millisecond)

		// Cleanup should remove idle connections
		cleaned := cm.CleanupIdleConnections(ctx)
		assert.Equal(t, 1, cleaned)

		activeConns := cm.GetActiveConnections()
		assert.Len(t, activeConns, 0)
	})

	mockConn.AssertExpectations(t)
}

// Test nested transactions
func TestTransactionManager_NestedTransactions(t *testing.T) {
	mockConn := &MockConnection{}
	mockTx := &MockTransaction{}

	mockConn.On("Type").Return(db.PostgreSQLConnectionType)
	mockConn.On("BeginTx", mock.Anything, (*sql.TxOptions)(nil)).Return(mockTx, nil)
	mockTx.On("Commit").Return(nil)

	tm := NewTransactionManager[TestEntity](mockConn, nil)

	ctx := context.Background()
	var outerTxID, innerTxID string

	err := tm.ExecuteInTransaction(ctx, func(outerCtx context.Context, outerTx *ManagedTransaction[TestEntity]) error {
		outerTxID = outerTx.GetTransactionID()
		assert.Equal(t, 0, outerTx.GetNestLevel())

		// Execute nested transaction
		return tm.ExecuteInTransaction(outerCtx, func(innerCtx context.Context, innerTx *ManagedTransaction[TestEntity]) error {
			innerTxID = innerTx.GetTransactionID()
			assert.Equal(t, 1, innerTx.GetNestLevel())
			assert.NotEqual(t, outerTxID, innerTxID)
			return nil
		})
	})

	assert.NoError(t, err)
	assert.NotEmpty(t, outerTxID)
	assert.NotEmpty(t, innerTxID)
	assert.NotEqual(t, outerTxID, innerTxID)

	mockConn.AssertExpectations(t)
	mockTx.AssertExpectations(t)
}

// Test transaction manager statistics
func TestTransactionManager_Statistics(t *testing.T) {
	mockConn := &MockConnection{}
	mockConn.On("Type").Return(db.PostgreSQLConnectionType)

	tm := NewTransactionManager[TestEntity](mockConn, nil)

	// Get connection manager stats
	stats := tm.GetConnectionManagerStats()
	assert.Equal(t, 100, stats.MaxConnections) // Default
	assert.Equal(t, 0, stats.ActiveConnections)
	assert.True(t, stats.ReuseEnabled)

	// Get active transactions (should be empty initially)
	activeTxns := tm.GetActiveTransactions()
	assert.Len(t, activeTxns, 0)

	mockConn.AssertExpectations(t)
}

// Benchmark transaction creation and execution
func BenchmarkTransactionManager_ExecuteInTransaction(b *testing.B) {
	mockConn := &MockConnection{}
	mockTx := &MockTransaction{}

	mockConn.On("Type").Return(db.PostgreSQLConnectionType)
	mockConn.On("BeginTx", mock.Anything, (*sql.TxOptions)(nil)).Return(mockTx, nil)
	mockTx.On("Commit").Return(nil)

	// Disable tracing for benchmark
	config := &TransactionConfig{
		EnableTracing: false,
	}

	tm := NewTransactionManager[TestEntity](mockConn, config)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tm.ExecuteInTransaction(ctx, func(ctx context.Context, tx *ManagedTransaction[TestEntity]) error {
			return nil
		})
	}
}

// Test concurrent transaction execution
func TestTransactionManager_ConcurrentExecution(t *testing.T) {
	mockConn := &MockConnection{}
	mockTx := &MockTransaction{}

	mockConn.On("Type").Return(db.PostgreSQLConnectionType)
	mockConn.On("BeginTx", mock.Anything, (*sql.TxOptions)(nil)).Return(mockTx, nil)
	mockTx.On("Commit").Return(nil)

	tm := NewTransactionManager[TestEntity](mockConn, nil)

	const numGoroutines = 10
	const transactionsPerGoroutine = 5

	ctx := context.Background()
	var wg sync.WaitGroup
	errChan := make(chan error, numGoroutines*transactionsPerGoroutine)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for j := 0; j < transactionsPerGoroutine; j++ {
				err := tm.ExecuteInTransaction(ctx, func(ctx context.Context, tx *ManagedTransaction[TestEntity]) error {
					// Simulate some work
					time.Sleep(time.Microsecond)
					return nil
				})
				if err != nil {
					errChan <- err
				}
			}
		}()
	}

	wg.Wait()
	close(errChan)

	// Check for errors
	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}

	assert.Empty(t, errors, "No errors should occur during concurrent execution")

	// We can't assert exact call counts due to concurrency and mocking limitations
	// but the test passing means the basic concurrent safety is working
}