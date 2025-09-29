// Transaction-based test isolation utilities for GORP testing
package testing

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/fathiraz/gorp/db"
	"github.com/fathiraz/gorp/transaction"
)

// TransactionTestCase represents a test case that runs within a transaction
type TransactionTestCase func(t *testing.T, tx db.Transaction)

// IsolatedTest represents a test that runs in complete isolation
type IsolatedTest struct {
	Name        string
	Setup       func(t *testing.T, conn db.Connection) error
	Test        TransactionTestCase
	Teardown    func(t *testing.T, conn db.Connection) error
	Timeout     time.Duration
	Retries     int
	Parallel    bool
	SkipCleanup bool
}

// TransactionTestSuite manages transaction-based test isolation
type TransactionTestSuite struct {
	connection   db.Connection
	activeTests  map[string]*activeTest
	mutex        sync.RWMutex
	testTimeout  time.Duration
	cleanupDelay time.Duration
}

// activeTest tracks a running test with its transaction
type activeTest struct {
	transaction db.Transaction
	startTime   time.Time
	cleanup     func() error
	done        chan struct{}
}

// NewTransactionTestSuite creates a new transaction test suite
func NewTransactionTestSuite(connection db.Connection) *TransactionTestSuite {
	return &TransactionTestSuite{
		connection:   connection,
		activeTests:  make(map[string]*activeTest),
		testTimeout:  30 * time.Second,
		cleanupDelay: 1 * time.Second,
	}
}

// SetTimeout sets the default timeout for isolated tests
func (tts *TransactionTestSuite) SetTimeout(timeout time.Duration) {
	tts.testTimeout = timeout
}

// SetCleanupDelay sets the delay before cleanup operations
func (tts *TransactionTestSuite) SetCleanupDelay(delay time.Duration) {
	tts.cleanupDelay = delay
}

// RunIsolated runs a test in complete transaction isolation
func (tts *TransactionTestSuite) RunIsolated(t *testing.T, test IsolatedTest) {
	t.Helper()

	// Set default timeout if not specified
	timeout := test.Timeout
	if timeout == 0 {
		timeout = tts.testTimeout
	}

	testKey := fmt.Sprintf("%s-%d", test.Name, time.Now().UnixNano())

	// Setup test environment
	if test.Setup != nil {
		require.NoError(t, test.Setup(t, tts.connection), "Test setup failed")
	}

	// Create transaction for test isolation
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	tx, err := tts.connection.Begin(ctx)
	require.NoError(t, err, "Failed to begin transaction")

	// Track active test
	activeTest := &activeTest{
		transaction: tx,
		startTime:   time.Now(),
		done:        make(chan struct{}),
		cleanup: func() error {
			// Always rollback to ensure isolation
			return tx.Rollback()
		},
	}

	tts.mutex.Lock()
	tts.activeTests[testKey] = activeTest
	tts.mutex.Unlock()

	// Setup cleanup
	defer func() {
		close(activeTest.done)

		// Wait for cleanup delay to allow any async operations to complete
		time.Sleep(tts.cleanupDelay)

		if !test.SkipCleanup {
			if err := activeTest.cleanup(); err != nil {
				t.Logf("Transaction cleanup failed: %v", err)
			}
		}

		// Run teardown
		if test.Teardown != nil {
			if err := test.Teardown(t, tts.connection); err != nil {
				t.Logf("Test teardown failed: %v", err)
			}
		}

		// Remove from active tests
		tts.mutex.Lock()
		delete(tts.activeTests, testKey)
		tts.mutex.Unlock()
	}()

	// Run the test with retries if specified
	maxAttempts := test.Retries + 1
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		func() {
			// Create a new transaction for each retry
			if attempt > 1 {
				// Rollback previous attempt
				tx.Rollback()

				// Create new transaction
				newTx, err := tts.connection.Begin(ctx)
				require.NoError(t, err, "Failed to begin retry transaction")
				tx = newTx
				activeTest.transaction = newTx
				activeTest.cleanup = func() error { return newTx.Rollback() }
			}

			defer func() {
				if r := recover(); r != nil {
					lastErr = fmt.Errorf("test panicked: %v", r)
				}
			}()

			// Run the test
			test.Test(t, tx)
			lastErr = nil // Test succeeded
		}()

		if lastErr == nil {
			break // Test succeeded
		}

		if attempt < maxAttempts {
			t.Logf("Test attempt %d failed, retrying: %v", attempt, lastErr)
			time.Sleep(time.Duration(attempt) * 100 * time.Millisecond)
		}
	}

	if lastErr != nil {
		t.Fatalf("Test failed after %d attempts: %v", maxAttempts, lastErr)
	}
}

// RunParallel runs multiple isolated tests in parallel
func (tts *TransactionTestSuite) RunParallel(t *testing.T, tests []IsolatedTest) {
	t.Helper()

	// Filter tests that can run in parallel
	parallelTests := make([]IsolatedTest, 0)
	sequentialTests := make([]IsolatedTest, 0)

	for _, test := range tests {
		if test.Parallel {
			parallelTests = append(parallelTests, test)
		} else {
			sequentialTests = append(sequentialTests, test)
		}
	}

	// Run sequential tests first
	for _, test := range sequentialTests {
		t.Run(test.Name, func(t *testing.T) {
			tts.RunIsolated(t, test)
		})
	}

	// Run parallel tests
	for _, test := range parallelTests {
		test := test // capture loop variable
		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()
			tts.RunIsolated(t, test)
		})
	}
}

// GetActiveTests returns information about currently running tests
func (tts *TransactionTestSuite) GetActiveTests() map[string]time.Duration {
	tts.mutex.RLock()
	defer tts.mutex.RUnlock()

	result := make(map[string]time.Duration)
	now := time.Now()

	for key, test := range tts.activeTests {
		result[key] = now.Sub(test.startTime)
	}

	return result
}

// ForceCleanup forcefully cleans up all active tests
func (tts *TransactionTestSuite) ForceCleanup() error {
	tts.mutex.Lock()
	defer tts.mutex.Unlock()

	var errors []error
	for key, test := range tts.activeTests {
		if err := test.cleanup(); err != nil {
			errors = append(errors, fmt.Errorf("cleanup failed for test %s: %w", key, err))
		}
		close(test.done)
		delete(tts.activeTests, key)
	}

	if len(errors) > 0 {
		return fmt.Errorf("force cleanup errors: %v", errors)
	}
	return nil
}

// SavepointManager manages database savepoints for nested test isolation
type SavepointManager struct {
	transaction db.Transaction
	savepoints  []string
	mutex       sync.Mutex
}

// NewSavepointManager creates a new savepoint manager
func NewSavepointManager(tx db.Transaction) *SavepointManager {
	return &SavepointManager{
		transaction: tx,
		savepoints:  make([]string, 0),
	}
}

// CreateSavepoint creates a new savepoint
func (sm *SavepointManager) CreateSavepoint(ctx context.Context, name string) error {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	query := fmt.Sprintf("SAVEPOINT %s", name)
	_, err := sm.transaction.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create savepoint %s: %w", name, err)
	}

	sm.savepoints = append(sm.savepoints, name)
	return nil
}

// RollbackToSavepoint rolls back to a specific savepoint
func (sm *SavepointManager) RollbackToSavepoint(ctx context.Context, name string) error {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	query := fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", name)
	_, err := sm.transaction.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to rollback to savepoint %s: %w", name, err)
	}

	// Remove savepoints created after this one
	for i, sp := range sm.savepoints {
		if sp == name {
			sm.savepoints = sm.savepoints[:i+1]
			break
		}
	}

	return nil
}

// ReleaseSavepoint releases a savepoint
func (sm *SavepointManager) ReleaseSavepoint(ctx context.Context, name string) error {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	query := fmt.Sprintf("RELEASE SAVEPOINT %s", name)
	_, err := sm.transaction.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to release savepoint %s: %w", name, err)
	}

	// Remove the savepoint from our list
	for i, sp := range sm.savepoints {
		if sp == name {
			sm.savepoints = append(sm.savepoints[:i], sm.savepoints[i+1:]...)
			break
		}
	}

	return nil
}

// GetSavepoints returns the list of active savepoints
func (sm *SavepointManager) GetSavepoints() []string {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	result := make([]string, len(sm.savepoints))
	copy(result, sm.savepoints)
	return result
}

// CleanupAllSavepoints releases all active savepoints
func (sm *SavepointManager) CleanupAllSavepoints(ctx context.Context) error {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	var errors []error
	for i := len(sm.savepoints) - 1; i >= 0; i-- {
		query := fmt.Sprintf("RELEASE SAVEPOINT %s", sm.savepoints[i])
		if _, err := sm.transaction.ExecContext(ctx, query); err != nil {
			errors = append(errors, err)
		}
	}

	sm.savepoints = sm.savepoints[:0]

	if len(errors) > 0 {
		return fmt.Errorf("savepoint cleanup errors: %v", errors)
	}
	return nil
}

// TestDataIsolator provides data isolation for tests
type TestDataIsolator struct {
	connection      db.Connection
	isolationLevel  sql.IsolationLevel
	readCommitted   bool
	snapshotMode    bool
	cleanupQueries  []string
}

// NewTestDataIsolator creates a new test data isolator
func NewTestDataIsolator(connection db.Connection) *TestDataIsolator {
	return &TestDataIsolator{
		connection:     connection,
		isolationLevel: sql.LevelSerializable,
		cleanupQueries: make([]string, 0),
	}
}

// SetIsolationLevel sets the transaction isolation level
func (tdi *TestDataIsolator) SetIsolationLevel(level sql.IsolationLevel) {
	tdi.isolationLevel = level
}

// SetReadCommitted enables read committed mode
func (tdi *TestDataIsolator) SetReadCommitted(enabled bool) {
	tdi.readCommitted = enabled
}

// SetSnapshotMode enables snapshot isolation mode
func (tdi *TestDataIsolator) SetSnapshotMode(enabled bool) {
	tdi.snapshotMode = enabled
}

// AddCleanupQuery adds a query to run during cleanup
func (tdi *TestDataIsolator) AddCleanupQuery(query string) {
	tdi.cleanupQueries = append(tdi.cleanupQueries, query)
}

// BeginIsolatedTest begins a new isolated test session
func (tdi *TestDataIsolator) BeginIsolatedTest(ctx context.Context) (*IsolatedTestSession, error) {
	tx, err := tdi.connection.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin isolated test: %w", err)
	}

	session := &IsolatedTestSession{
		transaction:    tx,
		isolator:       tdi,
		savepointMgr:   NewSavepointManager(tx),
		cleanupQueries: make([]string, len(tdi.cleanupQueries)),
	}

	copy(session.cleanupQueries, tdi.cleanupQueries)

	// Set isolation level if supported
	if tdi.snapshotMode {
		_, err = tx.ExecContext(ctx, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
		if err != nil {
			tx.Rollback()
			return nil, fmt.Errorf("failed to set snapshot isolation: %w", err)
		}
	}

	return session, nil
}

// IsolatedTestSession represents an active isolated test session
type IsolatedTestSession struct {
	transaction    db.Transaction
	isolator       *TestDataIsolator
	savepointMgr   *SavepointManager
	cleanupQueries []string
	closed         bool
}

// Transaction returns the underlying transaction
func (its *IsolatedTestSession) Transaction() db.Transaction {
	return its.transaction
}

// SavepointManager returns the savepoint manager
func (its *IsolatedTestSession) SavepointManager() *SavepointManager {
	return its.savepointMgr
}

// CreateSavepoint creates a savepoint within the session
func (its *IsolatedTestSession) CreateSavepoint(ctx context.Context, name string) error {
	if its.closed {
		return fmt.Errorf("session is closed")
	}
	return its.savepointMgr.CreateSavepoint(ctx, name)
}

// RollbackToSavepoint rolls back to a savepoint within the session
func (its *IsolatedTestSession) RollbackToSavepoint(ctx context.Context, name string) error {
	if its.closed {
		return fmt.Errorf("session is closed")
	}
	return its.savepointMgr.RollbackToSavepoint(ctx, name)
}

// AddCleanupQuery adds a cleanup query specific to this session
func (its *IsolatedTestSession) AddCleanupQuery(query string) {
	its.cleanupQueries = append(its.cleanupQueries, query)
}

// Rollback rolls back the entire test session
func (its *IsolatedTestSession) Rollback() error {
	if its.closed {
		return nil
	}
	its.closed = true
	return its.transaction.Rollback()
}

// Close closes the test session with optional cleanup
func (its *IsolatedTestSession) Close(ctx context.Context, runCleanup bool) error {
	if its.closed {
		return nil
	}
	its.closed = true

	if runCleanup {
		// Run cleanup queries
		for _, query := range its.cleanupQueries {
			if _, err := its.transaction.ExecContext(ctx, query); err != nil {
				// Log error but continue with cleanup
				fmt.Printf("Cleanup query failed: %v\n", err)
			}
		}
	}

	// Always rollback to ensure isolation
	return its.transaction.Rollback()
}

// ConcurrentTestRunner runs tests concurrently with isolation
type ConcurrentTestRunner struct {
	maxConcurrent int
	timeout       time.Duration
	retryDelay    time.Duration
}

// NewConcurrentTestRunner creates a new concurrent test runner
func NewConcurrentTestRunner(maxConcurrent int, timeout time.Duration) *ConcurrentTestRunner {
	return &ConcurrentTestRunner{
		maxConcurrent: maxConcurrent,
		timeout:       timeout,
		retryDelay:    100 * time.Millisecond,
	}
}

// Run runs multiple test functions concurrently with proper isolation
func (ctr *ConcurrentTestRunner) Run(t *testing.T, connection db.Connection, tests []func(t *testing.T, tx db.Transaction)) {
	t.Helper()

	semaphore := make(chan struct{}, ctr.maxConcurrent)
	results := make(chan error, len(tests))

	for i, testFunc := range tests {
		testFunc := testFunc // capture loop variable
		go func(testIndex int) {
			semaphore <- struct{}{} // acquire
			defer func() { <-semaphore }() // release

			ctx, cancel := context.WithTimeout(context.Background(), ctr.timeout)
			defer cancel()

			tx, err := connection.Begin(ctx)
			if err != nil {
				results <- fmt.Errorf("test %d: failed to begin transaction: %w", testIndex, err)
				return
			}
			defer tx.Rollback() // always rollback for isolation

			// Run test with panic recovery
			func() {
				defer func() {
					if r := recover(); r != nil {
						results <- fmt.Errorf("test %d: panicked: %v", testIndex, r)
					}
				}()

				testT := &testing.T{} // create isolated testing.T
				testFunc(testT, tx)
				results <- nil // success
			}()
		}(i)
	}

	// Wait for all tests to complete
	for i := 0; i < len(tests); i++ {
		if err := <-results; err != nil {
			t.Errorf("Concurrent test failed: %v", err)
		}
	}
}