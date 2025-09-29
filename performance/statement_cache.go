package performance

import (
	"container/list"
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/fathiraz/gorp/db"
)

// StatementCache provides LRU caching for prepared statements with TTL support
type StatementCache struct {
	maxSize    int
	ttl        time.Duration
	cache      map[string]*cacheEntry
	lruList    *list.List
	mu         sync.RWMutex
	tracer     trace.Tracer
	hitCount   int64
	missCount  int64
	evictions  int64
	cleanupTicker *time.Ticker
	stopCleanup   chan struct{}
}

// cacheEntry represents a cached prepared statement with metadata
type cacheEntry struct {
	stmt      *sql.Stmt
	query     string
	connType  db.ConnectionType
	createdAt time.Time
	accessedAt time.Time
	hitCount  int64
	element   *list.Element // for LRU tracking
}

// StatementCacheConfig holds configuration for the statement cache
type StatementCacheConfig struct {
	MaxSize        int           // Maximum number of statements to cache
	TTL            time.Duration // Time-to-live for cached statements
	CleanupInterval time.Duration // How often to run cleanup
	EnableTracing  bool          // Enable OpenTelemetry tracing
	TracerName     string        // Name for the tracer
}

// DefaultStatementCacheConfig returns sensible defaults
func DefaultStatementCacheConfig() *StatementCacheConfig {
	return &StatementCacheConfig{
		MaxSize:         1000,
		TTL:             30 * time.Minute,
		CleanupInterval: 5 * time.Minute,
		EnableTracing:   true,
		TracerName:      "gorp.statement_cache",
	}
}

// NewStatementCache creates a new statement cache with LRU eviction and TTL
func NewStatementCache(config *StatementCacheConfig) *StatementCache {
	if config == nil {
		config = DefaultStatementCacheConfig()
	}

	var tracer trace.Tracer
	if config.EnableTracing {
		tracer = otel.Tracer(config.TracerName)
	}

	cache := &StatementCache{
		maxSize:     config.MaxSize,
		ttl:         config.TTL,
		cache:       make(map[string]*cacheEntry, config.MaxSize),
		lruList:     list.New(),
		tracer:      tracer,
		stopCleanup: make(chan struct{}),
	}

	// Start background cleanup if TTL is configured
	if config.TTL > 0 && config.CleanupInterval > 0 {
		cache.cleanupTicker = time.NewTicker(config.CleanupInterval)
		go cache.cleanupWorker()
	}

	return cache
}

// Get retrieves a prepared statement from cache or prepares a new one
func (sc *StatementCache) Get(ctx context.Context, conn db.Connection, query string) (*sql.Stmt, error) {
	// Create span for cache operation
	var span trace.Span
	if sc.tracer != nil {
		ctx, span = sc.tracer.Start(ctx, "statement_cache.get",
			trace.WithAttributes(
				attribute.String("cache.operation", "get"),
				attribute.String("db.statement", query[:minInt(100, len(query))]), // Truncate long queries
			))
		defer span.End()
	}

	// Generate cache key including connection type for isolation
	key := sc.generateKey(conn.Type(), query)

	sc.mu.Lock()
	defer sc.mu.Unlock()

	// Check for cache hit
	if entry, exists := sc.cache[key]; exists {
		// Check TTL
		if sc.ttl > 0 && time.Since(entry.createdAt) > sc.ttl {
			// Expired entry
			sc.removeEntryLocked(key, entry)
			sc.evictions++

			if span != nil {
				span.SetAttributes(
					attribute.Bool("cache.hit", false),
					attribute.String("cache.miss_reason", "expired"),
				)
			}
		} else {
			// Valid cache hit
			sc.updateLRULocked(entry)
			entry.accessedAt = time.Now()
			entry.hitCount++
			sc.hitCount++

			if span != nil {
				span.SetAttributes(
					attribute.Bool("cache.hit", true),
					attribute.Int64("statement.hit_count", entry.hitCount),
				)
				span.SetStatus(codes.Ok, "Statement found in cache")
			}

			return entry.stmt, nil
		}
	}

	// Cache miss - prepare new statement
	sc.missCount++

	if span != nil {
		span.SetAttributes(
			attribute.Bool("cache.hit", false),
			attribute.String("cache.miss_reason", "not_found"),
		)
	}

	// Prepare statement using appropriate connection method
	var stmt *sql.Stmt
	var err error

	switch conn.Type() {
	case db.PostgreSQLConnectionType:
		// For PostgreSQL, use the generic preparation
		if preparer, ok := conn.(StatementPreparer); ok {
			stmt, err = preparer.PrepareContext(ctx, query)
		} else {
			return nil, ErrUnsupportedConnection
		}
	default:
		// For other databases, use generic preparation
		if preparer, ok := conn.(StatementPreparer); ok {
			stmt, err = preparer.PrepareContext(ctx, query)
		} else {
			return nil, ErrUnsupportedConnection
		}
	}

	if err != nil {
		if span != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "Failed to prepare statement")
		}
		return nil, err
	}

	// Add to cache
	entry := &cacheEntry{
		stmt:       stmt,
		query:      query,
		connType:   conn.Type(),
		createdAt:  time.Now(),
		accessedAt: time.Now(),
		hitCount:   1,
	}

	// Ensure we don't exceed max size
	if len(sc.cache) >= sc.maxSize {
		sc.evictLRULocked()
	}

	// Add to cache and LRU list
	entry.element = sc.lruList.PushFront(key)
	sc.cache[key] = entry

	if span != nil {
		span.SetAttributes(
			attribute.Bool("cache.stored", true),
			attribute.Int("cache.size", len(sc.cache)),
		)
		span.SetStatus(codes.Ok, "Statement prepared and cached")
	}

	return stmt, nil
}

// StatementPreparer interface for connections that can prepare statements
type StatementPreparer interface {
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}

// Put manually adds a prepared statement to the cache
func (sc *StatementCache) Put(conn db.Connection, query string, stmt *sql.Stmt) {
	key := sc.generateKey(conn.Type(), query)

	sc.mu.Lock()
	defer sc.mu.Unlock()

	// Check if already exists
	if existing, exists := sc.cache[key]; exists {
		sc.updateLRULocked(existing)
		return
	}

	// Ensure we don't exceed max size
	if len(sc.cache) >= sc.maxSize {
		sc.evictLRULocked()
	}

	// Add new entry
	entry := &cacheEntry{
		stmt:       stmt,
		query:      query,
		connType:   conn.Type(),
		createdAt:  time.Now(),
		accessedAt: time.Now(),
		hitCount:   0,
	}

	entry.element = sc.lruList.PushFront(key)
	sc.cache[key] = entry
}

// Remove removes a statement from the cache
func (sc *StatementCache) Remove(conn db.Connection, query string) {
	key := sc.generateKey(conn.Type(), query)

	sc.mu.Lock()
	defer sc.mu.Unlock()

	if entry, exists := sc.cache[key]; exists {
		sc.removeEntryLocked(key, entry)
	}
}

// Clear removes all cached statements
func (sc *StatementCache) Clear() {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	// Close all prepared statements
	for key, entry := range sc.cache {
		if entry.stmt != nil {
			entry.stmt.Close()
		}
		delete(sc.cache, key)
	}

	// Clear LRU list
	sc.lruList.Init()

	// Reset counters
	sc.hitCount = 0
	sc.missCount = 0
	sc.evictions = 0
}

// Stats returns cache statistics
func (sc *StatementCache) Stats() StatementCacheStats {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	return StatementCacheStats{
		Size:       len(sc.cache),
		MaxSize:    sc.maxSize,
		HitCount:   sc.hitCount,
		MissCount:  sc.missCount,
		Evictions:  sc.evictions,
		HitRate:    sc.calculateHitRate(),
		TTL:        sc.ttl,
	}
}

// StatementCacheStats represents cache performance statistics
type StatementCacheStats struct {
	Size       int
	MaxSize    int
	HitCount   int64
	MissCount  int64
	Evictions  int64
	HitRate    float64
	TTL        time.Duration
}

// Close shuts down the statement cache and closes all prepared statements
func (sc *StatementCache) Close() error {
	// Stop cleanup worker
	if sc.cleanupTicker != nil {
		sc.cleanupTicker.Stop()
		close(sc.stopCleanup)
	}

	// Clear all statements
	sc.Clear()

	return nil
}

// Internal helper methods

func (sc *StatementCache) generateKey(connType db.ConnectionType, query string) string {
	return string(connType) + ":" + query
}

func (sc *StatementCache) updateLRULocked(entry *cacheEntry) {
	sc.lruList.MoveToFront(entry.element)
}

func (sc *StatementCache) evictLRULocked() {
	if sc.lruList.Len() == 0 {
		return
	}

	// Get least recently used item
	elem := sc.lruList.Back()
	if elem != nil {
		key := elem.Value.(string)
		if entry, exists := sc.cache[key]; exists {
			sc.removeEntryLocked(key, entry)
			sc.evictions++
		}
	}
}

func (sc *StatementCache) removeEntryLocked(key string, entry *cacheEntry) {
	// Close the prepared statement
	if entry.stmt != nil {
		entry.stmt.Close()
	}

	// Remove from cache and LRU list
	delete(sc.cache, key)
	if entry.element != nil {
		sc.lruList.Remove(entry.element)
	}
}

func (sc *StatementCache) calculateHitRate() float64 {
	total := sc.hitCount + sc.missCount
	if total == 0 {
		return 0.0
	}
	return float64(sc.hitCount) / float64(total)
}

func (sc *StatementCache) cleanupWorker() {
	for {
		select {
		case <-sc.stopCleanup:
			return
		case <-sc.cleanupTicker.C:
			sc.cleanupExpired()
		}
	}
}

func (sc *StatementCache) cleanupExpired() {
	if sc.ttl <= 0 {
		return
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	cutoff := time.Now().Add(-sc.ttl)
	var toRemove []string

	// Find expired entries
	for key, entry := range sc.cache {
		if entry.createdAt.Before(cutoff) {
			toRemove = append(toRemove, key)
		}
	}

	// Remove expired entries
	for _, key := range toRemove {
		if entry, exists := sc.cache[key]; exists {
			sc.removeEntryLocked(key, entry)
			sc.evictions++
		}
	}
}

// Utility functions
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Error definitions
var (
	ErrUnsupportedConnection = fmt.Errorf("unsupported connection type for statement preparation")
)

// PreparedStatementManager manages prepared statements across connections
type PreparedStatementManager struct {
	caches map[db.ConnectionType]*StatementCache
	mu     sync.RWMutex
}

// NewPreparedStatementManager creates a new manager for prepared statements
func NewPreparedStatementManager() *PreparedStatementManager {
	return &PreparedStatementManager{
		caches: make(map[db.ConnectionType]*StatementCache),
	}
}

// GetCache returns the statement cache for a specific connection type
func (psm *PreparedStatementManager) GetCache(connType db.ConnectionType) *StatementCache {
	psm.mu.RLock()
	if cache, exists := psm.caches[connType]; exists {
		psm.mu.RUnlock()
		return cache
	}
	psm.mu.RUnlock()

	// Create new cache for this connection type
	psm.mu.Lock()
	defer psm.mu.Unlock()

	// Double-check after acquiring write lock
	if cache, exists := psm.caches[connType]; exists {
		return cache
	}

	// Create new cache with default config
	cache := NewStatementCache(DefaultStatementCacheConfig())
	psm.caches[connType] = cache
	return cache
}

// GetStatement retrieves or prepares a statement for the given connection
func (psm *PreparedStatementManager) GetStatement(ctx context.Context, conn db.Connection, query string) (*sql.Stmt, error) {
	cache := psm.GetCache(conn.Type())
	return cache.Get(ctx, conn, query)
}

// GetAllStats returns statistics for all cached connection types
func (psm *PreparedStatementManager) GetAllStats() map[db.ConnectionType]StatementCacheStats {
	psm.mu.RLock()
	defer psm.mu.RUnlock()

	stats := make(map[db.ConnectionType]StatementCacheStats)
	for connType, cache := range psm.caches {
		stats[connType] = cache.Stats()
	}
	return stats
}

// Close closes all statement caches
func (psm *PreparedStatementManager) Close() error {
	psm.mu.Lock()
	defer psm.mu.Unlock()

	for _, cache := range psm.caches {
		if err := cache.Close(); err != nil {
			return err
		}
	}

	psm.caches = make(map[db.ConnectionType]*StatementCache)
	return nil
}