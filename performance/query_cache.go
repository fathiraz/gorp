package performance

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// QueryCache provides pluggable caching for query results with TTL and invalidation
type QueryCache[T any] struct {
	backend      CacheBackend[T]
	ttl          time.Duration
	enabled      bool
	tracer       trace.Tracer
	hitCount     int64
	missCount    int64
	errorCount   int64
	mu           sync.RWMutex
	invalidationCallbacks map[string]func()
	loadingMap   map[string]*sync.WaitGroup
}

// CacheBackend defines the interface for cache storage backends
type CacheBackend[T any] interface {
	Get(ctx context.Context, key string) (T, bool, error)
	Set(ctx context.Context, key string, value T, ttl time.Duration) error
	Delete(ctx context.Context, key string) error
	Clear(ctx context.Context) error
	Keys(ctx context.Context, pattern string) ([]string, error)
	Stats() CacheBackendStats
}

// CacheBackendStats provides statistics for cache backends
type CacheBackendStats struct {
	Size         int
	HitCount     int64
	MissCount    int64
	ErrorCount   int64
	MemoryUsage  int64
	Evictions    int64
}

// QueryCacheConfig holds configuration for query cache
type QueryCacheConfig struct {
	Backend          CacheBackend[any]
	TTL              time.Duration
	Enabled          bool
	EnableTracing    bool
	TracerName       string
}

// NewQueryCache creates a new query cache with the given configuration
func NewQueryCache[T any](config *QueryCacheConfig, backend CacheBackend[T]) *QueryCache[T] {
	if config == nil {
		config = &QueryCacheConfig{
			TTL:           5 * time.Minute,
			Enabled:       true,
			EnableTracing: true,
			TracerName:    "gorp.query_cache",
		}
	}

	var tracer trace.Tracer
	if config.EnableTracing {
		tracer = otel.Tracer(config.TracerName)
	}

	return &QueryCache[T]{
		backend:               backend,
		ttl:                   config.TTL,
		enabled:               config.Enabled,
		tracer:                tracer,
		invalidationCallbacks: make(map[string]func()),
		loadingMap:            make(map[string]*sync.WaitGroup),
	}
}

// Get retrieves a value from cache, executing loader if not found
func (qc *QueryCache[T]) Get(ctx context.Context, key string, loader func(ctx context.Context) (T, error)) (T, error) {
	var result T

	if !qc.enabled {
		return loader(ctx)
	}

	// Create span for cache operation
	var span trace.Span
	if qc.tracer != nil {
		ctx, span = qc.tracer.Start(ctx, "query_cache.get",
			trace.WithAttributes(
				attribute.String("cache.key", key),
				attribute.String("cache.operation", "get"),
			))
		defer span.End()
	}

	// Use a loading map to prevent duplicate concurrent loads for the same key
	qc.mu.Lock()
	if wg, loading := qc.loadingMap[key]; loading {
		qc.mu.Unlock()
		wg.Wait()
		// After waiting, try cache again
		qc.mu.RLock()
		qc.hitCount++
		qc.mu.RUnlock()
		cached, found, _ := qc.backend.Get(ctx, key)
		if found {
			return cached, nil
		}
	} else {
		// Mark this key as loading
		wg := &sync.WaitGroup{}
		wg.Add(1)
		qc.loadingMap[key] = wg
		qc.mu.Unlock()

		defer func() {
			qc.mu.Lock()
			delete(qc.loadingMap, key)
			qc.mu.Unlock()
			wg.Done()
		}()
	}

	// Try to get from cache first
	start := time.Now()
	cached, found, err := qc.backend.Get(ctx, key)
	cacheLatency := time.Since(start)

	if err != nil {
		qc.mu.Lock()
		qc.errorCount++
		qc.mu.Unlock()

		if span != nil {
			span.RecordError(err)
			span.SetAttributes(
				attribute.Bool("cache.hit", false),
				attribute.String("cache.miss_reason", "error"),
				attribute.Int64("cache.latency_ms", cacheLatency.Milliseconds()),
			)
		}

		// Fall back to loader on cache error
		return loader(ctx)
	}

	if found {
		qc.mu.Lock()
		qc.hitCount++
		qc.mu.Unlock()

		if span != nil {
			span.SetAttributes(
				attribute.Bool("cache.hit", true),
				attribute.Int64("cache.latency_ms", cacheLatency.Milliseconds()),
			)
			span.SetStatus(codes.Ok, "Cache hit")
		}

		return cached, nil
	}

	// Cache miss - execute loader
	qc.mu.Lock()
	qc.missCount++
	qc.mu.Unlock()

	if span != nil {
		span.SetAttributes(
			attribute.Bool("cache.hit", false),
			attribute.String("cache.miss_reason", "not_found"),
			attribute.Int64("cache.latency_ms", cacheLatency.Milliseconds()),
		)
	}

	loadStart := time.Now()
	result, err = loader(ctx)
	loadLatency := time.Since(loadStart)

	if err != nil {
		if span != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "Loader failed")
		}
		return result, err
	}

	// Store in cache for future use
	storeStart := time.Now()
	storeErr := qc.backend.Set(ctx, key, result, qc.ttl)
	storeLatency := time.Since(storeStart)

	if storeErr != nil && span != nil {
		span.RecordError(storeErr)
		// Don't fail the request, just log the cache store error
	}

	if span != nil {
		span.SetAttributes(
			attribute.Int64("loader.latency_ms", loadLatency.Milliseconds()),
			attribute.Int64("cache.store_latency_ms", storeLatency.Milliseconds()),
			attribute.Bool("cache.stored", storeErr == nil),
		)
		span.SetStatus(codes.Ok, "Value loaded and cached")
	}

	return result, nil
}

// Set stores a value in the cache
func (qc *QueryCache[T]) Set(ctx context.Context, key string, value T) error {
	if !qc.enabled {
		return nil
	}

	return qc.backend.Set(ctx, key, value, qc.ttl)
}

// Delete removes a value from the cache
func (qc *QueryCache[T]) Delete(ctx context.Context, key string) error {
	if !qc.enabled {
		return nil
	}

	// Execute invalidation callbacks
	qc.mu.RLock()
	if callback, exists := qc.invalidationCallbacks[key]; exists {
		callback()
	}
	qc.mu.RUnlock()

	return qc.backend.Delete(ctx, key)
}

// InvalidatePattern removes all keys matching the given pattern
func (qc *QueryCache[T]) InvalidatePattern(ctx context.Context, pattern string) error {
	if !qc.enabled {
		return nil
	}

	keys, err := qc.backend.Keys(ctx, pattern)
	if err != nil {
		return err
	}

	for _, key := range keys {
		if err := qc.Delete(ctx, key); err != nil {
			return err
		}
	}

	return nil
}

// Clear removes all cached values
func (qc *QueryCache[T]) Clear(ctx context.Context) error {
	if !qc.enabled {
		return nil
	}

	return qc.backend.Clear(ctx)
}

// AddInvalidationCallback adds a callback to be executed when a key is invalidated
func (qc *QueryCache[T]) AddInvalidationCallback(key string, callback func()) {
	qc.mu.Lock()
	defer qc.mu.Unlock()
	qc.invalidationCallbacks[key] = callback
}

// RemoveInvalidationCallback removes an invalidation callback
func (qc *QueryCache[T]) RemoveInvalidationCallback(key string) {
	qc.mu.Lock()
	defer qc.mu.Unlock()
	delete(qc.invalidationCallbacks, key)
}

// Stats returns cache statistics
func (qc *QueryCache[T]) Stats() QueryCacheStats {
	qc.mu.RLock()
	defer qc.mu.RUnlock()

	backendStats := qc.backend.Stats()
	hitRate := float64(0)
	total := qc.hitCount + qc.missCount
	if total > 0 {
		hitRate = float64(qc.hitCount) / float64(total)
	}

	return QueryCacheStats{
		HitCount:    qc.hitCount,
		MissCount:   qc.missCount,
		ErrorCount:  qc.errorCount,
		HitRate:     hitRate,
		TTL:         qc.ttl,
		Enabled:     qc.enabled,
		BackendStats: backendStats,
	}
}

// QueryCacheStats represents cache statistics
type QueryCacheStats struct {
	HitCount     int64
	MissCount    int64
	ErrorCount   int64
	HitRate      float64
	TTL          time.Duration
	Enabled      bool
	BackendStats CacheBackendStats
}

// GenerateKey creates a cache key from query and parameters
func GenerateKey(query string, params ...interface{}) string {
	h := sha256.New()
	h.Write([]byte(query))

	for _, param := range params {
		h.Write([]byte(fmt.Sprintf("%v", param)))
	}

	return hex.EncodeToString(h.Sum(nil))[:16] // Use first 16 chars for shorter keys
}

// In-Memory Cache Backend Implementation
type MemoryCacheBackend[T any] struct {
	cache     map[string]*memoryCacheEntry[T]
	mu        sync.RWMutex
	hitCount  int64
	missCount int64
	evictions int64
	maxSize   int
}

type memoryCacheEntry[T any] struct {
	value     T
	expiresAt time.Time
	size      int64
}

// NewMemoryCacheBackend creates a new in-memory cache backend
func NewMemoryCacheBackend[T any](maxSize int) *MemoryCacheBackend[T] {
	return &MemoryCacheBackend[T]{
		cache:   make(map[string]*memoryCacheEntry[T]),
		maxSize: maxSize,
	}
}

// Get implements CacheBackend interface
func (m *MemoryCacheBackend[T]) Get(ctx context.Context, key string) (T, bool, error) {
	m.mu.RLock()
	entry, exists := m.cache[key]
	m.mu.RUnlock()

	var zero T

	if !exists {
		m.mu.Lock()
		m.missCount++
		m.mu.Unlock()
		return zero, false, nil
	}

	// Check expiration
	if time.Now().After(entry.expiresAt) {
		m.mu.Lock()
		delete(m.cache, key)
		m.missCount++
		m.mu.Unlock()
		return zero, false, nil
	}

	m.mu.Lock()
	m.hitCount++
	m.mu.Unlock()

	return entry.value, true, nil
}

// Set implements CacheBackend interface
func (m *MemoryCacheBackend[T]) Set(ctx context.Context, key string, value T, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Ensure we don't exceed max size
	if len(m.cache) >= m.maxSize {
		m.evictOldest()
	}

	entry := &memoryCacheEntry[T]{
		value:     value,
		expiresAt: time.Now().Add(ttl),
		size:      calculateSize(value),
	}

	m.cache[key] = entry
	return nil
}

// Delete implements CacheBackend interface
func (m *MemoryCacheBackend[T]) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.cache, key)
	return nil
}

// Clear implements CacheBackend interface
func (m *MemoryCacheBackend[T]) Clear(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cache = make(map[string]*memoryCacheEntry[T])
	return nil
}

// Keys implements CacheBackend interface
func (m *MemoryCacheBackend[T]) Keys(ctx context.Context, pattern string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var keys []string
	for key := range m.cache {
		// Simple pattern matching - in production you'd want more sophisticated matching
		if pattern == "*" || key == pattern {
			keys = append(keys, key)
		}
	}

	return keys, nil
}

// Stats implements CacheBackend interface
func (m *MemoryCacheBackend[T]) Stats() CacheBackendStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var memoryUsage int64
	for _, entry := range m.cache {
		memoryUsage += entry.size
	}

	return CacheBackendStats{
		Size:        len(m.cache),
		HitCount:    m.hitCount,
		MissCount:   m.missCount,
		MemoryUsage: memoryUsage,
		Evictions:   m.evictions,
	}
}

// evictOldest removes the oldest entry (simple FIFO for now)
func (m *MemoryCacheBackend[T]) evictOldest() {
	if len(m.cache) == 0 {
		return
	}

	var oldestKey string
	var oldestTime time.Time
	first := true

	for key, entry := range m.cache {
		if first || entry.expiresAt.Before(oldestTime) {
			oldestKey = key
			oldestTime = entry.expiresAt
			first = false
		}
	}

	if oldestKey != "" {
		delete(m.cache, oldestKey)
		m.evictions++
	}
}

// calculateSize estimates the memory size of a value
func calculateSize[T any](value T) int64 {
	// Simplified size calculation - in production you'd want more accurate sizing
	return int64(64) // Base estimate for most types
}

// Redis Cache Backend (Interface definition - would need actual Redis implementation)
type RedisCacheBackend[T any] struct {
	// Redis client and configuration would go here
	enabled bool
}

// NewRedisCacheBackend would create a Redis-backed cache
func NewRedisCacheBackend[T any](addr, password string, db int) *RedisCacheBackend[T] {
	return &RedisCacheBackend[T]{
		enabled: false, // Placeholder - would initialize Redis client
	}
}

// Get implements CacheBackend interface for Redis
func (r *RedisCacheBackend[T]) Get(ctx context.Context, key string) (T, bool, error) {
	var zero T
	if !r.enabled {
		return zero, false, fmt.Errorf("Redis backend not implemented")
	}
	// Redis GET implementation would go here
	return zero, false, nil
}

// Set implements CacheBackend interface for Redis
func (r *RedisCacheBackend[T]) Set(ctx context.Context, key string, value T, ttl time.Duration) error {
	if !r.enabled {
		return fmt.Errorf("Redis backend not implemented")
	}
	// Redis SET with TTL implementation would go here
	return nil
}

// Delete implements CacheBackend interface for Redis
func (r *RedisCacheBackend[T]) Delete(ctx context.Context, key string) error {
	if !r.enabled {
		return fmt.Errorf("Redis backend not implemented")
	}
	// Redis DEL implementation would go here
	return nil
}

// Clear implements CacheBackend interface for Redis
func (r *RedisCacheBackend[T]) Clear(ctx context.Context) error {
	if !r.enabled {
		return fmt.Errorf("Redis backend not implemented")
	}
	// Redis FLUSHDB implementation would go here
	return nil
}

// Keys implements CacheBackend interface for Redis
func (r *RedisCacheBackend[T]) Keys(ctx context.Context, pattern string) ([]string, error) {
	if !r.enabled {
		return nil, fmt.Errorf("Redis backend not implemented")
	}
	// Redis KEYS implementation would go here
	return nil, nil
}

// Stats implements CacheBackend interface for Redis
func (r *RedisCacheBackend[T]) Stats() CacheBackendStats {
	// Redis INFO implementation would go here
	return CacheBackendStats{}
}

// QueryCacheManager manages multiple query caches for different types
type QueryCacheManager struct {
	caches map[string]interface{}
	mu     sync.RWMutex
	config *QueryCacheConfig
}

// NewQueryCacheManager creates a new query cache manager
func NewQueryCacheManager(config *QueryCacheConfig) *QueryCacheManager {
	return &QueryCacheManager{
		caches: make(map[string]interface{}),
		config: config,
	}
}

// GetCache returns a typed cache for the given name
func GetQueryCache[T any](manager *QueryCacheManager, name string, backend CacheBackend[T]) *QueryCache[T] {
	manager.mu.RLock()
	if cache, exists := manager.caches[name]; exists {
		if typedCache, ok := cache.(*QueryCache[T]); ok {
			manager.mu.RUnlock()
			return typedCache
		}
	}
	manager.mu.RUnlock()

	manager.mu.Lock()
	defer manager.mu.Unlock()

	// Double-check after acquiring write lock
	if cache, exists := manager.caches[name]; exists {
		if typedCache, ok := cache.(*QueryCache[T]); ok {
			return typedCache
		}
	}

	// Create new cache
	newCache := NewQueryCache(manager.config, backend)
	manager.caches[name] = newCache
	return newCache
}

// GetAllStats returns statistics for all caches
func (qcm *QueryCacheManager) GetAllStats() map[string]QueryCacheStats {
	qcm.mu.RLock()
	defer qcm.mu.RUnlock()

	stats := make(map[string]QueryCacheStats)
	for name, cache := range qcm.caches {
		// Type assertion to get stats - in production you'd handle this more elegantly
		if statsProvider, ok := cache.(interface{ Stats() QueryCacheStats }); ok {
			stats[name] = statsProvider.Stats()
		}
	}

	return stats
}