package performance

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// Test data types
type CachedUser struct {
	ID   int
	Name string
}

type CachedPost struct {
	ID     int
	Title  string
	UserID int
}

func TestMemoryCacheBackend_Basic(t *testing.T) {
	backend := NewMemoryCacheBackend[string](10)
	ctx := context.Background()

	// Test Set and Get
	err := backend.Set(ctx, "key1", "value1", 5*time.Minute)
	if err != nil {
		t.Fatalf("Unexpected error setting value: %v", err)
	}

	value, found, err := backend.Get(ctx, "key1")
	if err != nil {
		t.Fatalf("Unexpected error getting value: %v", err)
	}

	if !found {
		t.Error("Expected to find cached value")
	}

	if value != "value1" {
		t.Errorf("Expected 'value1', got '%s'", value)
	}

	// Test non-existent key
	_, found, err = backend.Get(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if found {
		t.Error("Expected not to find non-existent key")
	}
}

func TestMemoryCacheBackend_Expiration(t *testing.T) {
	backend := NewMemoryCacheBackend[string](10)
	ctx := context.Background()

	// Set with very short TTL
	err := backend.Set(ctx, "expiring", "value", 1*time.Millisecond)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Wait for expiration
	time.Sleep(5 * time.Millisecond)

	_, found, err := backend.Get(ctx, "expiring")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if found {
		t.Error("Expected expired value to not be found")
	}
}

func TestMemoryCacheBackend_Delete(t *testing.T) {
	backend := NewMemoryCacheBackend[string](10)
	ctx := context.Background()

	// Set and verify
	backend.Set(ctx, "deleteme", "value", 5*time.Minute)
	_, found, _ := backend.Get(ctx, "deleteme")
	if !found {
		t.Error("Expected to find value before delete")
	}

	// Delete and verify
	err := backend.Delete(ctx, "deleteme")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, found, _ = backend.Get(ctx, "deleteme")
	if found {
		t.Error("Expected not to find deleted value")
	}
}

func TestMemoryCacheBackend_Clear(t *testing.T) {
	backend := NewMemoryCacheBackend[string](10)
	ctx := context.Background()

	// Add multiple values
	backend.Set(ctx, "key1", "value1", 5*time.Minute)
	backend.Set(ctx, "key2", "value2", 5*time.Minute)
	backend.Set(ctx, "key3", "value3", 5*time.Minute)

	// Clear all
	err := backend.Clear(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Verify all are gone
	_, found1, _ := backend.Get(ctx, "key1")
	_, found2, _ := backend.Get(ctx, "key2")
	_, found3, _ := backend.Get(ctx, "key3")

	if found1 || found2 || found3 {
		t.Error("Expected all values to be cleared")
	}
}

func TestMemoryCacheBackend_Eviction(t *testing.T) {
	backend := NewMemoryCacheBackend[string](2) // Very small max size
	ctx := context.Background()

	// Add values up to max size
	backend.Set(ctx, "key1", "value1", 5*time.Minute)
	backend.Set(ctx, "key2", "value2", 5*time.Minute)

	// Both should be present
	_, found1, _ := backend.Get(ctx, "key1")
	_, found2, _ := backend.Get(ctx, "key2")
	if !found1 || !found2 {
		t.Error("Expected both values to be present before eviction")
	}

	// Add third value, should trigger eviction
	backend.Set(ctx, "key3", "value3", 5*time.Minute)

	// Check stats for evictions
	stats := backend.Stats()
	if stats.Evictions == 0 {
		t.Error("Expected at least one eviction")
	}
}

func TestMemoryCacheBackend_Stats(t *testing.T) {
	backend := NewMemoryCacheBackend[string](10)
	ctx := context.Background()

	// Add some values
	backend.Set(ctx, "key1", "value1", 5*time.Minute)
	backend.Set(ctx, "key2", "value2", 5*time.Minute)

	// Generate some hits and misses
	backend.Get(ctx, "key1")      // hit
	backend.Get(ctx, "key1")      // hit
	backend.Get(ctx, "missing")   // miss

	stats := backend.Stats()
	if stats.Size != 2 {
		t.Errorf("Expected size 2, got %d", stats.Size)
	}
	if stats.HitCount != 2 {
		t.Errorf("Expected 2 hits, got %d", stats.HitCount)
	}
	if stats.MissCount != 1 {
		t.Errorf("Expected 1 miss, got %d", stats.MissCount)
	}
}

func TestQueryCache_Basic(t *testing.T) {
	backend := NewMemoryCacheBackend[CachedUser](10)
	config := &QueryCacheConfig{
		TTL:           5 * time.Minute,
		Enabled:       true,
		EnableTracing: false, // Disable for simpler test
	}
	cache := NewQueryCache(config, backend)

	ctx := context.Background()
	loadCount := 0

	loader := func(ctx context.Context) (CachedUser, error) {
		loadCount++
		return CachedUser{ID: 1, Name: fmt.Sprintf("User-%d", loadCount)}, nil
	}

	// First call should load
	user1, err := cache.Get(ctx, "user:1", loader)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if user1.Name != "User-1" {
		t.Errorf("Expected 'User-1', got '%s'", user1.Name)
	}

	if loadCount != 1 {
		t.Errorf("Expected loader called once, called %d times", loadCount)
	}

	// Second call should use cache
	user2, err := cache.Get(ctx, "user:1", loader)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if user2.Name != "User-1" {
		t.Errorf("Expected cached 'User-1', got '%s'", user2.Name)
	}

	if loadCount != 1 {
		t.Errorf("Expected loader called once (cached), called %d times", loadCount)
	}

	// Verify stats
	stats := cache.Stats()
	if stats.HitCount != 1 {
		t.Errorf("Expected 1 hit, got %d", stats.HitCount)
	}
	if stats.MissCount != 1 {
		t.Errorf("Expected 1 miss, got %d", stats.MissCount)
	}
}

func TestQueryCache_LoaderError(t *testing.T) {
	backend := NewMemoryCacheBackend[CachedUser](10)
	config := &QueryCacheConfig{
		TTL:           5 * time.Minute,
		Enabled:       true,
		EnableTracing: false,
	}
	cache := NewQueryCache(config, backend)

	ctx := context.Background()
	expectedError := fmt.Errorf("load error")

	loader := func(ctx context.Context) (CachedUser, error) {
		return CachedUser{}, expectedError
	}

	// Should return loader error
	_, err := cache.Get(ctx, "user:error", loader)
	if err != expectedError {
		t.Errorf("Expected error %v, got %v", expectedError, err)
	}
}

func TestQueryCache_Disabled(t *testing.T) {
	backend := NewMemoryCacheBackend[CachedUser](10)
	config := &QueryCacheConfig{
		TTL:           5 * time.Minute,
		Enabled:       false, // Disabled cache
		EnableTracing: false,
	}
	cache := NewQueryCache(config, backend)

	ctx := context.Background()
	loadCount := 0

	loader := func(ctx context.Context) (CachedUser, error) {
		loadCount++
		return CachedUser{ID: loadCount, Name: fmt.Sprintf("User-%d", loadCount)}, nil
	}

	// Both calls should execute loader
	user1, err := cache.Get(ctx, "user:1", loader)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	user2, err := cache.Get(ctx, "user:1", loader)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if user1.ID == user2.ID {
		t.Error("Expected different users when cache is disabled")
	}

	if loadCount != 2 {
		t.Errorf("Expected loader called twice, called %d times", loadCount)
	}
}

func TestQueryCache_SetAndDelete(t *testing.T) {
	backend := NewMemoryCacheBackend[CachedUser](10)
	config := &QueryCacheConfig{
		TTL:           5 * time.Minute,
		Enabled:       true,
		EnableTracing: false,
	}
	cache := NewQueryCache(config, backend)

	ctx := context.Background()
	user := CachedUser{ID: 1, Name: "Test User"}

	// Directly set in cache
	err := cache.Set(ctx, "user:direct", user)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Should be able to retrieve without loader
	loader := func(ctx context.Context) (CachedUser, error) {
		t.Error("Loader should not be called")
		return CachedUser{}, nil
	}

	retrieved, err := cache.Get(ctx, "user:direct", loader)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if retrieved.Name != user.Name {
		t.Errorf("Expected '%s', got '%s'", user.Name, retrieved.Name)
	}

	// Delete from cache
	err = cache.Delete(ctx, "user:direct")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Should now call loader
	loadCalled := false
	loader = func(ctx context.Context) (CachedUser, error) {
		loadCalled = true
		return CachedUser{ID: 2, Name: "Loaded User"}, nil
	}

	_, err = cache.Get(ctx, "user:direct", loader)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if !loadCalled {
		t.Error("Expected loader to be called after delete")
	}
}

func TestQueryCache_InvalidationCallback(t *testing.T) {
	backend := NewMemoryCacheBackend[CachedUser](10)
	config := &QueryCacheConfig{
		TTL:           5 * time.Minute,
		Enabled:       true,
		EnableTracing: false,
	}
	cache := NewQueryCache(config, backend)

	ctx := context.Background()
	callbackCalled := false

	cache.AddInvalidationCallback("user:1", func() {
		callbackCalled = true
	})

	// Set a value
	cache.Set(ctx, "user:1", CachedUser{ID: 1, Name: "Test"})

	// Delete should trigger callback
	cache.Delete(ctx, "user:1")

	if !callbackCalled {
		t.Error("Expected invalidation callback to be called")
	}
}

func TestQueryCache_Concurrent(t *testing.T) {
	backend := NewMemoryCacheBackend[CachedUser](10)
	config := &QueryCacheConfig{
		TTL:           5 * time.Minute,
		Enabled:       true,
		EnableTracing: false,
	}
	cache := NewQueryCache(config, backend)

	ctx := context.Background()
	loadCount := 0
	var loadMu sync.Mutex

	loader := func(ctx context.Context) (CachedUser, error) {
		loadMu.Lock()
		loadCount++
		count := loadCount
		loadMu.Unlock()

		// Simulate slow load
		time.Sleep(10 * time.Millisecond)
		return CachedUser{ID: count, Name: fmt.Sprintf("User-%d", count)}, nil
	}

	// Launch multiple goroutines
	var wg sync.WaitGroup
	results := make([]CachedUser, 10)
	errors := make([]error, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			user, err := cache.Get(ctx, "user:concurrent", loader)
			results[index] = user
			errors[index] = err
		}(i)
	}

	wg.Wait()

	// Check that all goroutines got the same result
	for i, err := range errors {
		if err != nil {
			t.Errorf("Goroutine %d got error: %v", i, err)
		}
	}

	firstResult := results[0]
	for i, result := range results {
		if result.ID != firstResult.ID || result.Name != firstResult.Name {
			t.Errorf("Goroutine %d got different result: %+v vs %+v", i, result, firstResult)
		}
	}

	// Due to concurrent access, loader might be called more than once
	// but it should be minimal
	if loadCount > 3 {
		t.Errorf("Expected minimal loader calls, got %d", loadCount)
	}
}

func TestGenerateKey(t *testing.T) {
	// Same inputs should generate same key
	key1 := GenerateKey("SELECT * FROM users WHERE id = ?", 1)
	key2 := GenerateKey("SELECT * FROM users WHERE id = ?", 1)

	if key1 != key2 {
		t.Error("Expected same keys for same inputs")
	}

	// Different inputs should generate different keys
	key3 := GenerateKey("SELECT * FROM users WHERE id = ?", 2)
	if key1 == key3 {
		t.Error("Expected different keys for different inputs")
	}

	// Different queries should generate different keys
	key4 := GenerateKey("SELECT * FROM posts WHERE user_id = ?", 1)
	if key1 == key4 {
		t.Error("Expected different keys for different queries")
	}
}

func TestQueryCacheManager(t *testing.T) {
	config := &QueryCacheConfig{
		TTL:           5 * time.Minute,
		Enabled:       true,
		EnableTracing: false,
	}
	manager := NewQueryCacheManager(config)

	// Get cache for users
	userBackend := NewMemoryCacheBackend[CachedUser](10)
	userCache := GetQueryCache(manager, "users", userBackend)

	// Get cache for posts
	postBackend := NewMemoryCacheBackend[CachedPost](10)
	postCache := GetQueryCache(manager, "posts", postBackend)

	ctx := context.Background()

	// Use both caches
	userCache.Set(ctx, "user:1", CachedUser{ID: 1, Name: "Test User"})
	postCache.Set(ctx, "post:1", CachedPost{ID: 1, Title: "Test Post", UserID: 1})

	// Get same caches again (should return existing)
	userCache2 := GetQueryCache(manager, "users", userBackend)
	if userCache != userCache2 {
		t.Error("Expected to get same cache instance")
	}

	// Get all stats
	allStats := manager.GetAllStats()
	if len(allStats) != 2 {
		t.Errorf("Expected 2 caches, got %d", len(allStats))
	}

	if _, exists := allStats["users"]; !exists {
		t.Error("Expected users cache in stats")
	}

	if _, exists := allStats["posts"]; !exists {
		t.Error("Expected posts cache in stats")
	}
}

func BenchmarkQueryCache_Get(b *testing.B) {
	backend := NewMemoryCacheBackend[CachedUser](1000)
	config := &QueryCacheConfig{
		TTL:           5 * time.Minute,
		Enabled:       true,
		EnableTracing: false,
	}
	cache := NewQueryCache(config, backend)

	ctx := context.Background()
	loader := func(ctx context.Context) (CachedUser, error) {
		return CachedUser{ID: 1, Name: "Bench User"}, nil
	}

	// Pre-warm cache
	cache.Get(ctx, "bench:user", loader)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Get(ctx, "bench:user", loader)
	}
}

func BenchmarkQueryCache_GetConcurrent(b *testing.B) {
	backend := NewMemoryCacheBackend[CachedUser](1000)
	config := &QueryCacheConfig{
		TTL:           5 * time.Minute,
		Enabled:       true,
		EnableTracing: false,
	}
	cache := NewQueryCache(config, backend)

	ctx := context.Background()
	loader := func(ctx context.Context) (CachedUser, error) {
		time.Sleep(1 * time.Millisecond) // Simulate slow load
		return CachedUser{ID: 1, Name: "Bench User"}, nil
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cache.Get(ctx, "bench:concurrent", loader)
		}
	})
}