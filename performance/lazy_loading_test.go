package performance

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"
)

// Mock types for testing
type User struct {
	ID       int
	Name     string
	Email    string
	Posts    *LazyRelation[Post]
	Profile  *LazyLoader[Profile]
}

type Post struct {
	ID     int
	Title  string
	UserID int
}

type Profile struct {
	ID     int
	Bio    string
	UserID int
}

// Mock collection loader
type mockCollectionLoader struct {
	data       []Post
	loadDelay  time.Duration
	errorOnLoad bool
}

func (m *mockCollectionLoader) LoadPage(ctx context.Context, page, pageSize int) ([]Post, int, error) {
	if m.errorOnLoad {
		return nil, 0, fmt.Errorf("mock load error")
	}

	if m.loadDelay > 0 {
		time.Sleep(m.loadDelay)
	}

	start := page * pageSize
	end := start + pageSize

	if start >= len(m.data) {
		return []Post{}, len(m.data), nil
	}

	if end > len(m.data) {
		end = len(m.data)
	}

	return m.data[start:end], len(m.data), nil
}

func (m *mockCollectionLoader) Count(ctx context.Context) (int, error) {
	if m.errorOnLoad {
		return 0, fmt.Errorf("mock count error")
	}
	return len(m.data), nil
}

// Mock connection for lazy loading tests
type mockLazyConnection struct {
	queryError bool
	noRows     bool
}

func (m *mockLazyConnection) QueryContext(ctx context.Context, query string, params ...interface{}) (*sql.Rows, error) {
	if m.queryError {
		return nil, fmt.Errorf("mock query error")
	}
	// This is a simplified mock - in real tests you'd use sqlmock or similar
	return nil, nil
}

// countingCollectionLoader wraps a loader to count calls
type countingCollectionLoader struct {
	inner CollectionLoader[Post]
	count *int
}

func (c *countingCollectionLoader) LoadPage(ctx context.Context, page, pageSize int) ([]Post, int, error) {
	*c.count++
	return c.inner.LoadPage(ctx, page, pageSize)
}

func (c *countingCollectionLoader) Count(ctx context.Context) (int, error) {
	return c.inner.Count(ctx)
}

func TestLazyLoader_Basic(t *testing.T) {
	loadCount := 0
	loader := func(ctx context.Context) (string, error) {
		loadCount++
		return fmt.Sprintf("loaded-%d", loadCount), nil
	}

	ll := NewLazyLoader(loader)

	// Test initial state
	if ll.IsLoaded() {
		t.Error("Expected loader to not be loaded initially")
	}

	// Test first load
	ctx := context.Background()
	value, err := ll.Get(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if value != "loaded-1" {
		t.Errorf("Expected 'loaded-1', got %s", value)
	}

	if !ll.IsLoaded() {
		t.Error("Expected loader to be loaded after Get()")
	}

	// Test cached load
	value2, err := ll.Get(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if value2 != "loaded-1" {
		t.Errorf("Expected cached value 'loaded-1', got %s", value2)
	}

	if loadCount != 1 {
		t.Errorf("Expected loader to be called once, called %d times", loadCount)
	}

	// Test stats
	stats := ll.Stats()
	if !stats.Loaded {
		t.Error("Expected stats to show loaded")
	}
	if stats.AccessCount != 2 {
		t.Errorf("Expected access count 2, got %d", stats.AccessCount)
	}
}

func TestLazyLoader_Error(t *testing.T) {
	expectedErr := fmt.Errorf("load error")
	loader := func(ctx context.Context) (string, error) {
		return "", expectedErr
	}

	ll := NewLazyLoader(loader)
	ctx := context.Background()

	value, err := ll.Get(ctx)
	if err != expectedErr {
		t.Errorf("Expected error %v, got %v", expectedErr, err)
	}

	if value != "" {
		t.Errorf("Expected empty value on error, got %s", value)
	}

	if ll.IsLoaded() {
		t.Error("Expected loader to not be loaded after error")
	}
}

func TestLazyLoader_Reset(t *testing.T) {
	loadCount := 0
	loader := func(ctx context.Context) (string, error) {
		loadCount++
		return fmt.Sprintf("loaded-%d", loadCount), nil
	}

	ll := NewLazyLoader(loader)
	ctx := context.Background()

	// Load first time
	value1, err := ll.Get(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Reset and load again
	ll.Reset()
	if ll.IsLoaded() {
		t.Error("Expected loader to not be loaded after reset")
	}

	value2, err := ll.Get(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if value1 == value2 {
		t.Error("Expected different values after reset")
	}

	if loadCount != 2 {
		t.Errorf("Expected loader to be called twice, called %d times", loadCount)
	}
}

func TestLazyLoader_Concurrent(t *testing.T) {
	loadCount := 0
	var loadMu sync.Mutex

	loader := func(ctx context.Context) (string, error) {
		loadMu.Lock()
		loadCount++
		count := loadCount
		loadMu.Unlock()

		// Simulate slow load
		time.Sleep(10 * time.Millisecond)
		return fmt.Sprintf("loaded-%d", count), nil
	}

	ll := NewLazyLoader(loader)
	ctx := context.Background()

	// Launch multiple goroutines
	var wg sync.WaitGroup
	results := make([]string, 10)
	errors := make([]error, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			value, err := ll.Get(ctx)
			results[index] = value
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
		if result != firstResult {
			t.Errorf("Goroutine %d got different result: %s vs %s", i, result, firstResult)
		}
	}

	// Check that loader was called only once
	if loadCount != 1 {
		t.Errorf("Expected loader to be called once, called %d times", loadCount)
	}
}

func TestLazyCollection_Basic(t *testing.T) {
	// Create test data
	testData := make([]Post, 25)
	for i := 0; i < 25; i++ {
		testData[i] = Post{
			ID:     i + 1,
			Title:  fmt.Sprintf("Post %d", i+1),
			UserID: 1,
		}
	}

	loader := &mockCollectionLoader{data: testData}
	lc := NewLazyCollection[Post](loader, 10)

	ctx := context.Background()

	// Test count
	count, err := lc.Count(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if count != 25 {
		t.Errorf("Expected count 25, got %d", count)
	}

	// Test first page
	page0, err := lc.GetPage(ctx, 0)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(page0) != 10 {
		t.Errorf("Expected page size 10, got %d", len(page0))
	}

	// Test second page
	page1, err := lc.GetPage(ctx, 1)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(page1) != 10 {
		t.Errorf("Expected page size 10, got %d", len(page1))
	}

	// Test last page
	page2, err := lc.GetPage(ctx, 2)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(page2) != 5 {
		t.Errorf("Expected page size 5 for last page, got %d", len(page2))
	}

	// Test stats
	stats := lc.Stats()
	if stats.TotalCount != 25 {
		t.Errorf("Expected total count 25, got %d", stats.TotalCount)
	}
	if stats.PagesLoaded != 3 {
		t.Errorf("Expected 3 pages loaded, got %d", stats.PagesLoaded)
	}
	if stats.PageSize != 10 {
		t.Errorf("Expected page size 10, got %d", stats.PageSize)
	}
}

func TestLazyCollection_GetAll(t *testing.T) {
	testData := make([]Post, 15)
	for i := 0; i < 15; i++ {
		testData[i] = Post{
			ID:     i + 1,
			Title:  fmt.Sprintf("Post %d", i+1),
			UserID: 1,
		}
	}

	loader := &mockCollectionLoader{data: testData}
	lc := NewLazyCollection[Post](loader, 7) // Use odd page size

	ctx := context.Background()
	allItems, err := lc.GetAll(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(allItems) != 15 {
		t.Errorf("Expected 15 items, got %d", len(allItems))
	}

	// Verify items are in correct order
	for i, item := range allItems {
		if item.ID != i+1 {
			t.Errorf("Expected item %d to have ID %d, got %d", i, i+1, item.ID)
		}
	}
}

func TestLazyCollection_Error(t *testing.T) {
	loader := &mockCollectionLoader{errorOnLoad: true}
	lc := NewLazyCollection[Post](loader, 10)

	ctx := context.Background()

	// Test count error
	_, err := lc.Count(ctx)
	if err == nil {
		t.Error("Expected error from Count()")
	}

	// Test page error
	_, err = lc.GetPage(ctx, 0)
	if err == nil {
		t.Error("Expected error from GetPage()")
	}
}

func TestLazyCollection_Cache(t *testing.T) {
	loadCallCount := 0
	testData := make([]Post, 10)
	for i := 0; i < 10; i++ {
		testData[i] = Post{
			ID:     i + 1,
			Title:  fmt.Sprintf("Post %d", i+1),
			UserID: 1,
		}
	}

	loader := &mockCollectionLoader{
		data: testData,
		loadDelay: 1 * time.Millisecond, // Small delay to detect multiple calls
	}

	// Create wrapper loader with call counting
	wrapper := &countingCollectionLoader{
		inner: loader,
		count: &loadCallCount,
	}

	lc := NewLazyCollection[Post](wrapper, 10)
	ctx := context.Background()

	// Load same page twice
	_, err := lc.GetPage(ctx, 0)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, err = lc.GetPage(ctx, 0)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if loadCallCount != 1 {
		t.Errorf("Expected LoadPage to be called once, called %d times", loadCallCount)
	}

	// Clear cache and load again
	lc.ClearCache()
	_, err = lc.GetPage(ctx, 0)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if loadCallCount != 2 {
		t.Errorf("Expected LoadPage to be called twice after cache clear, called %d times", loadCallCount)
	}
}

func TestLazyField_Basic(t *testing.T) {
	type TestStruct struct {
		ID   int
		Name string
	}

	loadCount := 0
	loader := func(ctx context.Context, parent interface{}) (interface{}, error) {
		loadCount++
		return "loaded-name", nil
	}

	lf := NewLazyField("Name", loader)
	testObj := &TestStruct{ID: 1}

	ctx := context.Background()

	// Test initial state
	if lf.IsLoaded() {
		t.Error("Expected field to not be loaded initially")
	}

	// Test load
	err := lf.Load(ctx, testObj)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if !lf.IsLoaded() {
		t.Error("Expected field to be loaded after Load()")
	}

	// Test value was set
	if testObj.Name != "loaded-name" {
		t.Errorf("Expected field value 'loaded-name', got '%s'", testObj.Name)
	}

	// Test double load doesn't call loader again
	err = lf.Load(ctx, testObj)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if loadCount != 1 {
		t.Errorf("Expected loader to be called once, called %d times", loadCount)
	}
}

func BenchmarkLazyLoader_Get(b *testing.B) {
	loader := func(ctx context.Context) (string, error) {
		return "test-value", nil
	}

	ll := NewLazyLoader(loader)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ll.Get(ctx)
	}
}

func BenchmarkLazyLoader_GetConcurrent(b *testing.B) {
	loader := func(ctx context.Context) (string, error) {
		time.Sleep(1 * time.Millisecond) // Simulate slow load
		return "test-value", nil
	}

	ll := NewLazyLoader(loader)
	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ll.Get(ctx)
		}
	})
}

func BenchmarkLazyCollection_GetPage(b *testing.B) {
	testData := make([]Post, 1000)
	for i := 0; i < 1000; i++ {
		testData[i] = Post{
			ID:     i + 1,
			Title:  fmt.Sprintf("Post %d", i+1),
			UserID: 1,
		}
	}

	loader := &mockCollectionLoader{data: testData}
	lc := NewLazyCollection[Post](loader, 10)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lc.GetPage(ctx, i%100) // Cycle through pages
	}
}