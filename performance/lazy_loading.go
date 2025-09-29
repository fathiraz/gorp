package performance

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/fathiraz/gorp/db"
)

// LazyLoader provides generic lazy loading capabilities with type safety
type LazyLoader[T any] struct {
	loaded    bool
	value     T
	loader    func(ctx context.Context) (T, error)
	mu        sync.RWMutex
	tracer    trace.Tracer
	loadTime  time.Time
	accessCount int64
}

// LazyCollection provides lazy loading for collections with pagination support
type LazyCollection[T any] struct {
	loaded      bool
	items       []T
	totalCount  int
	pageSize    int
	currentPage int
	loader      CollectionLoader[T]
	mu          sync.RWMutex
	tracer      trace.Tracer
	loadTime    time.Time
	accessCount int64
	cache       map[int][]T // page number to items cache
}

// CollectionLoader defines the interface for loading collections
type CollectionLoader[T any] interface {
	LoadPage(ctx context.Context, page, pageSize int) ([]T, int, error)
	Count(ctx context.Context) (int, error)
}

// RelationLoader implements lazy loading for database relations
type RelationLoader[T any] struct {
	conn      db.Connection
	query     string
	params    []interface{}
	mapper    func(*sql.Rows) (T, error)
	tracer    trace.Tracer
}

// NewLazyLoader creates a new lazy loader with the given loader function
func NewLazyLoader[T any](loader func(ctx context.Context) (T, error)) *LazyLoader[T] {
	return &LazyLoader[T]{
		loader: loader,
		tracer: otel.Tracer("gorp.lazy_loader"),
	}
}

// Get returns the loaded value, loading it if necessary
func (ll *LazyLoader[T]) Get(ctx context.Context) (T, error) {
	// Fast path for already loaded values
	ll.mu.RLock()
	if ll.loaded {
		ll.accessCount++
		value := ll.value
		ll.mu.RUnlock()
		return value, nil
	}
	ll.mu.RUnlock()

	// Slow path with write lock
	ll.mu.Lock()
	defer ll.mu.Unlock()

	// Double-check pattern
	if ll.loaded {
		ll.accessCount++
		return ll.value, nil
	}

	// Create span for lazy loading operation
	ctx, span := ll.tracer.Start(ctx, "lazy_loader.get",
		trace.WithAttributes(
			attribute.String("loader.type", fmt.Sprintf("%T", *new(T))),
		))
	defer span.End()

	start := time.Now()
	value, err := ll.loader(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to load value")
		return value, err
	}

	ll.value = value
	ll.loaded = true
	ll.loadTime = time.Now()
	ll.accessCount = 1

	span.SetAttributes(
		attribute.Int64("loader.load_duration_ms", time.Since(start).Milliseconds()),
		attribute.Bool("loader.success", true),
	)
	span.SetStatus(codes.Ok, "Value loaded successfully")

	return value, nil
}

// IsLoaded returns true if the value has been loaded
func (ll *LazyLoader[T]) IsLoaded() bool {
	ll.mu.RLock()
	defer ll.mu.RUnlock()
	return ll.loaded
}

// Reset clears the loaded value, forcing reload on next access
func (ll *LazyLoader[T]) Reset() {
	ll.mu.Lock()
	defer ll.mu.Unlock()
	ll.loaded = false
	var zero T
	ll.value = zero
}

// Stats returns loading statistics
func (ll *LazyLoader[T]) Stats() LazyLoaderStats {
	ll.mu.RLock()
	defer ll.mu.RUnlock()

	return LazyLoaderStats{
		Loaded:      ll.loaded,
		LoadTime:    ll.loadTime,
		AccessCount: ll.accessCount,
	}
}

// NewLazyCollection creates a new lazy collection with pagination support
func NewLazyCollection[T any](loader CollectionLoader[T], pageSize int) *LazyCollection[T] {
	return &LazyCollection[T]{
		loader:   loader,
		pageSize: pageSize,
		cache:    make(map[int][]T),
		tracer:   otel.Tracer("gorp.lazy_collection"),
	}
}

// GetPage returns items for the specified page, loading if necessary
func (lc *LazyCollection[T]) GetPage(ctx context.Context, page int) ([]T, error) {
	lc.mu.RLock()
	if items, exists := lc.cache[page]; exists {
		lc.accessCount++
		lc.mu.RUnlock()
		return items, nil
	}
	lc.mu.RUnlock()

	// Load page with write lock
	lc.mu.Lock()
	defer lc.mu.Unlock()

	// Double-check pattern
	if items, exists := lc.cache[page]; exists {
		lc.accessCount++
		return items, nil
	}

	// Create span for collection loading
	ctx, span := lc.tracer.Start(ctx, "lazy_collection.get_page",
		trace.WithAttributes(
			attribute.String("collection.type", fmt.Sprintf("%T", *new(T))),
			attribute.Int("collection.page", page),
			attribute.Int("collection.page_size", lc.pageSize),
		))
	defer span.End()

	start := time.Now()
	items, totalCount, err := lc.loader.LoadPage(ctx, page, lc.pageSize)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to load page")
		return nil, err
	}

	lc.cache[page] = items
	lc.totalCount = totalCount
	lc.currentPage = page
	lc.loadTime = time.Now()
	lc.accessCount++

	span.SetAttributes(
		attribute.Int64("collection.load_duration_ms", time.Since(start).Milliseconds()),
		attribute.Int("collection.items_loaded", len(items)),
		attribute.Int("collection.total_count", totalCount),
	)
	span.SetStatus(codes.Ok, "Page loaded successfully")

	return items, nil
}

// GetAll returns all items, loading them in chunks if necessary
func (lc *LazyCollection[T]) GetAll(ctx context.Context) ([]T, error) {
	// First get count
	totalCount, err := lc.Count(ctx)
	if err != nil {
		return nil, err
	}

	if totalCount == 0 {
		return []T{}, nil
	}

	totalPages := (totalCount + lc.pageSize - 1) / lc.pageSize
	var allItems []T

	for page := 0; page < totalPages; page++ {
		items, err := lc.GetPage(ctx, page)
		if err != nil {
			return nil, err
		}
		allItems = append(allItems, items...)
	}

	return allItems, nil
}

// Count returns the total count of items in the collection
func (lc *LazyCollection[T]) Count(ctx context.Context) (int, error) {
	lc.mu.RLock()
	if lc.totalCount > 0 {
		count := lc.totalCount
		lc.mu.RUnlock()
		return count, nil
	}
	lc.mu.RUnlock()

	lc.mu.Lock()
	defer lc.mu.Unlock()

	// Double-check pattern
	if lc.totalCount > 0 {
		return lc.totalCount, nil
	}

	count, err := lc.loader.Count(ctx)
	if err != nil {
		return 0, err
	}

	lc.totalCount = count
	return count, nil
}

// ClearCache clears the page cache, forcing reload on next access
func (lc *LazyCollection[T]) ClearCache() {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.cache = make(map[int][]T)
	lc.loaded = false
}

// Stats returns collection loading statistics
func (lc *LazyCollection[T]) Stats() LazyCollectionStats {
	lc.mu.RLock()
	defer lc.mu.RUnlock()

	return LazyCollectionStats{
		TotalCount:   lc.totalCount,
		PagesLoaded:  len(lc.cache),
		AccessCount:  lc.accessCount,
		PageSize:     lc.pageSize,
		LoadTime:     lc.loadTime,
	}
}

// NewRelationLoader creates a new relation loader for database queries
func NewRelationLoader[T any](conn db.Connection, query string, params []interface{}, mapper func(*sql.Rows) (T, error)) *RelationLoader[T] {
	return &RelationLoader[T]{
		conn:   conn,
		query:  query,
		params: params,
		mapper: mapper,
		tracer: otel.Tracer("gorp.relation_loader"),
	}
}

// Load executes the query and maps results to type T
func (rl *RelationLoader[T]) Load(ctx context.Context) (T, error) {
	var result T

	ctx, span := rl.tracer.Start(ctx, "relation_loader.load",
		trace.WithAttributes(
			attribute.String("db.statement", rl.query[:min(100, len(rl.query))]),
			attribute.String("relation.type", fmt.Sprintf("%T", result)),
		))
	defer span.End()

	// Note: This would need to be adapted to use the actual connection interface
	// For now, this is a placeholder that demonstrates the pattern
	span.SetStatus(codes.Error, "Connection interface needs implementation")
	return result, fmt.Errorf("connection interface not yet implemented")
}

// LoadCollection executes the query and maps results to a slice of type T
func (rl *RelationLoader[T]) LoadCollection(ctx context.Context) ([]T, error) {
	ctx, span := rl.tracer.Start(ctx, "relation_loader.load_collection",
		trace.WithAttributes(
			attribute.String("db.statement", rl.query[:min(100, len(rl.query))]),
			attribute.String("relation.type", fmt.Sprintf("%T", *new(T))),
		))
	defer span.End()

	// Note: This would need to be adapted to use the actual connection interface
	// For now, this is a placeholder that demonstrates the pattern
	span.SetStatus(codes.Error, "Connection interface needs implementation")
	return nil, fmt.Errorf("connection interface not yet implemented")
}

// LazyRelation provides lazy loading for database relations with caching
type LazyRelation[T any] struct {
	*LazyLoader[[]T]
	relationLoader *RelationLoader[T]
}

// NewLazyRelation creates a new lazy relation
func NewLazyRelation[T any](conn db.Connection, query string, params []interface{}, mapper func(*sql.Rows) (T, error)) *LazyRelation[T] {
	relationLoader := NewRelationLoader(conn, query, params, mapper)

	loader := func(ctx context.Context) ([]T, error) {
		return relationLoader.LoadCollection(ctx)
	}

	return &LazyRelation[T]{
		LazyLoader:     NewLazyLoader(loader),
		relationLoader: relationLoader,
	}
}

// GetSingle loads and returns a single item from the relation
func (lr *LazyRelation[T]) GetSingle(ctx context.Context) (T, error) {
	return lr.relationLoader.Load(ctx)
}

// Statistics structs
type LazyLoaderStats struct {
	Loaded      bool
	LoadTime    time.Time
	AccessCount int64
}

type LazyCollectionStats struct {
	TotalCount   int
	PagesLoaded  int
	AccessCount  int64
	PageSize     int
	LoadTime     time.Time
}

// Helper function for string truncation
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// LazyField provides lazy loading for struct fields using reflection
type LazyField struct {
	fieldName string
	loader    func(ctx context.Context, parent interface{}) (interface{}, error)
	value     interface{}
	loaded    bool
	mu        sync.RWMutex
}

// NewLazyField creates a new lazy field
func NewLazyField(fieldName string, loader func(ctx context.Context, parent interface{}) (interface{}, error)) *LazyField {
	return &LazyField{
		fieldName: fieldName,
		loader:    loader,
	}
}

// Load loads the field value if not already loaded
func (lf *LazyField) Load(ctx context.Context, parent interface{}) error {
	lf.mu.RLock()
	if lf.loaded {
		lf.mu.RUnlock()
		return nil
	}
	lf.mu.RUnlock()

	lf.mu.Lock()
	defer lf.mu.Unlock()

	// Double-check pattern
	if lf.loaded {
		return nil
	}

	value, err := lf.loader(ctx, parent)
	if err != nil {
		return err
	}

	lf.value = value
	lf.loaded = true

	// Use reflection to set the field value in the parent struct
	parentValue := reflect.ValueOf(parent)
	if parentValue.Kind() == reflect.Ptr {
		parentValue = parentValue.Elem()
	}

	fieldValue := parentValue.FieldByName(lf.fieldName)
	if fieldValue.IsValid() && fieldValue.CanSet() {
		fieldValue.Set(reflect.ValueOf(value))
	}

	return nil
}

// GetValue returns the loaded value
func (lf *LazyField) GetValue() interface{} {
	lf.mu.RLock()
	defer lf.mu.RUnlock()
	return lf.value
}

// IsLoaded returns true if the field has been loaded
func (lf *LazyField) IsLoaded() bool {
	lf.mu.RLock()
	defer lf.mu.RUnlock()
	return lf.loaded
}