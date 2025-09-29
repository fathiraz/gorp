package performance

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/fathiraz/gorp/db"
)

// BatchOperations provides high-performance batch operations with optimal sizing
type BatchOperations[T any] struct {
	conn           db.Connection
	tableName      string
	batchSize      int
	maxRetries     int
	retryDelay     time.Duration
	tracer         trace.Tracer
	fieldMapping   map[string]string // Go field name -> DB column name
	insertQuery    string           // Cached insert query
	updateQuery    string           // Cached update query
	deleteQuery    string           // Cached delete query
}

// BatchConfig holds configuration for batch operations
type BatchConfig struct {
	BatchSize      int           // Number of items per batch
	MaxRetries     int           // Maximum retry attempts for failed batches
	RetryDelay     time.Duration // Delay between retries
	EnableTracing  bool          // Enable OpenTelemetry tracing
	TracerName     string        // Name for the tracer
}

// DefaultBatchConfig returns sensible defaults
func DefaultBatchConfig() *BatchConfig {
	return &BatchConfig{
		BatchSize:     1000,
		MaxRetries:    3,
		RetryDelay:    100 * time.Millisecond,
		EnableTracing: true,
		TracerName:    "gorp.batch_operations",
	}
}

// NewBatchOperations creates a new batch operations manager for type T
func NewBatchOperations[T any](conn db.Connection, tableName string, config *BatchConfig) *BatchOperations[T] {
	if config == nil {
		config = DefaultBatchConfig()
	}

	var tracer trace.Tracer
	if config.EnableTracing {
		tracer = otel.Tracer(config.TracerName)
	}

	bo := &BatchOperations[T]{
		conn:         conn,
		tableName:    tableName,
		batchSize:    config.BatchSize,
		maxRetries:   config.MaxRetries,
		retryDelay:   config.RetryDelay,
		tracer:       tracer,
		fieldMapping: make(map[string]string),
	}

	// Analyze struct for field mapping
	bo.analyzeStruct()

	return bo
}

// BatchInsert inserts multiple records in optimized batches
func (bo *BatchOperations[T]) BatchInsert(ctx context.Context, items []T) (*BatchResult, error) {
	if len(items) == 0 {
		return &BatchResult{}, nil
	}

	// Create span for batch insert operation
	var span trace.Span
	if bo.tracer != nil {
		ctx, span = bo.tracer.Start(ctx, "batch_operations.insert",
			trace.WithAttributes(
				attribute.String("batch.operation", "insert"),
				attribute.String("batch.table", bo.tableName),
				attribute.Int("batch.total_items", len(items)),
				attribute.Int("batch.batch_size", bo.batchSize),
			))
		defer span.End()
	}

	result := &BatchResult{
		Operation:    "INSERT",
		TotalItems:   len(items),
		BatchSize:    bo.batchSize,
		StartTime:    time.Now(),
	}

	// Process items in batches
	totalProcessed := 0
	for i := 0; i < len(items); i += bo.batchSize {
		end := i + bo.batchSize
		if end > len(items) {
			end = len(items)
		}

		batch := items[i:end]
		_, err := bo.insertBatch(ctx, batch, i/bo.batchSize+1)
		if err != nil {
			result.Errors = append(result.Errors, BatchError{
				BatchIndex: i / bo.batchSize,
				ItemCount:  len(batch),
				Error:      err,
			})

			if span != nil {
				span.RecordError(err)
			}

			// Continue with next batch unless it's a critical error
			if !isRetryableError(err) {
				break
			}
		} else {
			result.SuccessfulBatches++
			totalProcessed += len(batch)
		}
	}

	result.ProcessedItems = totalProcessed
	result.Duration = time.Since(result.StartTime)
	result.ThroughputPerSecond = float64(totalProcessed) / result.Duration.Seconds()

	if span != nil {
		span.SetAttributes(
			attribute.Int("batch.processed_items", totalProcessed),
			attribute.Int("batch.successful_batches", result.SuccessfulBatches),
			attribute.Int("batch.failed_batches", len(result.Errors)),
			attribute.Float64("batch.throughput_per_second", result.ThroughputPerSecond),
		)

		if len(result.Errors) == 0 {
			span.SetStatus(codes.Ok, "Batch insert completed successfully")
		} else {
			span.SetStatus(codes.Error, fmt.Sprintf("Batch insert completed with %d errors", len(result.Errors)))
		}
	}

	return result, nil
}

// BatchUpdate updates multiple records in optimized batches
func (bo *BatchOperations[T]) BatchUpdate(ctx context.Context, items []T, whereClause string, whereArgs ...interface{}) (*BatchResult, error) {
	if len(items) == 0 {
		return &BatchResult{}, nil
	}

	// Create span for batch update operation
	var span trace.Span
	if bo.tracer != nil {
		ctx, span = bo.tracer.Start(ctx, "batch_operations.update",
			trace.WithAttributes(
				attribute.String("batch.operation", "update"),
				attribute.String("batch.table", bo.tableName),
				attribute.Int("batch.total_items", len(items)),
				attribute.String("batch.where_clause", whereClause),
			))
		defer span.End()
	}

	result := &BatchResult{
		Operation:    "UPDATE",
		TotalItems:   len(items),
		BatchSize:    bo.batchSize,
		StartTime:    time.Now(),
	}

	// For updates, we need to process each item individually or use CASE statements
	// This is a simplified implementation; real-world might use bulk update strategies
	totalProcessed := 0
	for i := 0; i < len(items); i += bo.batchSize {
		end := i + bo.batchSize
		if end > len(items) {
			end = len(items)
		}

		batch := items[i:end]
		_, err := bo.updateBatch(ctx, batch, whereClause, whereArgs...)
		if err != nil {
			result.Errors = append(result.Errors, BatchError{
				BatchIndex: i / bo.batchSize,
				ItemCount:  len(batch),
				Error:      err,
			})

			if span != nil {
				span.RecordError(err)
			}
		} else {
			result.SuccessfulBatches++
			totalProcessed += len(batch)
		}
	}

	result.ProcessedItems = totalProcessed
	result.Duration = time.Since(result.StartTime)
	result.ThroughputPerSecond = float64(totalProcessed) / result.Duration.Seconds()

	if span != nil {
		span.SetAttributes(
			attribute.Int("batch.processed_items", totalProcessed),
			attribute.Float64("batch.throughput_per_second", result.ThroughputPerSecond),
		)
	}

	return result, nil
}

// BatchDelete deletes multiple records in optimized batches
func (bo *BatchOperations[T]) BatchDelete(ctx context.Context, whereClause string, whereArgs ...interface{}) (*BatchResult, error) {
	// Create span for batch delete operation
	var span trace.Span
	if bo.tracer != nil {
		ctx, span = bo.tracer.Start(ctx, "batch_operations.delete",
			trace.WithAttributes(
				attribute.String("batch.operation", "delete"),
				attribute.String("batch.table", bo.tableName),
				attribute.String("batch.where_clause", whereClause),
			))
		defer span.End()
	}

	result := &BatchResult{
		Operation: "DELETE",
		BatchSize: 1, // Delete operations are typically single queries
		StartTime: time.Now(),
	}

	// Execute delete with retries
	var deleteResult sql.Result
	var err error

	for attempt := 0; attempt <= bo.maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(bo.retryDelay)
		}

		query := fmt.Sprintf("DELETE FROM %s WHERE %s", bo.tableName, whereClause)
		deleteResult, err = bo.conn.Exec(ctx, query, whereArgs...)

		if err == nil {
			break
		}

		if !isRetryableError(err) {
			break
		}
	}

	if err != nil {
		result.Errors = append(result.Errors, BatchError{
			BatchIndex: 0,
			ItemCount:  0,
			Error:      err,
		})

		if span != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "Batch delete failed")
		}
	} else {
		rowsAffected, _ := deleteResult.RowsAffected()
		result.ProcessedItems = int(rowsAffected)
		result.TotalItems = int(rowsAffected)
		result.SuccessfulBatches = 1

		if span != nil {
			span.SetAttributes(attribute.Int64("batch.rows_affected", rowsAffected))
			span.SetStatus(codes.Ok, "Batch delete completed successfully")
		}
	}

	result.Duration = time.Since(result.StartTime)
	if result.Duration.Seconds() > 0 {
		result.ThroughputPerSecond = float64(result.ProcessedItems) / result.Duration.Seconds()
	}

	return result, nil
}

// OptimizeBatchSize dynamically adjusts batch size based on performance metrics
func (bo *BatchOperations[T]) OptimizeBatchSize(ctx context.Context, sampleItems []T) (int, error) {
	if len(sampleItems) == 0 {
		return bo.batchSize, nil
	}

	// Test different batch sizes
	testSizes := []int{100, 500, 1000, 2000, 5000}
	bestSize := bo.batchSize
	bestThroughput := 0.0

	for _, testSize := range testSizes {
		if testSize > len(sampleItems) {
			continue
		}

		// Test with a subset
		testItems := sampleItems[:testSize]

		start := time.Now()
		tempBO := &BatchOperations[T]{
			conn:         bo.conn,
			tableName:    bo.tableName + "_test", // Use test table
			batchSize:    testSize,
			maxRetries:   1,
			fieldMapping: bo.fieldMapping,
		}

		// Simulate the operation (don't actually insert)
		_ = tempBO.simulateInsert(ctx, testItems)
		duration := time.Since(start)

		throughput := float64(testSize) / duration.Seconds()
		if throughput > bestThroughput {
			bestThroughput = throughput
			bestSize = testSize
		}
	}

	bo.batchSize = bestSize
	return bestSize, nil
}

// BatchResult represents the result of a batch operation
type BatchResult struct {
	Operation            string
	TotalItems           int
	ProcessedItems       int
	BatchSize            int
	SuccessfulBatches    int
	Errors               []BatchError
	StartTime            time.Time
	Duration             time.Duration
	ThroughputPerSecond  float64
}

// BatchError represents an error that occurred during batch processing
type BatchError struct {
	BatchIndex int
	ItemCount  int
	Error      error
}

// IsSuccessful returns true if the batch operation was completely successful
func (br *BatchResult) IsSuccessful() bool {
	return len(br.Errors) == 0
}

// GetErrorRate returns the error rate as a percentage
func (br *BatchResult) GetErrorRate() float64 {
	if br.TotalItems == 0 {
		return 0.0
	}
	failedItems := br.TotalItems - br.ProcessedItems
	return (float64(failedItems) / float64(br.TotalItems)) * 100.0
}

// Internal helper methods

func (bo *BatchOperations[T]) analyzeStruct() {
	var zero T
	t := reflect.TypeOf(zero)

	// Handle pointer types
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return
	}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		// Check for db tag
		if dbTag := field.Tag.Get("db"); dbTag != "" && dbTag != "-" {
			bo.fieldMapping[field.Name] = dbTag
		} else {
			// Use snake_case conversion of field name
			bo.fieldMapping[field.Name] = toSnakeCase(field.Name)
		}
	}
}

func (bo *BatchOperations[T]) insertBatch(ctx context.Context, batch []T, batchIndex int) (sql.Result, error) {
	if len(batch) == 0 {
		return nil, fmt.Errorf("empty batch")
	}

	// Generate insert query if not cached
	if bo.insertQuery == "" {
		bo.insertQuery = bo.generateInsertQuery(len(batch))
	}

	// Extract values from batch
	args := make([]interface{}, 0, len(batch)*len(bo.fieldMapping))
	for _, item := range batch {
		itemArgs := bo.extractValues(item)
		args = append(args, itemArgs...)
	}

	// Execute with retries
	var result sql.Result
	var err error

	for attempt := 0; attempt <= bo.maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(bo.retryDelay)
		}

		result, err = bo.conn.Exec(ctx, bo.insertQuery, args...)
		if err == nil {
			break
		}

		if !isRetryableError(err) {
			break
		}
	}

	return result, err
}

func (bo *BatchOperations[T]) updateBatch(ctx context.Context, batch []T, whereClause string, whereArgs ...interface{}) (sql.Result, error) {
	// For simplicity, this implementation updates each item individually
	// A more sophisticated implementation could use CASE statements or temp tables

	var lastResult sql.Result
	for _, item := range batch {
		values := bo.extractValues(item)

		// Build UPDATE query
		setParts := make([]string, 0, len(bo.fieldMapping))
		args := make([]interface{}, 0, len(values)+len(whereArgs))

		i := 0
		for _, columnName := range bo.fieldMapping {
			if i < len(values) {
				setParts = append(setParts, fmt.Sprintf("%s = ?", columnName))
				args = append(args, values[i])
				i++
			}
		}

		query := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
			bo.tableName,
			strings.Join(setParts, ", "),
			whereClause)

		args = append(args, whereArgs...)

		result, err := bo.conn.Exec(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		lastResult = result
	}

	return lastResult, nil
}

func (bo *BatchOperations[T]) generateInsertQuery(batchSize int) string {
	if len(bo.fieldMapping) == 0 {
		return ""
	}

	columns := make([]string, 0, len(bo.fieldMapping))
	for _, columnName := range bo.fieldMapping {
		columns = append(columns, columnName)
	}

	placeholderGroups := make([]string, batchSize)
	placeholders := strings.Repeat("?,", len(columns))
	placeholders = placeholders[:len(placeholders)-1] // Remove trailing comma

	for i := 0; i < batchSize; i++ {
		placeholderGroups[i] = "(" + placeholders + ")"
	}

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		bo.tableName,
		strings.Join(columns, ", "),
		strings.Join(placeholderGroups, ", "))
}

func (bo *BatchOperations[T]) extractValues(item T) []interface{} {
	v := reflect.ValueOf(item)

	// Handle pointer types
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	values := make([]interface{}, 0, len(bo.fieldMapping))

	for fieldName := range bo.fieldMapping {
		fieldValue := v.FieldByName(fieldName)
		if fieldValue.IsValid() {
			values = append(values, fieldValue.Interface())
		}
	}

	return values
}

func (bo *BatchOperations[T]) simulateInsert(ctx context.Context, items []T) error {
	// Simulate the work of preparing an insert without actually executing
	if bo.insertQuery == "" {
		bo.insertQuery = bo.generateInsertQuery(len(items))
	}

	// Simulate value extraction
	for _, item := range items {
		_ = bo.extractValues(item)
	}

	// Add some realistic delay
	time.Sleep(time.Millisecond * time.Duration(len(items)/10))
	return nil
}

// Utility functions

func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	errorStr := strings.ToLower(err.Error())
	retryablePatterns := []string{
		"connection", "timeout", "temporary", "deadlock",
		"lock", "busy", "retry", "network",
	}

	for _, pattern := range retryablePatterns {
		if strings.Contains(errorStr, pattern) {
			return true
		}
	}

	return false
}

func toSnakeCase(str string) string {
	var result strings.Builder

	for i, r := range str {
		if i > 0 && r >= 'A' && r <= 'Z' {
			result.WriteByte('_')
		}
		result.WriteRune(r | 32) // Convert to lowercase
	}

	return result.String()
}

// BatchOperationsManager manages batch operations for multiple entity types
type BatchOperationsManager struct {
	conn      db.Connection
	operators map[string]interface{} // Map of table name to BatchOperations instance
	mu        sync.RWMutex
}

// NewBatchOperationsManager creates a new manager for batch operations
func NewBatchOperationsManager(conn db.Connection) *BatchOperationsManager {
	return &BatchOperationsManager{
		conn:      conn,
		operators: make(map[string]interface{}),
	}
}

// GetBatchOperations returns batch operations for a specific type and table
func GetBatchOperations[T any](bom *BatchOperationsManager, tableName string) *BatchOperations[T] {
	bom.mu.RLock()
	if op, exists := bom.operators[tableName]; exists {
		if typedOp, ok := op.(*BatchOperations[T]); ok {
			bom.mu.RUnlock()
			return typedOp
		}
	}
	bom.mu.RUnlock()

	// Create new batch operations
	bom.mu.Lock()
	defer bom.mu.Unlock()

	// Double-check after acquiring write lock
	if op, exists := bom.operators[tableName]; exists {
		if typedOp, ok := op.(*BatchOperations[T]); ok {
			return typedOp
		}
	}

	// Create new batch operations instance
	batchOps := NewBatchOperations[T](bom.conn, tableName, DefaultBatchConfig())
	bom.operators[tableName] = batchOps
	return batchOps
}

// GetAllStats returns statistics for all batch operations
func (bom *BatchOperationsManager) GetAllStats() map[string]interface{} {
	bom.mu.RLock()
	defer bom.mu.RUnlock()

	stats := make(map[string]interface{})
	for tableName := range bom.operators {
		// This would need to be enhanced to extract actual stats
		// For now, just indicate that operations exist for this table
		stats[tableName] = fmt.Sprintf("Batch operations available for %s", tableName)
	}

	return stats
}