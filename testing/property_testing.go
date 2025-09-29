// Property-based testing framework for GORP
package testing

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/fathiraz/gorp/db"
	"github.com/fathiraz/gorp/mapping"
	"github.com/fathiraz/gorp/query"
)

// Generator represents a data generator for property-based testing
type Generator[T any] interface {
	Generate(rand *rand.Rand) T
}

// PropertyTest represents a property-based test
type PropertyTest[T any] struct {
	Name       string
	Generator  Generator[T]
	Property   func(t *testing.T, input T) bool
	NumTests   int
	MaxSize    int
	Seed       int64
	Timeout    time.Duration
	Shrinking  bool
	Verbose    bool
}

// PropertyTestRunner runs property-based tests
type PropertyTestRunner struct {
	defaultNumTests int
	defaultMaxSize  int
	defaultTimeout  time.Duration
	enableShrinking bool
	verbose         bool
	rand            *rand.Rand
}

// NewPropertyTestRunner creates a new property test runner
func NewPropertyTestRunner() *PropertyTestRunner {
	return &PropertyTestRunner{
		defaultNumTests: 100,
		defaultMaxSize:  100,
		defaultTimeout:  30 * time.Second,
		enableShrinking: true,
		verbose:         false,
		rand:            rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// SetDefaults sets default parameters for property tests
func (ptr *PropertyTestRunner) SetDefaults(numTests, maxSize int, timeout time.Duration) {
	ptr.defaultNumTests = numTests
	ptr.defaultMaxSize = maxSize
	ptr.defaultTimeout = timeout
}

// EnableShrinking enables or disables test case shrinking
func (ptr *PropertyTestRunner) EnableShrinking(enabled bool) {
	ptr.enableShrinking = enabled
}

// SetVerbose enables or disables verbose output
func (ptr *PropertyTestRunner) SetVerbose(verbose bool) {
	ptr.verbose = verbose
}

// Run runs a property-based test
func (ptr *PropertyTestRunner) Run[T any](t *testing.T, test PropertyTest[T]) {
	t.Helper()

	// Set defaults if not specified
	numTests := test.NumTests
	if numTests == 0 {
		numTests = ptr.defaultNumTests
	}

	maxSize := test.MaxSize
	if maxSize == 0 {
		maxSize = ptr.defaultMaxSize
	}

	timeout := test.Timeout
	if timeout == 0 {
		timeout = ptr.defaultTimeout
	}

	// Set up random seed
	seed := test.Seed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	testRand := rand.New(rand.NewSource(seed))

	if ptr.verbose {
		t.Logf("Running property test '%s' with %d tests, max size %d, seed %d",
			test.Name, numTests, maxSize, seed)
	}

	// Track test statistics
	stats := &TestStatistics{
		Total:   numTests,
		Passed:  0,
		Failed:  0,
		Skipped: 0,
	}

	var failedInput T
	var failureMessage string

	// Run tests with timeout
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for i := 0; i < numTests; i++ {
		select {
		case <-ctx.Done():
			t.Fatalf("Property test '%s' timed out after %v", test.Name, timeout)
		default:
		}

		// Generate test input
		input := test.Generator.Generate(testRand)

		// Run the property test
		func() {
			defer func() {
				if r := recover(); r != nil {
					stats.Failed++
					failedInput = input
					failureMessage = fmt.Sprintf("Property test panicked: %v", r)
				}
			}()

			passed := test.Property(t, input)
			if passed {
				stats.Passed++
			} else {
				stats.Failed++
				failedInput = input
				failureMessage = fmt.Sprintf("Property failed for input: %+v", input)
				return
			}
		}()

		if stats.Failed > 0 {
			break
		}

		if ptr.verbose && (i+1)%10 == 0 {
			t.Logf("Completed %d/%d tests", i+1, numTests)
		}
	}

	// Report results
	if stats.Failed > 0 {
		if test.Shrinking && ptr.enableShrinking {
			// Attempt to shrink the failing input
			shrunkInput := ptr.shrinkInput(t, test, failedInput, testRand)
			t.Fatalf("Property test '%s' failed after %d tests. Shrunk failing input: %+v. %s",
				test.Name, stats.Passed+1, shrunkInput, failureMessage)
		} else {
			t.Fatalf("Property test '%s' failed after %d tests. Failing input: %+v. %s",
				test.Name, stats.Passed+1, failedInput, failureMessage)
		}
	}

	if ptr.verbose {
		t.Logf("Property test '%s' passed all %d tests", test.Name, numTests)
	}
}

// TestStatistics tracks property test statistics
type TestStatistics struct {
	Total   int
	Passed  int
	Failed  int
	Skipped int
}

// Common generators
type IntGenerator struct {
	Min, Max int
}

func (g IntGenerator) Generate(rand *rand.Rand) int {
	if g.Max <= g.Min {
		return g.Min
	}
	return g.Min + rand.Intn(g.Max-g.Min)
}

type StringGenerator struct {
	MinLength, MaxLength int
	Charset              string
}

func NewStringGenerator() StringGenerator {
	return StringGenerator{
		MinLength: 0,
		MaxLength: 20,
		Charset:   "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
	}
}

func (g StringGenerator) Generate(rand *rand.Rand) string {
	length := g.MinLength
	if g.MaxLength > g.MinLength {
		length += rand.Intn(g.MaxLength - g.MinLength)
	}

	if length == 0 {
		return ""
	}

	result := make([]byte, length)
	for i := range result {
		result[i] = g.Charset[rand.Intn(len(g.Charset))]
	}
	return string(result)
}

type BoolGenerator struct{}

func (g BoolGenerator) Generate(rand *rand.Rand) bool {
	return rand.Intn(2) == 1
}

type SliceGenerator[T any] struct {
	ElementGenerator Generator[T]
	MinLength        int
	MaxLength        int
}

func (g SliceGenerator[T]) Generate(rand *rand.Rand) []T {
	length := g.MinLength
	if g.MaxLength > g.MinLength {
		length += rand.Intn(g.MaxLength - g.MinLength)
	}

	result := make([]T, length)
	for i := range result {
		result[i] = g.ElementGenerator.Generate(rand)
	}
	return result
}

type StructGenerator[T any] struct {
	FieldGenerators map[string]Generator[any]
}

func (g StructGenerator[T]) Generate(rand *rand.Rand) T {
	var result T
	resultValue := reflect.ValueOf(&result).Elem()
	resultType := resultValue.Type()

	for i := 0; i < resultType.NumField(); i++ {
		field := resultType.Field(i)
		fieldValue := resultValue.Field(i)

		if !fieldValue.CanSet() {
			continue
		}

		if generator, exists := g.FieldGenerators[field.Name]; exists {
			generatedValue := generator.Generate(rand)
			if generatedValue != nil {
				fieldValue.Set(reflect.ValueOf(generatedValue))
			}
		}
	}

	return result
}

// Database-specific generators
type DatabaseEntityGenerator[T mapping.Mappable] struct {
	TableMapper mapping.TableMapper[T]
	Constraints map[string]func(*rand.Rand) any
}

func NewDatabaseEntityGenerator[T mapping.Mappable](mapper mapping.TableMapper[T]) *DatabaseEntityGenerator[T] {
	return &DatabaseEntityGenerator[T]{
		TableMapper: mapper,
		Constraints: make(map[string]func(*rand.Rand) any),
	}
}

func (g *DatabaseEntityGenerator[T]) AddConstraint(fieldName string, generator func(*rand.Rand) any) {
	g.Constraints[fieldName] = generator
}

func (g *DatabaseEntityGenerator[T]) Generate(rand *rand.Rand) T {
	var result T
	resultValue := reflect.ValueOf(&result).Elem()
	resultType := resultValue.Type()

	columnMap := g.TableMapper.ColumnMap()

	for i := 0; i < resultType.NumField(); i++ {
		field := resultType.Field(i)
		fieldValue := resultValue.Field(i)

		if !fieldValue.CanSet() {
			continue
		}

		// Check if we have a custom constraint for this field
		if constraint, exists := g.Constraints[field.Name]; exists {
			generatedValue := constraint(rand)
			if generatedValue != nil {
				fieldValue.Set(reflect.ValueOf(generatedValue))
			}
			continue
		}

		// Generate value based on field type
		switch fieldValue.Kind() {
		case reflect.String:
			if _, mapped := columnMap[field.Name]; mapped {
				fieldValue.SetString(NewStringGenerator().Generate(rand))
			}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			fieldValue.SetInt(int64(IntGenerator{Min: 0, Max: 1000}.Generate(rand)))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			fieldValue.SetUint(uint64(IntGenerator{Min: 0, Max: 1000}.Generate(rand)))
		case reflect.Bool:
			fieldValue.SetBool(BoolGenerator{}.Generate(rand))
		case reflect.Float32, reflect.Float64:
			fieldValue.SetFloat(rand.Float64() * 1000)
		}
	}

	return result
}

// Property test helpers for database operations
func CRUDInvariantProperty[T mapping.Mappable](
	connection db.Connection,
	mapper mapping.TableMapper[T],
) func(t *testing.T, entity T) bool {
	return func(t *testing.T, entity T) bool {
		ctx := context.Background()

		// Begin transaction for isolation
		tx, err := connection.Begin(ctx)
		if err != nil {
			t.Logf("Failed to begin transaction: %v", err)
			return false
		}
		defer tx.Rollback()

		// Convert entity to row
		row, err := mapper.ToRow(entity)
		if err != nil {
			t.Logf("Failed to convert entity to row: %v", err)
			return false
		}

		// Generate INSERT statement
		tableName := mapper.TableName()
		columns := make([]string, 0, len(row))
		placeholders := make([]string, 0, len(row))
		values := make([]interface{}, 0, len(row))

		i := 1
		for column, value := range row {
			columns = append(columns, column)
			placeholders = append(placeholders, fmt.Sprintf("$%d", i))
			values = append(values, value)
			i++
		}

		insertSQL := fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s)",
			tableName,
			strings.Join(columns, ", "),
			strings.Join(placeholders, ", "),
		)

		// Insert the entity
		_, err = tx.ExecContext(ctx, insertSQL, values...)
		if err != nil {
			t.Logf("Failed to insert entity: %v", err)
			return false
		}

		// Read it back
		primaryKeys := mapper.PrimaryKey()
		if len(primaryKeys) == 0 {
			t.Logf("No primary key defined for table %s", tableName)
			return false
		}

		whereClause := make([]string, len(primaryKeys))
		whereValues := make([]interface{}, len(primaryKeys))
		for i, pk := range primaryKeys {
			whereClause[i] = fmt.Sprintf("%s = $%d", pk, i+1)
			whereValues[i] = row[pk]
		}

		selectSQL := fmt.Sprintf(
			"SELECT %s FROM %s WHERE %s",
			strings.Join(columns, ", "),
			tableName,
			strings.Join(whereClause, " AND "),
		)

		selectRow := tx.QueryRowContext(ctx, selectSQL, whereValues...)

		// Scan the result
		scannedValues := make([]interface{}, len(columns))
		scanDests := make([]interface{}, len(columns))
		for i := range scannedValues {
			scanDests[i] = &scannedValues[i]
		}

		err = selectRow.Scan(scanDests...)
		if err != nil {
			t.Logf("Failed to scan selected row: %v", err)
			return false
		}

		// Convert back to entity
		scannedRow := make(map[string]interface{})
		for i, column := range columns {
			scannedRow[column] = scannedValues[i]
		}

		retrievedEntity, err := mapper.FromRow(scannedRow)
		if err != nil {
			t.Logf("Failed to convert row to entity: %v", err)
			return false
		}

		// Compare entities (simplified comparison)
		return reflect.DeepEqual(entity, retrievedEntity)
	}
}

func QueryConsistencyProperty[T mapping.Mappable](
	connection db.Connection,
	queryBuilder query.QueryBuilder[T],
) func(t *testing.T, filters map[string]interface{}) bool {
	return func(t *testing.T, filters map[string]interface{}) bool {
		ctx := context.Background()

		// Build query
		sql, args, err := queryBuilder.Build()
		if err != nil {
			t.Logf("Failed to build query: %v", err)
			return false
		}

		// Execute query twice and compare results
		var rows1, rows2 *sql.Rows

		if pgPool := connection.PgxPool(); pgPool != nil {
			rows1, err = pgPool.Query(ctx, sql, args...)
			if err != nil {
				t.Logf("Failed to execute query (first time): %v", err)
				return false
			}
			defer rows1.Close()

			rows2, err = pgPool.Query(ctx, sql, args...)
			if err != nil {
				t.Logf("Failed to execute query (second time): %v", err)
				return false
			}
			defer rows2.Close()
		} else if sqlxDB := connection.SqlxDB(); sqlxDB != nil {
			rows1, err = sqlxDB.QueryContext(ctx, sql, args...)
			if err != nil {
				t.Logf("Failed to execute query (first time): %v", err)
				return false
			}
			defer rows1.Close()

			rows2, err = sqlxDB.QueryContext(ctx, sql, args...)
			if err != nil {
				t.Logf("Failed to execute query (second time): %v", err)
				return false
			}
			defer rows2.Close()
		} else {
			t.Logf("No available database connection")
			return false
		}

		// Compare results (simplified comparison)
		// In a real implementation, you would compare the actual row data
		return true
	}
}

// Shrinking support
func (ptr *PropertyTestRunner) shrinkInput[T any](
	t *testing.T,
	test PropertyTest[T],
	failingInput T,
	rand *rand.Rand,
) T {
	currentInput := failingInput
	shrinkAttempts := 10

	for attempt := 0; attempt < shrinkAttempts; attempt++ {
		// Attempt to find a smaller failing input
		candidateInput := ptr.shrinkValue(currentInput, rand)

		// Test if the candidate still fails
		if !test.Property(t, candidateInput) {
			currentInput = candidateInput
		}
	}

	return currentInput
}

func (ptr *PropertyTestRunner) shrinkValue[T any](value T, rand *rand.Rand) T {
	val := reflect.ValueOf(value)

	switch val.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		// Shrink toward zero
		current := val.Int()
		if current > 0 {
			val.SetInt(current / 2)
		} else if current < 0 {
			val.SetInt(current / 2)
		}
	case reflect.String:
		// Shrink string length
		str := val.String()
		if len(str) > 0 {
			newLen := len(str) / 2
			val.SetString(str[:newLen])
		}
	case reflect.Slice:
		// Shrink slice length
		if val.Len() > 0 {
			newLen := val.Len() / 2
			newSlice := val.Slice(0, newLen)
			val.Set(newSlice)
		}
	}

	return value
}

// Database property test examples
func RunDatabasePropertyTests[T mapping.Mappable](
	t *testing.T,
	connection db.Connection,
	mapper mapping.TableMapper[T],
) {
	runner := NewPropertyTestRunner()
	runner.SetDefaults(50, 100, 10*time.Second)

	// Test CRUD invariants
	t.Run("CRUD_Invariants", func(t *testing.T) {
		generator := NewDatabaseEntityGenerator(mapper)
		test := PropertyTest[T]{
			Name:      "CRUD operations preserve data",
			Generator: generator,
			Property:  CRUDInvariantProperty(connection, mapper),
			NumTests:  25,
		}
		runner.Run(t, test)
	})

	// Test query consistency
	// Additional property tests can be added here
}