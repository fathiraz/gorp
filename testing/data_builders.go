// Test data builders with faker integration for GORP testing
package testing

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"time"

	"github.com/fathiraz/gorp/db"
	"github.com/fathiraz/gorp/mapping"
)

// FakerProvider provides fake data generation capabilities
type FakerProvider interface {
	Name() string
	Email() string
	Phone() string
	Address() string
	City() string
	Country() string
	Company() string
	Text(maxLength int) string
	Number(min, max int) int
	Float(min, max float64) float64
	Date(from, to time.Time) time.Time
	Bool() bool
	UUID() string
	Lorem(words int) string
}

// SimpleFaker provides basic fake data generation
type SimpleFaker struct {
	rand *rand.Rand
}

// NewSimpleFaker creates a new simple faker
func NewSimpleFaker() *SimpleFaker {
	return &SimpleFaker{
		rand: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (f *SimpleFaker) Name() string {
	firstNames := []string{"John", "Jane", "Bob", "Alice", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry"}
	lastNames := []string{"Smith", "Johnson", "Brown", "Davis", "Miller", "Wilson", "Moore", "Taylor", "Anderson", "Thomas"}
	return firstNames[f.rand.Intn(len(firstNames))] + " " + lastNames[f.rand.Intn(len(lastNames))]
}

func (f *SimpleFaker) Email() string {
	domains := []string{"example.com", "test.org", "sample.net", "demo.io"}
	users := []string{"user", "test", "demo", "sample", "admin"}
	return fmt.Sprintf("%s%d@%s", users[f.rand.Intn(len(users))], f.rand.Intn(1000), domains[f.rand.Intn(len(domains))])
}

func (f *SimpleFaker) Phone() string {
	return fmt.Sprintf("+1-%03d-%03d-%04d", f.rand.Intn(900)+100, f.rand.Intn(900)+100, f.rand.Intn(9000)+1000)
}

func (f *SimpleFaker) Address() string {
	streetNumbers := f.rand.Intn(9999) + 1
	streets := []string{"Main St", "Oak Ave", "Pine Rd", "Elm Dr", "Cedar Ln", "Maple Way"}
	return fmt.Sprintf("%d %s", streetNumbers, streets[f.rand.Intn(len(streets))])
}

func (f *SimpleFaker) City() string {
	cities := []string{"New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"}
	return cities[f.rand.Intn(len(cities))]
}

func (f *SimpleFaker) Country() string {
	countries := []string{"United States", "Canada", "United Kingdom", "Germany", "France", "Japan", "Australia", "Brazil", "India", "China"}
	return countries[f.rand.Intn(len(countries))]
}

func (f *SimpleFaker) Company() string {
	prefixes := []string{"Global", "Advanced", "Premier", "Dynamic", "Innovative", "Strategic"}
	suffixes := []string{"Solutions", "Technologies", "Systems", "Services", "Industries", "Corporation"}
	return prefixes[f.rand.Intn(len(prefixes))] + " " + suffixes[f.rand.Intn(len(suffixes))]
}

func (f *SimpleFaker) Text(maxLength int) string {
	words := []string{"lorem", "ipsum", "dolor", "sit", "amet", "consectetur", "adipiscing", "elit", "sed", "do", "eiusmod", "tempor", "incididunt"}
	result := ""
	for len(result) < maxLength && len(result) < 1000 {
		if len(result) > 0 {
			result += " "
		}
		word := words[f.rand.Intn(len(words))]
		if len(result)+len(word) <= maxLength {
			result += word
		} else {
			break
		}
	}
	return result
}

func (f *SimpleFaker) Number(min, max int) int {
	if max <= min {
		return min
	}
	return min + f.rand.Intn(max-min)
}

func (f *SimpleFaker) Float(min, max float64) float64 {
	return min + f.rand.Float64()*(max-min)
}

func (f *SimpleFaker) Date(from, to time.Time) time.Time {
	delta := to.Sub(from)
	randomDuration := time.Duration(f.rand.Int63n(int64(delta)))
	return from.Add(randomDuration)
}

func (f *SimpleFaker) Bool() bool {
	return f.rand.Intn(2) == 1
}

func (f *SimpleFaker) UUID() string {
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		f.rand.Uint32(),
		f.rand.Uint32()&0xffff,
		f.rand.Uint32()&0xffff,
		f.rand.Uint32()&0xffff,
		f.rand.Uint64()&0xffffffffffff)
}

func (f *SimpleFaker) Lorem(words int) string {
	loremWords := []string{"lorem", "ipsum", "dolor", "sit", "amet", "consectetur", "adipiscing", "elit", "sed", "do", "eiusmod", "tempor", "incididunt", "ut", "labore", "et", "dolore", "magna", "aliqua"}
	result := make([]string, words)
	for i := 0; i < words; i++ {
		result[i] = loremWords[f.rand.Intn(len(loremWords))]
	}
	return strings.Join(result, " ")
}

// DataBuilder provides a fluent interface for building test data
type DataBuilder[T mapping.Mappable] struct {
	faker         FakerProvider
	mapper        mapping.TableMapper[T]
	overrides     map[string]interface{}
	constraints   map[string]func(FakerProvider) interface{}
	relationships map[string]DataBuilder[any]
	count         int
}

// NewDataBuilder creates a new data builder
func NewDataBuilder[T mapping.Mappable](mapper mapping.TableMapper[T]) *DataBuilder[T] {
	return &DataBuilder[T]{
		faker:         NewSimpleFaker(),
		mapper:        mapper,
		overrides:     make(map[string]interface{}),
		constraints:   make(map[string]func(FakerProvider) interface{}),
		relationships: make(map[string]DataBuilder[any]),
		count:         1,
	}
}

// WithFaker sets a custom faker provider
func (db *DataBuilder[T]) WithFaker(faker FakerProvider) *DataBuilder[T] {
	db.faker = faker
	return db
}

// With sets a specific field value
func (db *DataBuilder[T]) With(fieldName string, value interface{}) *DataBuilder[T] {
	db.overrides[fieldName] = value
	return db
}

// WithConstraint sets a constraint for a field using faker
func (db *DataBuilder[T]) WithConstraint(fieldName string, constraint func(FakerProvider) interface{}) *DataBuilder[T] {
	db.constraints[fieldName] = constraint
	return db
}

// WithEmail sets an email constraint
func (db *DataBuilder[T]) WithEmail(fieldName string) *DataBuilder[T] {
	return db.WithConstraint(fieldName, func(f FakerProvider) interface{} {
		return f.Email()
	})
}

// WithName sets a name constraint
func (db *DataBuilder[T]) WithName(fieldName string) *DataBuilder[T] {
	return db.WithConstraint(fieldName, func(f FakerProvider) interface{} {
		return f.Name()
	})
}

// WithPhone sets a phone constraint
func (db *DataBuilder[T]) WithPhone(fieldName string) *DataBuilder[T] {
	return db.WithConstraint(fieldName, func(f FakerProvider) interface{} {
		return f.Phone()
	})
}

// WithText sets a text constraint with max length
func (db *DataBuilder[T]) WithText(fieldName string, maxLength int) *DataBuilder[T] {
	return db.WithConstraint(fieldName, func(f FakerProvider) interface{} {
		return f.Text(maxLength)
	})
}

// WithNumberRange sets a number constraint with range
func (db *DataBuilder[T]) WithNumberRange(fieldName string, min, max int) *DataBuilder[T] {
	return db.WithConstraint(fieldName, func(f FakerProvider) interface{} {
		return f.Number(min, max)
	})
}

// WithDateRange sets a date constraint with range
func (db *DataBuilder[T]) WithDateRange(fieldName string, from, to time.Time) *DataBuilder[T] {
	return db.WithConstraint(fieldName, func(f FakerProvider) interface{} {
		return f.Date(from, to)
	})
}

// Count sets the number of entities to build
func (db *DataBuilder[T]) Count(count int) *DataBuilder[T] {
	db.count = count
	return db
}

// Build creates the test data
func (db *DataBuilder[T]) Build() []T {
	results := make([]T, db.count)
	for i := 0; i < db.count; i++ {
		results[i] = db.buildSingle()
	}
	return results
}

// BuildOne creates a single entity
func (db *DataBuilder[T]) BuildOne() T {
	return db.buildSingle()
}

func (db *DataBuilder[T]) buildSingle() T {
	var entity T
	entityValue := reflect.ValueOf(&entity).Elem()
	entityType := entityValue.Type()

	for i := 0; i < entityType.NumField(); i++ {
		field := entityType.Field(i)
		fieldValue := entityValue.Field(i)

		if !fieldValue.CanSet() {
			continue
		}

		fieldName := field.Name

		// Check for explicit override
		if override, exists := db.overrides[fieldName]; exists {
			if override != nil {
				fieldValue.Set(reflect.ValueOf(override))
			}
			continue
		}

		// Check for constraint
		if constraint, exists := db.constraints[fieldName]; exists {
			value := constraint(db.faker)
			if value != nil {
				fieldValue.Set(reflect.ValueOf(value))
			}
			continue
		}

		// Generate based on field type and tags
		db.generateFieldValue(field, fieldValue)
	}

	return entity
}

func (db *DataBuilder[T]) generateFieldValue(field reflect.StructField, fieldValue reflect.Value) {
	// Check for database tags
	dbTag := field.Tag.Get("db")
	jsonTag := field.Tag.Get("json")
	gopTag := field.Tag.Get("gorp")

	// Skip certain fields
	if dbTag == "-" || jsonTag == "-" {
		return
	}

	// Auto-detect field purpose from name and tags
	fieldNameLower := strings.ToLower(field.Name)

	switch fieldValue.Kind() {
	case reflect.String:
		value := db.generateStringValue(fieldNameLower, field.Tag)
		fieldValue.SetString(value)

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		value := db.generateIntValue(fieldNameLower, field.Tag)
		fieldValue.SetInt(int64(value))

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		value := db.generateIntValue(fieldNameLower, field.Tag)
		fieldValue.SetUint(uint64(value))

	case reflect.Float32, reflect.Float64:
		value := db.faker.Float(0, 1000)
		fieldValue.SetFloat(value)

	case reflect.Bool:
		fieldValue.SetBool(db.faker.Bool())

	case reflect.Struct:
		if fieldValue.Type() == reflect.TypeOf(time.Time{}) {
			value := db.generateTimeValue(fieldNameLower, field.Tag)
			fieldValue.Set(reflect.ValueOf(value))
		}

	case reflect.Ptr:
		// Handle pointer types
		if fieldValue.Type().Elem().Kind() == reflect.String {
			value := db.generateStringValue(fieldNameLower, field.Tag)
			fieldValue.Set(reflect.ValueOf(&value))
		} else if fieldValue.Type().Elem() == reflect.TypeOf(time.Time{}) {
			value := db.generateTimeValue(fieldNameLower, field.Tag)
			fieldValue.Set(reflect.ValueOf(&value))
		}
	}
}

func (db *DataBuilder[T]) generateStringValue(fieldName string, tag reflect.StructTag) string {
	// Smart field detection based on name
	if strings.Contains(fieldName, "email") {
		return db.faker.Email()
	}
	if strings.Contains(fieldName, "name") || strings.Contains(fieldName, "username") {
		return db.faker.Name()
	}
	if strings.Contains(fieldName, "phone") || strings.Contains(fieldName, "mobile") {
		return db.faker.Phone()
	}
	if strings.Contains(fieldName, "address") {
		return db.faker.Address()
	}
	if strings.Contains(fieldName, "city") {
		return db.faker.City()
	}
	if strings.Contains(fieldName, "country") {
		return db.faker.Country()
	}
	if strings.Contains(fieldName, "company") || strings.Contains(fieldName, "organization") {
		return db.faker.Company()
	}
	if strings.Contains(fieldName, "uuid") || strings.Contains(fieldName, "id") {
		return db.faker.UUID()
	}
	if strings.Contains(fieldName, "description") || strings.Contains(fieldName, "content") || strings.Contains(fieldName, "text") {
		return db.faker.Lorem(10)
	}

	// Check for length constraints in tag
	if maxLen := tag.Get("maxlength"); maxLen != "" {
		// Parse maxLen and use it
		return db.faker.Text(100) // Default to 100 if parsing fails
	}

	// Default text
	return db.faker.Text(50)
}

func (db *DataBuilder[T]) generateIntValue(fieldName string, tag reflect.StructTag) int {
	if strings.Contains(fieldName, "id") && !strings.Contains(fieldName, "uuid") {
		return db.faker.Number(1, 100000)
	}
	if strings.Contains(fieldName, "age") {
		return db.faker.Number(18, 80)
	}
	if strings.Contains(fieldName, "count") || strings.Contains(fieldName, "quantity") {
		return db.faker.Number(1, 1000)
	}
	if strings.Contains(fieldName, "price") || strings.Contains(fieldName, "amount") {
		return db.faker.Number(100, 50000)
	}

	// Default range
	return db.faker.Number(1, 1000)
}

func (db *DataBuilder[T]) generateTimeValue(fieldName string, tag reflect.StructTag) time.Time {
	now := time.Now()
	if strings.Contains(fieldName, "created") || strings.Contains(fieldName, "birth") {
		return db.faker.Date(now.AddDate(-10, 0, 0), now)
	}
	if strings.Contains(fieldName, "updated") || strings.Contains(fieldName, "modified") {
		return db.faker.Date(now.AddDate(0, -1, 0), now)
	}
	if strings.Contains(fieldName, "expire") || strings.Contains(fieldName, "deadline") {
		return db.faker.Date(now, now.AddDate(1, 0, 0))
	}

	// Default to recent past
	return db.faker.Date(now.AddDate(0, -6, 0), now)
}

// DatabaseDataBuilder provides database-aware test data building
type DatabaseDataBuilder[T mapping.Mappable] struct {
	*DataBuilder[T]
	connection db.Connection
	persistent bool
}

// NewDatabaseDataBuilder creates a new database-aware data builder
func NewDatabaseDataBuilder[T mapping.Mappable](mapper mapping.TableMapper[T], connection db.Connection) *DatabaseDataBuilder[T] {
	return &DatabaseDataBuilder[T]{
		DataBuilder: NewDataBuilder(mapper),
		connection:  connection,
		persistent:  false,
	}
}

// Persistent enables persistent storage of built entities
func (ddb *DatabaseDataBuilder[T]) Persistent() *DatabaseDataBuilder[T] {
	ddb.persistent = true
	return ddb
}

// BuildAndSave creates entities and saves them to the database
func (ddb *DatabaseDataBuilder[T]) BuildAndSave(ctx context.Context) ([]T, error) {
	entities := ddb.Build()

	if !ddb.persistent {
		return entities, nil
	}

	tx, err := ddb.connection.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	for _, entity := range entities {
		err := ddb.saveEntity(ctx, tx, entity)
		if err != nil {
			return nil, fmt.Errorf("failed to save entity: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return entities, nil
}

func (ddb *DatabaseDataBuilder[T]) saveEntity(ctx context.Context, tx db.Transaction, entity T) error {
	row, err := ddb.mapper.ToRow(entity)
	if err != nil {
		return err
	}

	tableName := ddb.mapper.TableName()
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

	sql := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	_, err = tx.ExecContext(ctx, sql, values...)
	return err
}

// TestDataFactory provides a factory for creating common test data patterns
type TestDataFactory struct {
	faker FakerProvider
}

// NewTestDataFactory creates a new test data factory
func NewTestDataFactory() *TestDataFactory {
	return &TestDataFactory{
		faker: NewSimpleFaker(),
	}
}

// WithFaker sets a custom faker provider
func (tdf *TestDataFactory) WithFaker(faker FakerProvider) *TestDataFactory {
	tdf.faker = faker
	return tdf
}

// CreateUser creates a user entity with common fields
func (tdf *TestDataFactory) CreateUser() map[string]interface{} {
	return map[string]interface{}{
		"id":         tdf.faker.UUID(),
		"username":   strings.ToLower(strings.ReplaceAll(tdf.faker.Name(), " ", "_")),
		"email":      tdf.faker.Email(),
		"first_name": strings.Split(tdf.faker.Name(), " ")[0],
		"last_name":  strings.Split(tdf.faker.Name(), " ")[1],
		"phone":      tdf.faker.Phone(),
		"created_at": tdf.faker.Date(time.Now().AddDate(-1, 0, 0), time.Now()),
		"updated_at": time.Now(),
		"active":     tdf.faker.Bool(),
	}
}

// CreateProduct creates a product entity with common fields
func (tdf *TestDataFactory) CreateProduct() map[string]interface{} {
	return map[string]interface{}{
		"id":          tdf.faker.UUID(),
		"name":        tdf.faker.Company() + " Product",
		"description": tdf.faker.Lorem(20),
		"price":       tdf.faker.Float(10.0, 1000.0),
		"category":    []string{"Electronics", "Books", "Clothing", "Home", "Sports"}[tdf.faker.Number(0, 5)],
		"sku":         fmt.Sprintf("SKU-%d", tdf.faker.Number(10000, 99999)),
		"in_stock":    tdf.faker.Bool(),
		"quantity":    tdf.faker.Number(0, 1000),
		"created_at":  tdf.faker.Date(time.Now().AddDate(-2, 0, 0), time.Now()),
		"updated_at":  time.Now(),
	}
}

// CreateOrder creates an order entity with common fields
func (tdf *TestDataFactory) CreateOrder() map[string]interface{} {
	return map[string]interface{}{
		"id":           tdf.faker.UUID(),
		"user_id":      tdf.faker.UUID(),
		"order_number": fmt.Sprintf("ORD-%d", tdf.faker.Number(100000, 999999)),
		"total":        tdf.faker.Float(50.0, 2000.0),
		"status":       []string{"pending", "processing", "shipped", "delivered", "cancelled"}[tdf.faker.Number(0, 5)],
		"shipping_address": tdf.faker.Address(),
		"billing_address":  tdf.faker.Address(),
		"created_at":       tdf.faker.Date(time.Now().AddDate(0, -3, 0), time.Now()),
		"updated_at":       time.Now(),
	}
}

// BatchDataGenerator provides utilities for generating large amounts of test data
type BatchDataGenerator struct {
	faker       FakerProvider
	batchSize   int
	concurrency int
}

// NewBatchDataGenerator creates a new batch data generator
func NewBatchDataGenerator() *BatchDataGenerator {
	return &BatchDataGenerator{
		faker:       NewSimpleFaker(),
		batchSize:   1000,
		concurrency: 4,
	}
}

// SetBatchSize sets the batch size for data generation
func (bdg *BatchDataGenerator) SetBatchSize(size int) *BatchDataGenerator {
	bdg.batchSize = size
	return bdg
}

// SetConcurrency sets the concurrency level for data generation
func (bdg *BatchDataGenerator) SetConcurrency(concurrency int) *BatchDataGenerator {
	bdg.concurrency = concurrency
	return bdg
}

// GenerateBatch generates a batch of test data using the provided generator function
func (bdg *BatchDataGenerator) GenerateBatch[T any](totalCount int, generator func() T) []T {
	if totalCount <= bdg.batchSize {
		// Generate in single batch
		results := make([]T, totalCount)
		for i := 0; i < totalCount; i++ {
			results[i] = generator()
		}
		return results
	}

	// Generate in parallel batches
	results := make([]T, totalCount)
	batchCount := (totalCount + bdg.batchSize - 1) / bdg.batchSize

	// Use channels for coordination
	jobs := make(chan int, batchCount)
	done := make(chan struct{}, bdg.concurrency)

	// Start workers
	for w := 0; w < bdg.concurrency; w++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for batchIndex := range jobs {
				start := batchIndex * bdg.batchSize
				end := start + bdg.batchSize
				if end > totalCount {
					end = totalCount
				}

				for i := start; i < end; i++ {
					results[i] = generator()
				}
			}
		}()
	}

	// Send jobs
	for i := 0; i < batchCount; i++ {
		jobs <- i
	}
	close(jobs)

	// Wait for completion
	for w := 0; w < bdg.concurrency; w++ {
		<-done
	}

	return results
}