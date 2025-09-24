package db

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPostgreSQLTypes(t *testing.T) {
	t.Run("JSONB Type", func(t *testing.T) {
		// Test JSONB creation
		data := map[string]interface{}{
			"name": "test",
			"age":  30,
		}
		jsonb := NewJSONB(data)
		assert.True(t, jsonb.Valid)
		assert.Equal(t, data, jsonb.Data)

		// Test Value method
		value, err := jsonb.Value()
		require.NoError(t, err)
		assert.NotNil(t, value)

		// Test Scan method
		var scanned JSONB[map[string]interface{}]
		err = scanned.Scan(`{"name": "test", "age": 30}`)
		require.NoError(t, err)
		assert.True(t, scanned.Valid)
		assert.Equal(t, "test", scanned.Data["name"])
		assert.Equal(t, float64(30), scanned.Data["age"]) // JSON unmarshals numbers as float64

		// Test null value
		err = scanned.Scan(nil)
		require.NoError(t, err)
		assert.False(t, scanned.Valid)
	})

	t.Run("PostgreSQL Array Type", func(t *testing.T) {
		// Test array creation
		elements := []string{"a", "b", "c"}
		array := NewPostgreSQLArray(elements)
		assert.True(t, array.Valid)
		assert.Equal(t, elements, array.Elements)

		// Test Value method
		value, err := array.Value()
		require.NoError(t, err)
		assert.Equal(t, `{"a","b","c"}`, value)

		// Test empty array
		emptyArray := NewPostgreSQLArray([]string{})
		value, err = emptyArray.Value()
		require.NoError(t, err)
		assert.Equal(t, "{}", value)

		// Test Scan method
		var scanned PostgreSQLArray[string]
		err = scanned.Scan("{test1,test2,test3}")
		require.NoError(t, err)
		assert.True(t, scanned.Valid)
		assert.Len(t, scanned.Elements, 3)
		assert.Equal(t, "test1", scanned.Elements[0])
		assert.Equal(t, "test2", scanned.Elements[1])
		assert.Equal(t, "test3", scanned.Elements[2])

		// Test empty array scan
		err = scanned.Scan("{}")
		require.NoError(t, err)
		assert.True(t, scanned.Valid)
		assert.Len(t, scanned.Elements, 0)

		// Test null scan
		err = scanned.Scan(nil)
		require.NoError(t, err)
		assert.False(t, scanned.Valid)
	})

	t.Run("PostgreSQL UUID Type", func(t *testing.T) {
		// Create test UUID
		uuid := [16]byte{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0}
		pgUUID := NewPostgreSQLUUID(uuid)
		assert.True(t, pgUUID.Valid)
		assert.Equal(t, uuid, pgUUID.UUID)

		// Test string representation
		expectedStr := "12345678-9abc-def0-1234-56789abcdef0"
		assert.Equal(t, expectedStr, pgUUID.String())

		// Test Value method
		value, err := pgUUID.Value()
		require.NoError(t, err)
		assert.Equal(t, expectedStr, value)

		// Test Scan method
		var scanned PostgreSQLUUID
		err = scanned.Scan("12345678-9abc-def0-1234-56789abcdef0")
		require.NoError(t, err)
		assert.True(t, scanned.Valid)
		assert.Equal(t, uuid, scanned.UUID)

		// Test null scan
		err = scanned.Scan(nil)
		require.NoError(t, err)
		assert.False(t, scanned.Valid)
	})

	t.Run("PostgreSQL HStore Type", func(t *testing.T) {
		// Create test HStore
		data := map[string]*string{
			"key1": stringPtr("value1"),
			"key2": stringPtr("value2"),
			"key3": nil,
		}
		hstore := NewPostgreSQLHStore(data)
		assert.True(t, hstore.Valid)
		assert.Equal(t, data, hstore.Map)

		// Test Value method
		value, err := hstore.Value()
		require.NoError(t, err)
		valueStr, ok := value.(string)
		require.True(t, ok)
		assert.Contains(t, valueStr, `"key1"=>"value1"`)
		assert.Contains(t, valueStr, `"key2"=>"value2"`)
		assert.Contains(t, valueStr, `"key3"=>NULL`)

		// Test Scan method
		var scanned PostgreSQLHStore
		err = scanned.Scan(`"name"=>"John", "age"=>"30", "city"=>NULL`)
		require.NoError(t, err)
		assert.True(t, scanned.Valid)
		assert.Equal(t, "John", *scanned.Map["name"])
		assert.Equal(t, "30", *scanned.Map["age"])
		assert.Nil(t, scanned.Map["city"])

		// Test null scan
		err = scanned.Scan(nil)
		require.NoError(t, err)
		assert.False(t, scanned.Valid)
	})
}

func TestPostgreSQLTypeMapper(t *testing.T) {
	t.Run("Type Registration", func(t *testing.T) {
		mapper := NewPostgreSQLTypeMapper()
		assert.NotNil(t, mapper)

		// Test that custom types map is initialized
		assert.NotNil(t, mapper.customTypes)
	})
}

func TestPostgreSQLEnhancedConnection(t *testing.T) {
	t.Run("Configuration", func(t *testing.T) {
		// Test with default configuration
		conn := &PostgreSQLEnhancedConnection{}
		defaultConfig := &PostgreSQLConfig{
			PreferSimpleProtocol:      false,
			ConnMaxLifetime:           time.Hour,
			ConnMaxIdleTime:           30 * time.Minute,
			MaxConnections:            25,
			MinConnections:            2,
			HealthCheckPeriod:         30 * time.Second,
			MaxConnLifetimeJitter:     10 * time.Second,
		}

		conn.SetConnectionConfig(defaultConfig)
		retrievedConfig := conn.GetConnectionConfig()
		assert.Equal(t, defaultConfig, retrievedConfig)
	})

	t.Run("Bulk Operations", func(t *testing.T) {
		// Test struct analysis for bulk insert
		conn := &PostgreSQLEnhancedConnection{}

		type TestStruct struct {
			ID   int    `db:"id"`
			Name string `db:"name"`
			Age  int    `db:"age"`
		}

		columns, indices := conn.analyzeStructForBulkInsert(reflect.TypeOf(TestStruct{}))

		expectedColumns := []string{"id", "name", "age"}
		expectedIndices := []int{0, 1, 2}

		assert.Equal(t, expectedColumns, columns)
		assert.Equal(t, expectedIndices, indices)
	})
}

func TestIndexSpec(t *testing.T) {
	t.Run("Build Index Query", func(t *testing.T) {
		conn := &PostgreSQLEnhancedConnection{}

		// Test basic index
		spec := IndexSpec{
			Name:    "idx_users_name",
			Table:   "users",
			Columns: []string{"name"},
		}
		query := conn.buildIndexQuery(spec)
		expected := "CREATE INDEX idx_users_name ON users (name)"
		assert.Equal(t, expected, query)

		// Test unique index with type
		spec = IndexSpec{
			Name:      "idx_users_email",
			Table:     "users",
			Columns:   []string{"email"},
			IndexType: BTreeIndex,
			Unique:    true,
		}
		query = conn.buildIndexQuery(spec)
		expected = "CREATE UNIQUE INDEX idx_users_email ON users USING BTREE (email)"
		assert.Equal(t, expected, query)

		// Test GIN index with WHERE clause
		spec = IndexSpec{
			Name:      "idx_users_data_gin",
			Table:     "users",
			Columns:   []string{"data"},
			IndexType: GINIndex,
			Where:     "data IS NOT NULL",
		}
		query = conn.buildIndexQuery(spec)
		expected = "CREATE INDEX idx_users_data_gin ON users USING GIN (data) WHERE data IS NOT NULL"
		assert.Equal(t, expected, query)

		// Test index with INCLUDE columns and WITH options
		spec = IndexSpec{
			Name:      "idx_users_complex",
			Table:     "users",
			Columns:   []string{"name", "created_at"},
			Include:   []string{"email", "updated_at"},
			With:      map[string]string{"fillfactor": "90"},
			Concurrent: true,
		}
		query = conn.buildIndexQuery(spec)
		assert.Contains(t, query, "CREATE INDEX CONCURRENTLY")
		assert.Contains(t, query, "INCLUDE (email, updated_at)")
		assert.Contains(t, query, "WITH (fillfactor = 90)")
	})
}

func TestBulkOperations(t *testing.T) {
	t.Run("Batch Size Calculation", func(t *testing.T) {
		// Create mock data
		data := make([][]interface{}, 2500)
		for i := range data {
			data[i] = []interface{}{i, fmt.Sprintf("name_%d", i)}
		}

		// Test that batch operations would split correctly
		batchSize := 1000
		expectedBatches := 3 // 1000, 1000, 500

		batches := 0
		for i := 0; i < len(data); i += batchSize {
			batches++
			end := i + batchSize
			if end > len(data) {
				end = len(data)
			}
			batch := data[i:end]

			if batches < 3 {
				assert.Len(t, batch, batchSize)
			} else {
				assert.Len(t, batch, 500) // Last batch
			}
		}

		assert.Equal(t, expectedBatches, batches)
	})
}

func BenchmarkPostgreSQLTypes(b *testing.B) {
	b.Run("JSONB Marshal", func(b *testing.B) {
		data := map[string]interface{}{
			"name": "test",
			"age":  30,
			"tags": []string{"tag1", "tag2", "tag3"},
		}
		jsonb := NewJSONB(data)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := jsonb.Value()
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("JSONB Unmarshal", func(b *testing.B) {
		jsonStr := `{"name": "test", "age": 30, "tags": ["tag1", "tag2", "tag3"]}`
		var jsonb JSONB[map[string]interface{}]

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := jsonb.Scan(jsonStr)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Array Value", func(b *testing.B) {
		elements := make([]string, 100)
		for i := range elements {
			elements[i] = fmt.Sprintf("element_%d", i)
		}
		array := NewPostgreSQLArray(elements)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := array.Value()
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// Helper function for tests
func stringPtr(s string) *string {
	return &s
}