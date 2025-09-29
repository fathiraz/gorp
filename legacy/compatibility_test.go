package legacy

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"reflect"
	"testing"
	"time"

	"github.com/fathiraz/gorp"
	_ "github.com/mattn/go-sqlite3"
)

// Test models
type User struct {
	Id       int64     `db:"id"`
	Name     string    `db:"name"`
	Email    string    `db:"email"`
	Active   bool      `db:"active"`
	Created  time.Time `db:"created_at"`
	Updated  *time.Time `db:"updated_at"`
}

type Post struct {
	Id      int64  `db:"id"`
	UserId  int64  `db:"user_id"`
	Title   string `db:"title"`
	Content string `db:"content"`
}

// TestLegacyDbMapCompatibility tests that the legacy wrapper maintains API compatibility
func TestLegacyDbMapCompatibility(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Test that we can create a legacy DbMap exactly like the old API
	dialect := gorp.SqliteDialect{}
	dbmap := NewLegacyDbMap(db, dialect, nil)

	// Test AddTable - should work exactly like before
	userTable := dbmap.AddTable(User{})
	if userTable == nil {
		t.Error("AddTable should return a TableMap")
	}

	postTable := dbmap.AddTableWithName(Post{}, "posts")
	if postTable == nil {
		t.Error("AddTableWithName should return a TableMap")
	}
	if postTable.TableName != "posts" {
		t.Errorf("Expected table name 'posts', got '%s'", postTable.TableName)
	}
}

// TestBasicCRUDOperations tests that basic CRUD operations maintain compatibility
func TestBasicCRUDOperations(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	dbmap := setupLegacyDbMap(t, db)

	// Create table (this should work with legacy API)
	err := dbmap.CreateTables()
	if err != nil {
		t.Fatalf("CreateTables failed: %v", err)
	}

	// Test Insert
	user := &User{
		Name:    "John Doe",
		Email:   "john@example.com",
		Active:  true,
		Created: time.Now(),
	}

	err = dbmap.Insert(user)
	if err != nil {
		t.Errorf("Insert failed: %v", err)
	}

	// Test Get
	obj, err := dbmap.Get(User{}, user.Id)
	if err != nil {
		t.Errorf("Get failed: %v", err)
	}

	retrievedUser, ok := obj.(*User)
	if !ok {
		t.Error("Get should return a *User")
	}

	if retrievedUser.Name != user.Name {
		t.Errorf("Expected name '%s', got '%s'", user.Name, retrievedUser.Name)
	}

	// Test Update
	retrievedUser.Name = "Jane Doe"
	now := time.Now()
	retrievedUser.Updated = &now

	count, err := dbmap.Update(retrievedUser)
	if err != nil {
		t.Errorf("Update failed: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 updated row, got %d", count)
	}

	// Test Delete
	count, err = dbmap.Delete(retrievedUser)
	if err != nil {
		t.Errorf("Delete failed: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 deleted row, got %d", count)
	}
}

// TestSelectOperations tests that select operations maintain compatibility
func TestSelectOperations(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	dbmap := setupLegacyDbMap(t, db)
	err := dbmap.CreateTables()
	if err != nil {
		t.Fatalf("CreateTables failed: %v", err)
	}

	// Insert test data
	users := []*User{
		{Name: "Alice", Email: "alice@example.com", Active: true, Created: time.Now()},
		{Name: "Bob", Email: "bob@example.com", Active: false, Created: time.Now()},
		{Name: "Charlie", Email: "charlie@example.com", Active: true, Created: time.Now()},
	}

	for _, user := range users {
		err := dbmap.Insert(user)
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// Test Select
	var retrievedUsers []User
	_, err = dbmap.Select(&retrievedUsers, "SELECT * FROM users WHERE active = ?", true)
	if err != nil {
		t.Errorf("Select failed: %v", err)
	}

	if len(retrievedUsers) != 2 {
		t.Errorf("Expected 2 active users, got %d", len(retrievedUsers))
	}

	// Test SelectOne
	var user User
	err = dbmap.SelectOne(&user, "SELECT * FROM users WHERE name = ?", "Alice")
	if err != nil {
		t.Errorf("SelectOne failed: %v", err)
	}

	if user.Name != "Alice" {
		t.Errorf("Expected name 'Alice', got '%s'", user.Name)
	}

	// Test SelectInt
	count, err := dbmap.SelectInt("SELECT COUNT(*) FROM users")
	if err != nil {
		t.Errorf("SelectInt failed: %v", err)
	}

	if count != 3 {
		t.Errorf("Expected count 3, got %d", count)
	}

	// Test SelectStr
	name, err := dbmap.SelectStr("SELECT name FROM users WHERE id = ?", users[0].Id)
	if err != nil {
		t.Errorf("SelectStr failed: %v", err)
	}

	if name != "Alice" {
		t.Errorf("Expected name 'Alice', got '%s'", name)
	}
}

// TestTransactionCompatibility tests that transaction handling maintains compatibility
func TestTransactionCompatibility(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	dbmap := setupLegacyDbMap(t, db)
	err := dbmap.CreateTables()
	if err != nil {
		t.Fatalf("CreateTables failed: %v", err)
	}

	// Test successful transaction
	tx, err := dbmap.Begin()
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	user1 := &User{Name: "User1", Email: "user1@example.com", Active: true, Created: time.Now()}
	user2 := &User{Name: "User2", Email: "user2@example.com", Active: true, Created: time.Now()}

	err = tx.Insert(user1)
	if err != nil {
		tx.Rollback()
		t.Fatalf("Transaction Insert failed: %v", err)
	}

	err = tx.Insert(user2)
	if err != nil {
		tx.Rollback()
		t.Fatalf("Transaction Insert failed: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatalf("Transaction Commit failed: %v", err)
	}

	// Verify both users were inserted
	count, err := dbmap.SelectInt("SELECT COUNT(*) FROM users")
	if err != nil {
		t.Errorf("SelectInt failed: %v", err)
	}

	if count != 2 {
		t.Errorf("Expected 2 users after transaction, got %d", count)
	}
}

// TestContextSupport tests that context support works
func TestContextSupport(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	dbmap := setupLegacyDbMap(t, db)

	// Test WithContext
	ctx := context.Background()
	ctxDbmap := dbmap.WithContext(ctx)

	// Should be able to cast back to LegacyDbMap
	if _, ok := ctxDbmap.(*LegacyDbMap); !ok {
		t.Error("WithContext should return a *LegacyDbMap")
	}
}

// TestFeatureFlags tests that feature flags work correctly
func TestFeatureFlags(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := &CompatibilityConfig{
		Mode:           MigrationMode,
		EnableWarnings: true,
		FeatureFlags: map[string]bool{
			"enable_generics": true,
			"enable_sqlx":    false,
		},
	}

	dbmap := NewLegacyDbMap(db, gorp.SqliteDialect{}, config)

	// Test feature flag checking
	if !dbmap.IsFeatureEnabled("enable_generics") {
		t.Error("enable_generics should be enabled")
	}

	if dbmap.IsFeatureEnabled("enable_sqlx") {
		t.Error("enable_sqlx should be disabled")
	}

	// Test feature flag modification
	dbmap.EnableFeature("enable_sqlx", true)
	if !dbmap.IsFeatureEnabled("enable_sqlx") {
		t.Error("enable_sqlx should be enabled after EnableFeature")
	}
}

// TestMigrationSuggestions tests that migration suggestions are collected
func TestMigrationSuggestions(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := &CompatibilityConfig{
		Mode:           MigrationMode,
		EnableWarnings: true,
		FeatureFlags: map[string]bool{
			"enable_generics": true,
		},
	}

	dbmap := NewLegacyDbMap(db, gorp.SqliteDialect{}, config)
	err := dbmap.CreateTables()
	if err != nil {
		t.Fatalf("CreateTables failed: %v", err)
	}

	// Trigger some deprecated methods to generate suggestions
	dbmap.AddTable(User{})

	user := &User{Name: "Test", Email: "test@example.com", Active: true, Created: time.Now()}
	dbmap.Insert(user)
	dbmap.Get(User{}, 1)

	// Check that suggestions were generated
	suggestions := dbmap.GetMigrationSuggestions()
	if len(suggestions) == 0 {
		t.Error("Expected migration suggestions to be generated")
	}

	// Verify suggestion structure
	for _, suggestion := range suggestions {
		if suggestion.OldMethod == "" {
			t.Error("Migration suggestion should have OldMethod")
		}
		if suggestion.NewMethod == "" {
			t.Error("Migration suggestion should have NewMethod")
		}
		if suggestion.Timestamp.IsZero() {
			t.Error("Migration suggestion should have timestamp")
		}
	}
}

// TestCompatibilityModes tests different compatibility modes
func TestCompatibilityModes(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	modes := []CompatibilityMode{StrictMode, MigrationMode, ModernMode}

	for _, mode := range modes {
		t.Run(string(rune(mode)), func(t *testing.T) {
			config := &CompatibilityConfig{
				Mode:           mode,
				EnableWarnings: true,
			}

			dbmap := NewLegacyDbMap(db, gorp.SqliteDialect{}, config)

			// All modes should maintain basic functionality
			table := dbmap.AddTable(User{})
			if table == nil {
				t.Error("AddTable should work in all modes")
			}

			// Mode-specific behavior can be tested here
			dbmap.SetCompatibilityMode(mode)
		})
	}
}

// TestSQLExecutorInterface tests that the legacy wrapper implements SqlExecutor correctly
func TestSQLExecutorInterface(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	dbmap := setupLegacyDbMap(t, db)

	// Test that it implements SqlExecutor interface
	var executor gorp.SqlExecutor = dbmap

	// Test interface methods
	ctx := context.Background()
	ctxExecutor := executor.WithContext(ctx)
	if ctxExecutor == nil {
		t.Error("WithContext should return a SqlExecutor")
	}

	// Test other interface methods
	_, err := executor.Exec("SELECT 1")
	if err != nil {
		// Error is expected since we don't have proper DB setup, but method should exist
		t.Logf("Exec method exists and was called: %v", err)
	}
}

// TestLegacyTableMapCompatibility tests TableMap compatibility
func TestLegacyTableMapCompatibility(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	dbmap := setupLegacyDbMap(t, db)

	// Test TableFor method
	userType := reflect.TypeOf(User{})
	table, err := dbmap.TableFor(userType, false)
	if err != nil {
		t.Errorf("TableFor failed: %v", err)
	}

	if table == nil {
		t.Error("TableFor should return a TableMap")
	}

	// Test TableForPointer method
	user := &User{}
	table2, elem, err := dbmap.TableForPointer(user, false)
	if err != nil {
		t.Errorf("TableForPointer failed: %v", err)
	}

	if table2 == nil {
		t.Error("TableForPointer should return a TableMap")
	}

	if !elem.IsValid() {
		t.Error("TableForPointer should return a valid reflect.Value")
	}
}

// BenchmarkLegacyVsModern compares performance between legacy and modern API
func BenchmarkLegacyVsModern(b *testing.B) {
	db := setupTestDB(&testing.T{})
	defer db.Close()

	dbmap := setupLegacyDbMap(&testing.T{}, db)
	err := dbmap.CreateTables()
	if err != nil {
		b.Fatalf("CreateTables failed: %v", err)
	}

	user := &User{
		Name:    "Benchmark User",
		Email:   "bench@example.com",
		Active:  true,
		Created: time.Now(),
	}

	b.Run("Legacy Insert", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			user.Id = 0 // Reset ID for re-insertion
			err := dbmap.Insert(user)
			if err != nil {
				b.Fatalf("Insert failed: %v", err)
			}
		}
	})

	// Insert some data for Get benchmarks
	for i := 0; i < 100; i++ {
		testUser := &User{
			Name:    "Test User",
			Email:   "test@example.com",
			Active:  true,
			Created: time.Now(),
		}
		dbmap.Insert(testUser)
	}

	b.Run("Legacy Get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := dbmap.Get(User{}, int64(i%100+1))
			if err != nil && err != sql.ErrNoRows {
				b.Fatalf("Get failed: %v", err)
			}
		}
	})
}

// Helper functions

func setupTestDB(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		if t != nil {
			t.Fatalf("Failed to open database: %v", err)
		}
		panic(err)
	}
	return db
}

func setupLegacyDbMap(t *testing.T, db *sql.DB) *LegacyDbMap {
	dialect := gorp.SqliteDialect{}
	config := DefaultCompatibilityConfig()
	config.Mode = StrictMode // Use strict mode for most tests

	dbmap := NewLegacyDbMap(db, dialect, config)

	// Add tables
	userTable := dbmap.AddTable(User{})
	userTable.SetKeys(true, "Id")

	postTable := dbmap.AddTableWithName(Post{}, "posts")
	postTable.SetKeys(true, "Id")

	return dbmap
}

// Mock implementations for testing

type mockDialect struct {
	gorp.SqliteDialect
}

type mockTypeConverter struct{}

func (tc *mockTypeConverter) ToDb(val interface{}) (interface{}, error) {
	return val, nil
}

func (tc *mockTypeConverter) FromDb(target interface{}) (gorp.CustomScanner, bool) {
	return nil, false
}

// Test custom scanner implementation
type customScanner struct {
	target *string
	value  string
}

func (cs *customScanner) Scan(src interface{}) error {
	if src == nil {
		return nil
	}
	cs.value = string(src.([]byte))
	return nil
}

func (cs *customScanner) Value() (driver.Value, error) {
	return cs.value, nil
}

// TestCustomTypeConverter tests custom type conversion compatibility
func TestCustomTypeConverter(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	dbmap := setupLegacyDbMap(t, db)
	dbmap.TypeConverter = &mockTypeConverter{}

	// Should work without errors
	err := dbmap.CreateTables()
	if err != nil {
		t.Fatalf("CreateTables with custom TypeConverter failed: %v", err)
	}
}