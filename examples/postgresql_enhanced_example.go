package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/go-gorp/gorp/v3/db"
)

// User represents a user with PostgreSQL-specific types
type User struct {
	ID       int                    `db:"id"`
	Name     string                 `db:"name"`
	Email    string                 `db:"email"`
	Profile  db.JSONB[UserProfile]  `db:"profile"`
	Tags     db.PostgreSQLArray[string] `db:"tags"`
	UUID     db.PostgreSQLUUID      `db:"uuid"`
	Metadata db.PostgreSQLHStore    `db:"metadata"`
}

// UserProfile represents the JSONB profile data
type UserProfile struct {
	Age      int      `json:"age"`
	City     string   `json:"city"`
	Hobbies  []string `json:"hobbies"`
	Settings map[string]interface{} `json:"settings"`
}

func main() {
	ctx := context.Background()

	// Create enhanced PostgreSQL connection
	fmt.Println("=== Enhanced PostgreSQL Connection Example ===")

	pgConfig := &db.PostgreSQLConfig{
		PreferSimpleProtocol:      false,
		ConnMaxLifetime:           time.Hour,
		ConnMaxIdleTime:           30 * time.Minute,
		MaxConnections:            50,
		MinConnections:            5,
		HealthCheckPeriod:         30 * time.Second,
		MaxConnLifetimeJitter:     10 * time.Second,
	}

	conn, err := db.NewDatabaseBuilder(db.PostgreSQLConnectionType).
		WithDSN("postgres://user:password@localhost:5432/testdb?sslmode=disable").
		WithRole(db.PrimaryRole).
		WithMaxOpenConns(int(pgConfig.MaxConnections)).
		WithMaxIdleConns(int(pgConfig.MinConnections)).
		WithConnMaxLifetime(pgConfig.ConnMaxLifetime).
		Build(ctx)

	if err != nil {
		log.Printf("Failed to create PostgreSQL connection: %v", err)
		return
	}
	defer conn.Close()

	// Cast to enhanced connection (in real usage, you'd have a factory method)
	enhancedConn := &db.PostgreSQLEnhancedConnection{}
	enhancedConn.SetConnectionConfig(pgConfig)

	fmt.Printf("PostgreSQL connection created successfully\n")
	fmt.Printf("Connection type: %s, role: %s\n", conn.Type(), conn.Role())

	// Example 1: Working with JSONB data
	fmt.Println("\n=== JSONB Example ===")
	profile := db.NewJSONB(UserProfile{
		Age:  30,
		City: "San Francisco",
		Hobbies: []string{"programming", "reading", "hiking"},
		Settings: map[string]interface{}{
			"theme":         "dark",
			"notifications": true,
			"language":      "en",
		},
	})

	fmt.Printf("JSONB Profile: %+v\n", profile.Data)
	value, err := profile.Value()
	if err == nil {
		fmt.Printf("JSONB as SQL value: %s\n", value)
	}

	// Example 2: Working with PostgreSQL arrays
	fmt.Println("\n=== PostgreSQL Array Example ===")
	tags := db.NewPostgreSQLArray([]string{"developer", "go-enthusiast", "postgresql-user"})
	fmt.Printf("Array tags: %v\n", tags.Elements)

	arrayValue, err := tags.Value()
	if err == nil {
		fmt.Printf("Array as SQL value: %s\n", arrayValue)
	}

	// Example 3: Working with UUID
	fmt.Println("\n=== PostgreSQL UUID Example ===")
	uuid := [16]byte{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0}
	pgUUID := db.NewPostgreSQLUUID(uuid)
	fmt.Printf("UUID: %s\n", pgUUID.String())

	// Example 4: Working with HStore
	fmt.Println("\n=== PostgreSQL HStore Example ===")
	metadata := map[string]*string{
		"department": stringPtr("Engineering"),
		"level":      stringPtr("Senior"),
		"remote":     stringPtr("true"),
		"manager":    nil, // NULL value
	}
	hstore := db.NewPostgreSQLHStore(metadata)
	fmt.Printf("HStore metadata: %+v\n", hstore.Map)

	hstoreValue, err := hstore.Value()
	if err == nil {
		fmt.Printf("HStore as SQL value: %s\n", hstoreValue)
	}

	// Example 5: Bulk operations
	fmt.Println("\n=== Bulk Operations Example ===")

	// Prepare bulk insert data
	users := make([]User, 1000)
	for i := range users {
		users[i] = User{
			ID:    i + 1,
			Name:  fmt.Sprintf("User %d", i+1),
			Email: fmt.Sprintf("user%d@example.com", i+1),
			Profile: db.NewJSONB(UserProfile{
				Age:  25 + (i % 40),
				City: []string{"New York", "San Francisco", "London", "Tokyo"}[i%4],
				Hobbies: []string{"reading", "coding", "gaming"}[:((i%3)+1)],
			}),
			Tags: db.NewPostgreSQLArray([]string{
				fmt.Sprintf("tag_%d", i%10),
				fmt.Sprintf("category_%s", []string{"A", "B", "C"}[i%3]),
			}),
		}
	}

	fmt.Printf("Prepared %d users for bulk insert\n", len(users))

	// Example 6: Index creation
	fmt.Println("\n=== Index Creation Example ===")
	indexSpecs := []db.IndexSpec{
		{
			Name:      "idx_users_email",
			Table:     "users",
			Columns:   []string{"email"},
			IndexType: db.BTreeIndex,
			Unique:    true,
		},
		{
			Name:      "idx_users_profile_gin",
			Table:     "users",
			Columns:   []string{"profile"},
			IndexType: db.GINIndex,
		},
		{
			Name:      "idx_users_tags_gin",
			Table:     "users",
			Columns:   []string{"tags"},
			IndexType: db.GINIndex,
		},
		{
			Name:      "idx_users_name_active",
			Table:     "users",
			Columns:   []string{"name"},
			IndexType: db.BTreeIndex,
			Where:     "active = true",
		},
	}

	for _, spec := range indexSpecs {
		fmt.Printf("Index spec: %s on %s(%s) using %s\n",
			spec.Name, spec.Table, spec.Columns[0], spec.IndexType)
		if spec.Where != "" {
			fmt.Printf("  WHERE: %s\n", spec.Where)
		}
	}

	// Example 7: Connection pool monitoring
	fmt.Println("\n=== Connection Pool Monitoring Example ===")
	if pgConn, ok := conn.(*db.PostgreSQLConnection); ok {
		stats := pgConn.Stats()
		fmt.Printf("Connection Pool Stats:\n")
		fmt.Printf("  Max Open Connections: %d\n", stats.MaxOpenConnections)
		fmt.Printf("  Open Connections: %d\n", stats.OpenConnections)
		fmt.Printf("  In Use: %d\n", stats.InUse)
		fmt.Printf("  Idle: %d\n", stats.Idle)
	}

	// Example 8: Type mapper usage
	fmt.Println("\n=== Type Mapper Example ===")
	typeMapper := db.NewPostgreSQLTypeMapper()
	fmt.Printf("Type mapper initialized with %d custom types\n", len(typeMapper.customTypes))

	// Example 9: Bulk operations helper
	fmt.Println("\n=== Bulk Operations Helper Example ===")
	if enhancedConn.PostgreSQLConnection != nil {
		bulkOps := db.NewPostgreSQLBulkOperations(enhancedConn)
		fmt.Printf("Bulk operations helper created\n")

		// Simulate batch insert parameters
		columns := []string{"id", "name", "email"}
		data := [][]interface{}{
			{1, "John Doe", "john@example.com"},
			{2, "Jane Smith", "jane@example.com"},
			{3, "Bob Wilson", "bob@example.com"},
		}

		fmt.Printf("Would batch insert %d rows with batch size 1000\n", len(data))
		_ = bulkOps // Acknowledge usage
	}

	fmt.Println("\n=== PostgreSQL Enhanced Features Demonstration Complete ===")
}

// Helper function
func stringPtr(s string) *string {
	return &s
}