// Package testing provides comprehensive testing utilities for GORP enhancements
// including MockExecutor interfaces, Docker integration, transaction-based isolation,
// schema verification, property-based testing, performance regression testing,
// test data builders with faker integration, and coverage reporting.
package testing

import (
	"context"
	"testing"
	"time"

	"github.com/fathiraz/gorp/db"
	"github.com/fathiraz/gorp/mapping"
)

// Example usage demonstrating the comprehensive testing framework

// ExampleUser represents a user entity for testing
type ExampleUser struct {
	ID        string    `db:"id" json:"id"`
	Username  string    `db:"username" json:"username"`
	Email     string    `db:"email" json:"email"`
	FirstName string    `db:"first_name" json:"first_name"`
	LastName  string    `db:"last_name" json:"last_name"`
	Phone     string    `db:"phone" json:"phone"`
	CreatedAt time.Time `db:"created_at" json:"created_at"`
	UpdatedAt time.Time `db:"updated_at" json:"updated_at"`
	Active    bool      `db:"active" json:"active"`
}

// ExampleUserMapper implements mapping for ExampleUser
type ExampleUserMapper struct {
	tableName string
}

func NewExampleUserMapper() *ExampleUserMapper {
	return &ExampleUserMapper{tableName: "users"}
}

func (m *ExampleUserMapper) TableName() string {
	return m.tableName
}

func (m *ExampleUserMapper) ColumnMap() map[string]string {
	return map[string]string{
		"ID":        "id",
		"Username":  "username",
		"Email":     "email",
		"FirstName": "first_name",
		"LastName":  "last_name",
		"Phone":     "phone",
		"CreatedAt": "created_at",
		"UpdatedAt": "updated_at",
		"Active":    "active",
	}
}

func (m *ExampleUserMapper) PrimaryKey() []string {
	return []string{"id"}
}

func (m *ExampleUserMapper) Indexes() []mapping.IndexDefinition {
	return []mapping.IndexDefinition{
		{
			Name:    "idx_users_email",
			Columns: []string{"email"},
			Unique:  true,
		},
		{
			Name:    "idx_users_username",
			Columns: []string{"username"},
			Unique:  true,
		},
		{
			Name:    "idx_users_active",
			Columns: []string{"active"},
			Unique:  false,
		},
	}
}

func (m *ExampleUserMapper) ToRow(entity ExampleUser) (map[string]interface{}, error) {
	return map[string]interface{}{
		"id":         entity.ID,
		"username":   entity.Username,
		"email":      entity.Email,
		"first_name": entity.FirstName,
		"last_name":  entity.LastName,
		"phone":      entity.Phone,
		"created_at": entity.CreatedAt,
		"updated_at": entity.UpdatedAt,
		"active":     entity.Active,
	}, nil
}

func (m *ExampleUserMapper) FromRow(row map[string]interface{}) (ExampleUser, error) {
	user := ExampleUser{}

	if id, ok := row["id"].(string); ok {
		user.ID = id
	}
	if username, ok := row["username"].(string); ok {
		user.Username = username
	}
	if email, ok := row["email"].(string); ok {
		user.Email = email
	}
	if firstName, ok := row["first_name"].(string); ok {
		user.FirstName = firstName
	}
	if lastName, ok := row["last_name"].(string); ok {
		user.LastName = lastName
	}
	if phone, ok := row["phone"].(string); ok {
		user.Phone = phone
	}
	if createdAt, ok := row["created_at"].(time.Time); ok {
		user.CreatedAt = createdAt
	}
	if updatedAt, ok := row["updated_at"].(time.Time); ok {
		user.UpdatedAt = updatedAt
	}
	if active, ok := row["active"].(bool); ok {
		user.Active = active
	}

	return user, nil
}

func (m *ExampleUserMapper) Schema() *mapping.TableSchema {
	// Simplified schema implementation
	return &mapping.TableSchema{
		Name: m.tableName,
		// Additional schema details would be implemented here
	}
}

// ExampleTestSuite demonstrates comprehensive testing usage
func ExampleTestSuite(t *testing.T) {
	// 1. Setup Docker test environment
	config := DefaultDockerConfig()
	suite := NewTestSuite(t, config)

	// 2. Create test data builders
	userMapper := NewExampleUserMapper()
	dataBuilder := NewDataBuilder(userMapper).
		WithEmail("Email").
		WithName("FirstName").
		WithName("LastName").
		WithPhone("Phone").
		Count(10)

	// 3. Run tests on all databases
	suite.RunOnAllDatabases(func(t *testing.T, conn db.Connection, dbType db.ConnectionType) {
		// 3a. Schema verification
		t.Run("Schema_Verification", func(t *testing.T) {
			verifier := NewSchemaVerifier(conn)

			// Verify table creation
			err := verifier.CreateTableFromMapping(context.Background(), userMapper)
			if err != nil {
				t.Skipf("Cannot create table for %s: %v", dbType, err)
			}

			// Verify mapping consistency
			verifier.VerifyMappingConsistency(t, userMapper)
		})

		// 3b. Transaction isolation testing
		t.Run("Transaction_Isolation", func(t *testing.T) {
			testSuite := NewTransactionTestSuite(conn)

			test := IsolatedTest{
				Name: "User_CRUD_Isolation",
				Test: func(t *testing.T, tx db.Transaction) {
					// Generate test data
					users := dataBuilder.Build()

					// Test CRUD operations in isolation
					for _, user := range users {
						row, err := userMapper.ToRow(user)
						if err != nil {
							t.Fatalf("Failed to convert user to row: %v", err)
						}

						// Insert user
						_, err = tx.ExecContext(context.Background(),
							"INSERT INTO users (id, username, email, first_name, last_name, phone, created_at, updated_at, active) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
							row["id"], row["username"], row["email"], row["first_name"], row["last_name"], row["phone"], row["created_at"], row["updated_at"], row["active"])
						if err != nil {
							t.Fatalf("Failed to insert user: %v", err)
						}
					}
				},
				Parallel: true,
			}

			testSuite.RunIsolated(t, test)
		})

		// 3c. Property-based testing
		t.Run("Property_Based_Testing", func(t *testing.T) {
			runner := NewPropertyTestRunner()

			generator := NewDatabaseEntityGenerator(userMapper)
			generator.AddConstraint("Email", func(r *rand.Rand) any {
				return fmt.Sprintf("user%d@example.com", r.Intn(10000))
			})

			test := PropertyTest[ExampleUser]{
				Name:      "User_CRUD_Properties",
				Generator: generator,
				Property:  CRUDInvariantProperty(conn, userMapper),
				NumTests:  25,
			}

			runner.Run(t, test)
		})

		// 3d. Performance testing
		t.Run("Performance_Testing", func(t *testing.T) {
			perfRunner := NewPerformanceTestRunner(conn, "test-baselines")

			perfTest := CreateCRUDPerformanceTest(userMapper, func(size int) []ExampleUser {
				return dataBuilder.Count(size).Build()
			})

			perfRunner.RunPerformanceTest(t, perfTest)
		})
	})

	// 4. Coverage validation (would be run separately with go test -cover)
	t.Run("Coverage_Validation", func(t *testing.T) {
		policy := DefaultCoveragePolicy()
		policy.MinimumCoverage = 70.0 // Lower for testing package

		// This would typically be run in a separate coverage test
		// validator := NewCoverageValidator(".", policy)
		// validator.ValidateCoverage(t)
		t.Skip("Coverage validation requires separate execution with -cover flag")
	})
}

// ExampleMockUsage demonstrates mock usage for unit testing
func ExampleMockUsage(t *testing.T) {
	// Create mocks
	mockConn := NewMockConnection()
	mockTx := NewMockTransaction()
	mockExecutor := NewMockExecutor()

	// Setup mock expectations
	ctx := context.Background()
	mockConn.On("Begin", ctx).Return(mockTx, nil)
	mockTx.On("ExecContext", ctx, "INSERT INTO users VALUES ($1, $2)", "1", "test@example.com").Return(NewMockResult(1, 1), nil)
	mockTx.On("Commit").Return(nil)

	// Test code using mocks
	tx, err := mockConn.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	result, err := tx.ExecContext(ctx, "INSERT INTO users VALUES ($1, $2)", "1", "test@example.com")
	if err != nil {
		t.Fatalf("ExecContext failed: %v", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected != 1 {
		t.Errorf("Expected 1 row affected, got %d", rowsAffected)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify all expectations were met
	mockConn.AssertExpectations(t)
	mockTx.AssertExpectations(t)
}

// ExampleDataGeneration demonstrates test data generation
func ExampleDataGeneration(t *testing.T) {
	mapper := NewExampleUserMapper()

	// Basic data builder
	users := NewDataBuilder(mapper).
		WithEmail("Email").
		WithName("FirstName").
		WithName("LastName").
		WithPhone("Phone").
		WithNumberRange("ID", 1, 1000).
		Count(5).
		Build()

	t.Logf("Generated %d users", len(users))
	for i, user := range users {
		t.Logf("User %d: %s <%s>", i+1, user.Username, user.Email)
	}

	// Factory-based generation
	factory := NewTestDataFactory()
	userData := factory.CreateUser()
	t.Logf("Factory user: %+v", userData)

	// Batch generation
	batchGen := NewBatchDataGenerator()
	largeUserSet := batchGen.GenerateBatch(1000, func() ExampleUser {
		return NewDataBuilder(mapper).BuildOne()
	})

	t.Logf("Generated batch of %d users", len(largeUserSet))
}

// ExampleSchemaValidation demonstrates schema verification
func ExampleSchemaValidation(t *testing.T) {
	// This would typically use a real database connection
	// connection := CreateTestConnection(t, db.PostgreSQLConnectionType)
	// verifier := NewSchemaVerifier(connection)

	mapper := NewExampleUserMapper()

	expectedTable := ExpectedTable{
		Name:   "users",
		Schema: "public",
		Columns: []ExpectedColumn{
			{Name: "id", DataType: "varchar", Nullable: false, IsPrimary: true},
			{Name: "username", DataType: "varchar", Nullable: false},
			{Name: "email", DataType: "varchar", Nullable: false},
			{Name: "first_name", DataType: "varchar", Nullable: true},
			{Name: "last_name", DataType: "varchar", Nullable: true},
			{Name: "phone", DataType: "varchar", Nullable: true},
			{Name: "created_at", DataType: "timestamp", Nullable: false},
			{Name: "updated_at", DataType: "timestamp", Nullable: false},
			{Name: "active", DataType: "boolean", Nullable: false},
		},
		Indexes: []ExpectedIndex{
			{Name: "idx_users_email", Columns: []string{"email"}, IsUnique: true},
			{Name: "idx_users_username", Columns: []string{"username"}, IsUnique: true},
		},
	}

	// This demonstrates the expected verification pattern
	// verifier.VerifyTable(t, expectedTable)
	// verifier.VerifyMappingConsistency(t, mapper)

	t.Logf("Schema validation would verify table: %s with %d columns and %d indexes",
		expectedTable.Name, len(expectedTable.Columns), len(expectedTable.Indexes))
}

// Best Practices Documentation

/*
TESTING BEST PRACTICES FOR GORP:

1. TEST ORGANIZATION:
   - Use TestSuite for comprehensive database testing
   - Separate unit tests (with mocks) from integration tests
   - Use Docker containers for consistent test environments
   - Group related tests in subtests

2. DATA GENERATION:
   - Use DataBuilder for consistent test data
   - Leverage faker integration for realistic data
   - Use constraints for domain-specific data generation
   - Prefer builders over hardcoded test data

3. TRANSACTION ISOLATION:
   - Always use transaction isolation for database tests
   - Use savepoints for nested test scenarios
   - Rollback transactions to ensure test independence
   - Consider concurrent test scenarios

4. SCHEMA VERIFICATION:
   - Verify mapping consistency between code and database
   - Use schema verification for migration testing
   - Test against multiple database types
   - Validate indexes and constraints

5. PERFORMANCE TESTING:
   - Establish baselines for critical operations
   - Use property-based testing for edge cases
   - Monitor performance regressions
   - Test with realistic data volumes

6. COVERAGE REQUIREMENTS:
   - Maintain minimum 80% overall coverage
   - Require 90% coverage for critical packages
   - Use branch coverage for complex logic
   - Document any coverage exceptions

7. MOCK USAGE:
   - Use mocks for unit testing business logic
   - Mock external dependencies, not database logic
   - Verify mock expectations
   - Prefer integration tests for database operations

8. DOCKER INTEGRATION:
   - Use consistent database versions across environments
   - Implement proper container lifecycle management
   - Use parallel container startup for performance
   - Provide fallback to environment-based connections
*/