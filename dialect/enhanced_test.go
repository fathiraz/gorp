package dialect

import (
	"testing"
)

func TestEnhancedMySQLDialect(t *testing.T) {
	dialect := NewEnhancedMySQLDialect("InnoDB", "utf8mb4", "8.0")

	t.Run("Basic Properties", func(t *testing.T) {
		if dialect.Name() != "mysql" {
			t.Errorf("Expected name 'mysql', got %s", dialect.Name())
		}

		if dialect.GetVersion() != "8.0" {
			t.Errorf("Expected version '8.0', got %s", dialect.GetVersion())
		}
	})

	t.Run("Feature Support", func(t *testing.T) {
		if !dialect.SupportsFeature(FeatureJSON) {
			t.Error("MySQL 8.0 should support JSON")
		}

		if !dialect.SupportsFeature(FeatureUpsert) {
			t.Error("MySQL should support upsert")
		}

		if !dialect.SupportsFeature(FeatureGeneratedColumns) {
			t.Error("MySQL 8.0 should support generated columns")
		}

		if dialect.SupportsFeature(FeatureJSONB) {
			t.Error("MySQL should not support JSONB")
		}

		if dialect.SupportsFeature(FeatureArrays) {
			t.Error("MySQL should not support native arrays")
		}
	})

	t.Run("JSON Operations", func(t *testing.T) {
		expected := "`data` JSON NOT NULL"
		result := dialect.CreateJSONColumn("data", "NOT NULL")
		if result != expected {
			t.Errorf("Expected %s, got %s", expected, result)
		}

		expected = "JSON_EXTRACT(data, '$.name')"
		result = dialect.JSONExtract("data", "'$.name'")
		if result != expected {
			t.Errorf("Expected %s, got %s", expected, result)
		}
	})

	t.Run("Upsert Operations", func(t *testing.T) {
		columns := []string{"id", "name", "email"}
		conflictCols := []string{"email"}
		updateCols := []string{"name"}

		result := dialect.BuildUpsert("users", columns, conflictCols, updateCols)

		// Should contain INSERT INTO and ON DUPLICATE KEY UPDATE
		if !contains(result, "INSERT INTO") {
			t.Error("Upsert should contain INSERT INTO")
		}

		if !contains(result, "ON DUPLICATE KEY UPDATE") {
			t.Error("Upsert should contain ON DUPLICATE KEY UPDATE")
		}
	})

	t.Run("Generated Columns", func(t *testing.T) {
		result := dialect.CreateStoredGeneratedColumn("full_name", "CONCAT(first_name, ' ', last_name)", "VARCHAR(255)")
		expected := "`full_name` VARCHAR(255) GENERATED ALWAYS AS (CONCAT(first_name, ' ', last_name)) STORED"

		if result != expected {
			t.Errorf("Expected %s, got %s", expected, result)
		}
	})

	t.Run("Capabilities", func(t *testing.T) {
		caps := dialect.GetCapabilities()

		if !caps.SupportsJSON {
			t.Error("MySQL 8.0 capabilities should support JSON")
		}

		if caps.JSONType != "JSON" {
			t.Errorf("Expected JSON type 'JSON', got %s", caps.JSONType)
		}

		if caps.UpsertSyntax != UpsertOnDuplicateKey {
			t.Errorf("Expected upsert syntax 'on_duplicate_key', got %s", caps.UpsertSyntax)
		}
	})
}

func TestEnhancedPostgreSQLDialect(t *testing.T) {
	dialect := NewEnhancedPostgreSQLDialect("14", false)

	t.Run("Basic Properties", func(t *testing.T) {
		if dialect.Name() != "postgresql" {
			t.Errorf("Expected name 'postgresql', got %s", dialect.Name())
		}

		if dialect.GetVersion() != "14" {
			t.Errorf("Expected version '14', got %s", dialect.GetVersion())
		}
	})

	t.Run("Feature Support", func(t *testing.T) {
		if !dialect.SupportsFeature(FeatureJSON) {
			t.Error("PostgreSQL should support JSON")
		}

		if !dialect.SupportsFeature(FeatureJSONB) {
			t.Error("PostgreSQL should support JSONB")
		}

		if !dialect.SupportsFeature(FeatureArrays) {
			t.Error("PostgreSQL should support arrays")
		}

		if !dialect.SupportsFeature(FeaturePartialIndex) {
			t.Error("PostgreSQL should support partial indexes")
		}

		if !dialect.SupportsFeature(FeaturePartitioning) {
			t.Error("PostgreSQL 14 should support partitioning")
		}
	})

	t.Run("Array Operations", func(t *testing.T) {
		result := dialect.CreateArrayColumn("INTEGER", 1)
		expected := "INTEGER[]"
		if result != expected {
			t.Errorf("Expected %s, got %s", expected, result)
		}

		result = dialect.ArrayAppend("tags", "'new_tag'")
		expected = "array_append(tags, 'new_tag')"
		if result != expected {
			t.Errorf("Expected %s, got %s", expected, result)
		}
	})

	t.Run("Upsert Operations", func(t *testing.T) {
		columns := []string{"id", "name", "email"}
		conflictCols := []string{"email"}
		updateCols := []string{"name"}

		result := dialect.BuildUpsert("users", columns, conflictCols, updateCols)

		if !contains(result, "INSERT INTO") {
			t.Error("Upsert should contain INSERT INTO")
		}

		if !contains(result, "ON CONFLICT") {
			t.Error("Upsert should contain ON CONFLICT")
		}

		if !contains(result, "DO UPDATE SET") {
			t.Error("Upsert should contain DO UPDATE SET")
		}
	})

	t.Run("Advanced Indexing", func(t *testing.T) {
		result := dialect.CreatePartialIndex("idx_active", "users", []string{"email"}, "active = true")
		expected := `CREATE INDEX "idx_active" ON "users" ("email") WHERE active = true`

		if result != expected {
			t.Errorf("Expected %s, got %s", expected, result)
		}
	})
}

func TestEnhancedSQLiteDialect(t *testing.T) {
	dialect := NewEnhancedSQLiteDialect("3.38")

	t.Run("Basic Properties", func(t *testing.T) {
		if dialect.Name() != "sqlite" {
			t.Errorf("Expected name 'sqlite', got %s", dialect.Name())
		}

		if dialect.GetVersion() != "3.38" {
			t.Errorf("Expected version '3.38', got %s", dialect.GetVersion())
		}
	})

	t.Run("Feature Support", func(t *testing.T) {
		if !dialect.SupportsFeature(FeatureJSON) {
			t.Error("SQLite 3.38 should support JSON")
		}

		if !dialect.SupportsFeature(FeatureFTS) {
			t.Error("SQLite should support FTS")
		}

		if !dialect.SupportsFeature(FeaturePartialIndex) {
			t.Error("SQLite should support partial indexes")
		}

		if dialect.SupportsFeature(FeatureJSONB) {
			t.Error("SQLite should not support JSONB")
		}

		if dialect.SupportsFeature(FeatureArrays) {
			t.Error("SQLite should not support native arrays")
		}
	})

	t.Run("PRAGMA Operations", func(t *testing.T) {
		pragmas := dialect.OptimizeForWAL()

		if len(pragmas) == 0 {
			t.Error("WAL optimization should return pragmas")
		}

		// Check if WAL mode is set
		found := false
		for _, pragma := range pragmas {
			if contains(pragma, "journal_mode = WAL") {
				found = true
				break
			}
		}
		if !found {
			t.Error("WAL optimization should include journal_mode = WAL")
		}
	})

	t.Run("FTS Operations", func(t *testing.T) {
		result := dialect.CreateFTSTable("docs_fts", []string{"title", "content"}, "fts5")
		expected := `CREATE VIRTUAL TABLE "docs_fts" USING fts5("title", "content")`

		if result != expected {
			t.Errorf("Expected %s, got %s", expected, result)
		}
	})

	t.Run("Upsert Operations", func(t *testing.T) {
		columns := []string{"id", "name"}

		result := dialect.BuildInsertIgnore("users", columns)
		expected := `INSERT OR IGNORE INTO "users" ("id", "name") VALUES (?, ?)`

		if result != expected {
			t.Errorf("Expected %s, got %s", expected, result)
		}
	})
}

func TestDialectRegistry(t *testing.T) {
	t.Run("Registry Operations", func(t *testing.T) {
		// Test getting registered dialects
		dialectNames := EnhancedRegistry.List()

		expectedDialects := []string{"mysql", "postgresql", "sqlite"}
		for _, expected := range expectedDialects {
			found := false
			for _, name := range dialectNames {
				if name == expected {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Expected dialect %s to be registered", expected)
			}
		}

		// Test getting specific dialects
		mysql, err := GetExtendedDialect("mysql")
		if err != nil {
			t.Errorf("Error getting MySQL dialect: %v", err)
		}
		if mysql.Name() != "mysql" {
			t.Errorf("Expected MySQL dialect name 'mysql', got %s", mysql.Name())
		}
	})

	t.Run("Feature Matrix", func(t *testing.T) {
		matrix := GetFeatureMatrix()

		if len(matrix) == 0 {
			t.Error("Feature matrix should not be empty")
		}

		// Check that MySQL appears in matrix
		if _, exists := matrix["mysql"]; !exists {
			t.Error("MySQL should appear in feature matrix")
		}

		// Check that JSON feature is properly mapped
		if mysqlFeatures, exists := matrix["mysql"]; exists {
			if !mysqlFeatures[FeatureJSON] {
				t.Error("MySQL should support JSON in feature matrix")
			}
		}
	})
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && s[:len(substr)] == substr ||
		   (len(s) > len(substr) && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}