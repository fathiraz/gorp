package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/fathiraz/gorp/dialect"
)

func main() {
	// Demonstrate enhanced dialect system
	fmt.Println("=== Enhanced GORP Dialect System Demo ===")

	// Test MySQL dialect
	fmt.Println("\n--- MySQL Enhanced Dialect ---")
	testMySQLDialect()

	// Test PostgreSQL dialect
	fmt.Println("\n--- PostgreSQL Enhanced Dialect ---")
	testPostgreSQLDialect()

	// Test SQLite dialect
	fmt.Println("\n--- SQLite Enhanced Dialect ---")
	testSQLiteDialect()

	// Show feature comparison matrix
	fmt.Println("\n--- Feature Comparison Matrix ---")
	showFeatureMatrix()

	// Demonstrate performance benchmarking
	fmt.Println("\n--- Performance Benchmarking ---")
	demonstrateBenchmarking()
}

func testMySQLDialect() {
	mysql, err := dialect.GetExtendedDialect("mysql")
	if err != nil {
		log.Printf("Error getting MySQL dialect: %v", err)
		return
	}

	fmt.Printf("MySQL Version: %s\n", mysql.GetVersion())
	fmt.Printf("Supports JSON: %t\n", mysql.SupportsFeature(dialect.FeatureJSON))
	fmt.Printf("Supports Generated Columns: %t\n", mysql.SupportsFeature(dialect.FeatureGeneratedColumns))
	fmt.Printf("Supports Upsert: %t\n", mysql.SupportsFeature(dialect.FeatureUpsert))

	// Test JSON operations
	if jsonDialect, ok := mysql.(dialect.JSONDialect); ok {
		fmt.Println("\nJSON Operations:")
		fmt.Printf("  Create JSON column: %s\n", jsonDialect.CreateJSONColumn("data", "NOT NULL"))
		fmt.Printf("  Extract JSON: %s\n", jsonDialect.JSONExtract("data", "'$.name'"))
		fmt.Printf("  Set JSON: %s\n", jsonDialect.JSONSet("data", "'$.age'", "25"))
	}

	// Test Upsert operations
	if upsertDialect, ok := mysql.(dialect.UpsertDialect); ok {
		fmt.Println("\nUpsert Operations:")
		columns := []string{"id", "name", "email"}
		conflictCols := []string{"email"}
		updateCols := []string{"name"}
		fmt.Printf("  Upsert SQL: %s\n",
			upsertDialect.BuildUpsert("users", columns, conflictCols, updateCols))
	}

	// Test Generated Columns
	if genColDialect, ok := mysql.(dialect.GeneratedColumnDialect); ok {
		fmt.Println("\nGenerated Columns:")
		fmt.Printf("  Stored generated: %s\n",
			genColDialect.CreateStoredGeneratedColumn("full_name", "CONCAT(first_name, ' ', last_name)", "VARCHAR(255)"))
	}
}

func testPostgreSQLDialect() {
	postgres, err := dialect.GetExtendedDialect("postgresql")
	if err != nil {
		log.Printf("Error getting PostgreSQL dialect: %v", err)
		return
	}

	fmt.Printf("PostgreSQL Version: %s\n", postgres.GetVersion())
	fmt.Printf("Supports JSONB: %t\n", postgres.SupportsFeature(dialect.FeatureJSONB))
	fmt.Printf("Supports Arrays: %t\n", postgres.SupportsFeature(dialect.FeatureArrays))
	fmt.Printf("Supports Partitioning: %t\n", postgres.SupportsFeature(dialect.FeaturePartitioning))
	fmt.Printf("Supports Partial Indexes: %t\n", postgres.SupportsFeature(dialect.FeaturePartialIndex))

	// Test Array operations
	if arrayDialect, ok := postgres.(dialect.ArrayDialect); ok {
		fmt.Println("\nArray Operations:")
		fmt.Printf("  Create array column: %s\n", arrayDialect.CreateArrayColumn("INTEGER", 1))
		fmt.Printf("  Array append: %s\n", arrayDialect.ArrayAppend("tags", "'new_tag'"))
		fmt.Printf("  Array length: %s\n", arrayDialect.ArrayLength("tags", 1))
	}

	// Test Advanced Indexing
	if indexDialect, ok := postgres.(dialect.AdvancedIndexDialect); ok {
		fmt.Println("\nAdvanced Indexing:")
		fmt.Printf("  Partial index: %s\n",
			indexDialect.CreatePartialIndex("idx_active_users", "users", []string{"email"}, "active = true"))
		fmt.Printf("  Expression index: %s\n",
			indexDialect.CreateExpressionIndex("idx_lower_email", "users", "LOWER(email)"))
	}

	// Test Partitioning
	if partitionDialect, ok := postgres.(dialect.PartitioningDialect); ok {
		fmt.Println("\nPartitioning:")
		fmt.Printf("  Create partitioned table: %s\n",
			partitionDialect.CreatePartitionedTable("sales", dialect.PartitionTypeRange, "sale_date"))
		fmt.Printf("  Create partition: %s\n",
			partitionDialect.CreatePartition("sales", "sales_2024", "FROM ('2024-01-01') TO ('2025-01-01')"))
	}
}

func testSQLiteDialect() {
	sqlite, err := dialect.GetExtendedDialect("sqlite")
	if err != nil {
		log.Printf("Error getting SQLite dialect: %v", err)
		return
	}

	fmt.Printf("SQLite Version: %s\n", sqlite.GetVersion())
	fmt.Printf("Supports JSON: %t\n", sqlite.SupportsFeature(dialect.FeatureJSON))
	fmt.Printf("Supports FTS: %t\n", sqlite.SupportsFeature(dialect.FeatureFTS))
	fmt.Printf("Supports Generated Columns: %t\n", sqlite.SupportsFeature(dialect.FeatureGeneratedColumns))

	// Test SQLite-specific features
	if sqliteDialect, ok := sqlite.(*dialect.EnhancedSQLiteDialect); ok {
		fmt.Println("\nSQLite PRAGMA Optimizations:")

		// WAL optimization
		walPragmas := sqliteDialect.OptimizeForWAL()
		fmt.Println("  WAL Mode Optimization:")
		for _, pragma := range walPragmas {
			fmt.Printf("    %s\n", pragma)
		}

		// Concurrency optimization
		concurrencyPragmas := sqliteDialect.OptimizeForConcurrency()
		fmt.Println("  Concurrency Optimization:")
		for _, pragma := range concurrencyPragmas[:3] { // Show first 3
			fmt.Printf("    %s\n", pragma)
		}

		// FTS operations
		fmt.Println("\nFull-Text Search:")
		fmt.Printf("  Create FTS table: %s\n",
			sqliteDialect.CreateFTSTable("documents_fts", []string{"title", "content"}, "fts5"))
		fmt.Printf("  FTS query: %s\n",
			sqliteDialect.FTSQuery("documents_fts", "search terms"))
	}
}

func showFeatureMatrix() {
	matrix := dialect.GetFeatureMatrix()

	// Define features to display
	features := []dialect.Feature{
		dialect.FeatureJSON,
		dialect.FeatureJSONB,
		dialect.FeatureArrays,
		dialect.FeaturePartitioning,
		dialect.FeatureUpsert,
		dialect.FeatureGeneratedColumns,
		dialect.FeatureFTS,
		dialect.FeaturePartialIndex,
	}

	// Print header
	fmt.Printf("%-20s", "Feature")
	for dialectName := range matrix {
		fmt.Printf("%-12s", dialectName)
	}
	fmt.Println()

	// Print separator
	fmt.Printf("%-20s", strings.Repeat("-", 20))
	for range matrix {
		fmt.Printf("%-12s", strings.Repeat("-", 12))
	}
	fmt.Println()

	// Print feature matrix
	for _, feature := range features {
		fmt.Printf("%-20s", string(feature))
		for _, dialectFeatures := range matrix {
			supported := dialectFeatures[feature]
			mark := "✗"
			if supported {
				mark = "✓"
			}
			fmt.Printf("%-12s", mark)
		}
		fmt.Println()
	}
}

func demonstrateBenchmarking() {
	// This would require actual database connections in a real implementation
	fmt.Println("Benchmarking capabilities available for:")

	dialectNames := dialect.EnhancedRegistry.List()
	for _, name := range dialectNames {
		d, err := dialect.GetExtendedDialect(name)
		if err != nil {
			continue
		}

		fmt.Printf("  %s (%s)\n", name, d.GetVersion())

		// Show capabilities
		caps := d.GetCapabilities()
		fmt.Printf("    - Bulk Insert: %t\n", caps.SupportsBulkInsert)
		fmt.Printf("    - Batch Operations: %t\n", caps.SupportsBatchOperations)
		fmt.Printf("    - Query Plans: %t\n", caps.SupportsQueryPlan)
	}

	fmt.Println("\nBenchmark Example:")
	fmt.Println("  // Create benchmark instance")
	fmt.Println("  mysql, _ := dialect.GetExtendedDialect(\"mysql\")")
	fmt.Println("  benchmark := dialect.NewDialectBenchmark(mysql, db)")
	fmt.Println("  ")
	fmt.Println("  // Benchmark a query")
	fmt.Println("  metric, err := benchmark.BenchmarkQuery(\"SELECT * FROM users WHERE active = ?\", true)")
	fmt.Println("  fmt.Printf(\"Query took: %v\\n\", metric.QueryExecutionTime)")
}