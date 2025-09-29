package instrumentation

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDashboardGenerator(t *testing.T) {
	generator := NewDashboardGenerator("prometheus-uid", "gorp")

	t.Run("NewDashboardGenerator", func(t *testing.T) {
		if generator.prometheusUID != "prometheus-uid" {
			t.Errorf("Expected prometheusUID to be 'prometheus-uid', got '%s'", generator.prometheusUID)
		}
		if generator.namespace != "gorp" {
			t.Errorf("Expected namespace to be 'gorp', got '%s'", generator.namespace)
		}
	})

	t.Run("NewDashboardGenerator with empty namespace", func(t *testing.T) {
		gen := NewDashboardGenerator("test-uid", "")
		if gen.namespace != "gorp" {
			t.Errorf("Expected default namespace to be 'gorp', got '%s'", gen.namespace)
		}
	})
}

func TestGenerateOverviewDashboard(t *testing.T) {
	generator := NewDashboardGenerator("prometheus-uid", "test")
	dashboard := generator.GenerateOverviewDashboard()

	t.Run("Basic properties", func(t *testing.T) {
		if dashboard.UID != "gorp-overview" {
			t.Errorf("Expected UID to be 'gorp-overview', got '%s'", dashboard.UID)
		}
		if dashboard.Title != "GORP Database Overview" {
			t.Errorf("Expected title to be 'GORP Database Overview', got '%s'", dashboard.Title)
		}
		if len(dashboard.Tags) == 0 {
			t.Error("Expected dashboard to have tags")
		}
	})

	t.Run("Time range", func(t *testing.T) {
		if dashboard.Time.From != "now-1h" {
			t.Errorf("Expected time from to be 'now-1h', got '%s'", dashboard.Time.From)
		}
		if dashboard.Time.To != "now" {
			t.Errorf("Expected time to to be 'now', got '%s'", dashboard.Time.To)
		}
	})

	t.Run("Panels", func(t *testing.T) {
		if len(dashboard.Panels) < 6 {
			t.Errorf("Expected at least 6 panels, got %d", len(dashboard.Panels))
		}

		// Test specific panels exist
		panelTitles := make(map[string]bool)
		for _, panel := range dashboard.Panels {
			panelTitles[panel.Title] = true
		}

		expectedPanels := []string{
			"Database Connections",
			"Query Rate",
			"Query Latency",
			"Error Rate",
			"Transactions",
			"Database Sizes",
		}

		for _, expected := range expectedPanels {
			if !panelTitles[expected] {
				t.Errorf("Expected panel '%s' not found", expected)
			}
		}
	})

	t.Run("Templating", func(t *testing.T) {
		if len(dashboard.Templating.List) < 2 {
			t.Errorf("Expected at least 2 template variables, got %d", len(dashboard.Templating.List))
		}

		// Check for instance and database templates
		templateNames := make(map[string]bool)
		for _, template := range dashboard.Templating.List {
			templateNames[template.Name] = true
		}

		if !templateNames["instance"] {
			t.Error("Expected 'instance' template variable")
		}
		if !templateNames["database"] {
			t.Error("Expected 'database' template variable")
		}
	})
}

func TestGeneratePerformanceDashboard(t *testing.T) {
	generator := NewDashboardGenerator("prometheus-uid", "test")
	dashboard := generator.GeneratePerformanceDashboard()

	t.Run("Basic properties", func(t *testing.T) {
		if dashboard.UID != "gorp-performance" {
			t.Errorf("Expected UID to be 'gorp-performance', got '%s'", dashboard.UID)
		}
		if dashboard.Title != "GORP Performance Details" {
			t.Errorf("Expected title to be 'GORP Performance Details', got '%s'", dashboard.Title)
		}
	})

	t.Run("Performance specific panels", func(t *testing.T) {
		panelTitles := make(map[string]bool)
		for _, panel := range dashboard.Panels {
			panelTitles[panel.Title] = true
		}

		expectedPanels := []string{
			"Query Latency Distribution",
			"Slow Queries (>1s)",
			"Connection Pool Status",
			"Query Count by Type",
			"Cache Hit Rate",
			"Database-Specific Metrics",
		}

		for _, expected := range expectedPanels {
			if !panelTitles[expected] {
				t.Errorf("Expected panel '%s' not found", expected)
			}
		}
	})
}

func TestGenerateSecurityDashboard(t *testing.T) {
	generator := NewDashboardGenerator("prometheus-uid", "test")
	dashboard := generator.GenerateSecurityDashboard()

	t.Run("Basic properties", func(t *testing.T) {
		if dashboard.UID != "gorp-security" {
			t.Errorf("Expected UID to be 'gorp-security', got '%s'", dashboard.UID)
		}
		if dashboard.Title != "GORP Security Monitoring" {
			t.Errorf("Expected title to be 'GORP Security Monitoring', got '%s'", dashboard.Title)
		}
	})

	t.Run("Security specific panels", func(t *testing.T) {
		panelTitles := make(map[string]bool)
		for _, panel := range dashboard.Panels {
			panelTitles[panel.Title] = true
		}

		expectedPanels := []string{
			"Security Violations",
			"Failed Authentication Attempts",
			"Audit Events",
			"SQL Injection Attempts",
			"Connection Security Events",
			"Parameter Sanitization Events",
		}

		for _, expected := range expectedPanels {
			if !panelTitles[expected] {
				t.Errorf("Expected panel '%s' not found", expected)
			}
		}
	})
}

func TestDashboardJSONExport(t *testing.T) {
	generator := NewDashboardGenerator("prometheus-uid", "test")
	dashboard := generator.GenerateOverviewDashboard()

	t.Run("ExportToJSON", func(t *testing.T) {
		data, err := dashboard.ExportToJSON()
		if err != nil {
			t.Fatalf("Failed to export dashboard to JSON: %v", err)
		}

		// Verify it's valid JSON
		var result map[string]interface{}
		if err := json.Unmarshal(data, &result); err != nil {
			t.Fatalf("Exported JSON is invalid: %v", err)
		}

		// Check for key fields
		if result["uid"] != dashboard.UID {
			t.Errorf("JSON UID mismatch: expected '%s', got '%s'", dashboard.UID, result["uid"])
		}
		if result["title"] != dashboard.Title {
			t.Errorf("JSON title mismatch: expected '%s', got '%s'", dashboard.Title, result["title"])
		}
	})

	t.Run("SaveToFile", func(t *testing.T) {
		tmpDir, err := os.MkdirTemp("", "gorp-test")
		if err != nil {
			t.Fatalf("Failed to create temp dir: %v", err)
		}
		defer os.RemoveAll(tmpDir)

		filename := filepath.Join(tmpDir, "test-dashboard.json")
		if err := dashboard.SaveToFile(filename); err != nil {
			t.Fatalf("Failed to save dashboard to file: %v", err)
		}

		// Verify file exists and contains valid JSON
		data, err := os.ReadFile(filename)
		if err != nil {
			t.Fatalf("Failed to read saved file: %v", err)
		}

		var result map[string]interface{}
		if err := json.Unmarshal(data, &result); err != nil {
			t.Fatalf("Saved JSON is invalid: %v", err)
		}
	})
}

func TestGenerateAllDashboards(t *testing.T) {
	generator := NewDashboardGenerator("prometheus-uid", "test")
	dashboards := generator.GenerateAllDashboards()

	t.Run("All dashboards generated", func(t *testing.T) {
		expected := []string{"overview", "performance", "security"}
		if len(dashboards) != len(expected) {
			t.Errorf("Expected %d dashboards, got %d", len(expected), len(dashboards))
		}

		for _, name := range expected {
			if _, exists := dashboards[name]; !exists {
				t.Errorf("Expected dashboard '%s' not found", name)
			}
		}
	})

	t.Run("Dashboard uniqueness", func(t *testing.T) {
		uids := make(map[string]bool)
		for _, dashboard := range dashboards {
			if uids[dashboard.UID] {
				t.Errorf("Duplicate UID found: %s", dashboard.UID)
			}
			uids[dashboard.UID] = true
		}
	})
}

func TestPanelConfiguration(t *testing.T) {
	generator := NewDashboardGenerator("prometheus-uid", "test")

	t.Run("Connection panel", func(t *testing.T) {
		panel := generator.createConnectionsPanel(1, 0, 0)

		if panel.Type != "timeseries" {
			t.Errorf("Expected panel type 'timeseries', got '%s'", panel.Type)
		}
		if len(panel.Targets) < 3 {
			t.Errorf("Expected at least 3 targets for connections panel, got %d", len(panel.Targets))
		}

		// Check prometheus queries contain namespace
		for _, target := range panel.Targets {
			if !strings.Contains(target.Expr, "test_database_connections") {
				t.Errorf("Expected target expression to contain namespace 'test', got '%s'", target.Expr)
			}
		}
	})

	t.Run("Query rate panel", func(t *testing.T) {
		panel := generator.createQueryRatePanel(2, 12, 0)

		if panel.FieldConfig.Defaults.Unit != "qps" {
			t.Errorf("Expected unit 'qps', got '%s'", panel.FieldConfig.Defaults.Unit)
		}
	})

	t.Run("Error rate panel with thresholds", func(t *testing.T) {
		panel := generator.createErrorRatePanel(4, 12, 9)

		if panel.ThresholdConfig == nil {
			t.Error("Expected threshold configuration for error rate panel")
		} else {
			if len(panel.ThresholdConfig.Steps) < 3 {
				t.Errorf("Expected at least 3 threshold steps, got %d", len(panel.ThresholdConfig.Steps))
			}
		}
	})
}

func TestTemplateGeneration(t *testing.T) {
	generator := NewDashboardGenerator("prometheus-uid", "test")
	templating := generator.generateTemplating()

	t.Run("Template structure", func(t *testing.T) {
		if len(templating.List) != 2 {
			t.Errorf("Expected exactly 2 templates, got %d", len(templating.List))
		}

		for _, template := range templating.List {
			if template.Datasource == nil {
				t.Errorf("Template '%s' missing datasource", template.Name)
			}
			if template.Query == "" {
				t.Errorf("Template '%s' missing query", template.Name)
			}
		}
	})

	t.Run("Template queries", func(t *testing.T) {
		for _, template := range templating.List {
			if !strings.Contains(template.Query, "test_database_connections") {
				t.Errorf("Template query should contain namespace 'test', got '%s'", template.Query)
			}
		}
	})
}

func TestDatasourceConfiguration(t *testing.T) {
	generator := NewDashboardGenerator("custom-prometheus-uid", "test")
	datasource := generator.promDatasource()

	t.Run("Datasource properties", func(t *testing.T) {
		if datasource.Type != "prometheus" {
			t.Errorf("Expected datasource type 'prometheus', got '%s'", datasource.Type)
		}
		if datasource.UID != "custom-prometheus-uid" {
			t.Errorf("Expected datasource UID 'custom-prometheus-uid', got '%s'", datasource.UID)
		}
	})
}

// Benchmark tests for dashboard generation
func BenchmarkGenerateOverviewDashboard(b *testing.B) {
	generator := NewDashboardGenerator("prometheus-uid", "test")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = generator.GenerateOverviewDashboard()
	}
}

func BenchmarkGenerateAllDashboards(b *testing.B) {
	generator := NewDashboardGenerator("prometheus-uid", "test")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = generator.GenerateAllDashboards()
	}
}

func BenchmarkDashboardJSONExport(b *testing.B) {
	generator := NewDashboardGenerator("prometheus-uid", "test")
	dashboard := generator.GenerateOverviewDashboard()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = dashboard.ExportToJSON()
	}
}