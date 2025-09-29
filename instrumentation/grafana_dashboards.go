package instrumentation

import (
	"encoding/json"
	"fmt"
	"os"
)

// GrafanaDashboard represents a complete Grafana dashboard configuration
type GrafanaDashboard struct {
	ID              *int                   `json:"id,omitempty"`
	UID             string                 `json:"uid"`
	Title           string                 `json:"title"`
	Tags            []string               `json:"tags"`
	Style           string                 `json:"style"`
	Timezone        string                 `json:"timezone"`
	Editable        bool                   `json:"editable"`
	HideControls    bool                   `json:"hideControls"`
	GraphTooltip    int                    `json:"graphTooltip"`
	Time            TimeRange              `json:"time"`
	TimePicker      TimePicker             `json:"timepicker"`
	Templating      Templating             `json:"templating"`
	Annotations     Annotations            `json:"annotations"`
	Refresh         string                 `json:"refresh"`
	SchemaVersion   int                    `json:"schemaVersion"`
	Version         int                    `json:"version"`
	Panels          []Panel                `json:"panels"`
	Links           []DashboardLink        `json:"links"`
}

// TimeRange defines the dashboard time range
type TimeRange struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// TimePicker configuration
type TimePicker struct {
	RefreshIntervals []string `json:"refresh_intervals"`
	TimeOptions      []string `json:"time_options"`
}

// Panel represents a dashboard panel
type Panel struct {
	ID              int                    `json:"id"`
	Title           string                 `json:"title"`
	Type            string                 `json:"type"`
	GridPos         GridPos                `json:"gridPos"`
	Targets         []Target               `json:"targets"`
	XAxis           *Axis                  `json:"xAxis,omitempty"`
	YAxes           []YAxis                `json:"yAxes,omitempty"`
	Legend          *Legend                `json:"legend,omitempty"`
	Tooltip         *Tooltip               `json:"tooltip,omitempty"`
	FieldConfig     *FieldConfig           `json:"fieldConfig,omitempty"`
	Options         map[string]interface{} `json:"options,omitempty"`
	Transparent     bool                   `json:"transparent"`
	Datasource      *Datasource            `json:"datasource,omitempty"`
	ThresholdConfig *ThresholdConfig       `json:"thresholds,omitempty"`
}

// GridPos defines panel position and size
type GridPos struct {
	H int `json:"h"`
	W int `json:"w"`
	X int `json:"x"`
	Y int `json:"y"`
}

// Target defines a metric query
type Target struct {
	Expr         string            `json:"expr"`
	Interval     string            `json:"interval,omitempty"`
	LegendFormat string            `json:"legendFormat,omitempty"`
	RefID        string            `json:"refId"`
	Datasource   *Datasource       `json:"datasource,omitempty"`
	Format       string            `json:"format,omitempty"`
	Hide         bool              `json:"hide,omitempty"`
	Step         int               `json:"step,omitempty"`
	ExtraOptions map[string]string `json:",inline"`
}

// Datasource configuration
type Datasource struct {
	Type string `json:"type"`
	UID  string `json:"uid"`
	Name string `json:"name,omitempty"`
}

// Additional panel configuration types
type Axis struct {
	Show bool `json:"show"`
}

type YAxis struct {
	Label string  `json:"label"`
	Max   *string `json:"max,omitempty"`
	Min   *string `json:"min,omitempty"`
	Show  bool    `json:"show"`
}

type Legend struct {
	DisplayMode string   `json:"displayMode"`
	Placement   string   `json:"placement"`
	Values      []string `json:"values,omitempty"`
}

type Tooltip struct {
	Mode string `json:"mode"`
	Sort string `json:"sort"`
}

type FieldConfig struct {
	Defaults FieldDefaults `json:"defaults"`
}

type FieldDefaults struct {
	Unit   string     `json:"unit"`
	Min    *float64   `json:"min,omitempty"`
	Max    *float64   `json:"max,omitempty"`
	Custom FieldTheme `json:"custom"`
}

type FieldTheme struct {
	DrawStyle   string `json:"drawStyle"`
	LineWidth   int    `json:"lineWidth"`
	FillOpacity int    `json:"fillOpacity"`
}

type ThresholdConfig struct {
	Mode  string      `json:"mode"`
	Steps []Threshold `json:"steps"`
}

type Threshold struct {
	Color string   `json:"color"`
	Value *float64 `json:"value"`
}

// Templating for dashboard variables
type Templating struct {
	List []Template `json:"list"`
}

type Template struct {
	Name        string            `json:"name"`
	Type        string            `json:"type"`
	Label       string            `json:"label"`
	Query       string            `json:"query"`
	Refresh     int               `json:"refresh"`
	Options     []TemplateOption  `json:"options"`
	Current     TemplateOption    `json:"current"`
	Hide        int               `json:"hide"`
	IncludeAll  bool              `json:"includeAll"`
	Multi       bool              `json:"multi"`
	AllValue    string            `json:"allValue,omitempty"`
	Datasource  *Datasource       `json:"datasource,omitempty"`
	Definition  string            `json:"definition,omitempty"`
	TagsQuery   string            `json:"tagsQuery,omitempty"`
	TagValues   string            `json:"tagValuesQuery,omitempty"`
	Sort        int               `json:"sort"`
	Regex       string            `json:"regex,omitempty"`
	UseTags     bool              `json:"useTags,omitempty"`
	Extra       map[string]string `json:",inline"`
}

type TemplateOption struct {
	Selected bool   `json:"selected"`
	Text     string `json:"text"`
	Value    string `json:"value"`
}

// Annotations configuration
type Annotations struct {
	List []Annotation `json:"list"`
}

type Annotation struct {
	Name       string      `json:"name"`
	Datasource *Datasource `json:"datasource"`
	Enable     bool        `json:"enable"`
	Hide       bool        `json:"hide"`
	IconColor  string      `json:"iconColor"`
	Query      string      `json:"query"`
	ShowLine   bool        `json:"showLine"`
	Step       string      `json:"step"`
	TagKeys    string      `json:"tagKeys"`
	Tags       []string    `json:"tags"`
	TextFormat string      `json:"textFormat"`
	TitleFormat string     `json:"titleFormat"`
}

// DashboardLink for navigation
type DashboardLink struct {
	AsDropdown  bool     `json:"asDropdown"`
	Icon        string   `json:"icon"`
	IncludeVars bool     `json:"includeVars"`
	KeepTime    bool     `json:"keepTime"`
	Tags        []string `json:"tags"`
	TargetBlank bool     `json:"targetBlank"`
	Title       string   `json:"title"`
	Tooltip     string   `json:"tooltip"`
	Type        string   `json:"type"`
	URL         string   `json:"url"`
}

// DashboardGenerator creates Grafana dashboards for GORP metrics
type DashboardGenerator struct {
	prometheusUID string
	namespace     string
}

// NewDashboardGenerator creates a new dashboard generator
func NewDashboardGenerator(prometheusUID, namespace string) *DashboardGenerator {
	if namespace == "" {
		namespace = "gorp"
	}
	return &DashboardGenerator{
		prometheusUID: prometheusUID,
		namespace:     namespace,
	}
}

// GenerateOverviewDashboard creates the main GORP overview dashboard
func (g *DashboardGenerator) GenerateOverviewDashboard() *GrafanaDashboard {
	return &GrafanaDashboard{
		UID:           "gorp-overview",
		Title:         "GORP Database Overview",
		Tags:          []string{"gorp", "database", "orm"},
		Style:         "dark",
		Timezone:      "browser",
		Editable:      true,
		HideControls:  false,
		GraphTooltip:  1,
		SchemaVersion: 27,
		Version:       1,
		Time: TimeRange{
			From: "now-1h",
			To:   "now",
		},
		Refresh: "5s",
		TimePicker: TimePicker{
			RefreshIntervals: []string{"5s", "10s", "30s", "1m", "5m", "15m", "30m", "1h", "2h", "1d"},
			TimeOptions:      []string{"5m", "15m", "1h", "6h", "12h", "24h", "2d", "7d", "30d"},
		},
		Templating: g.generateTemplating(),
		Annotations: Annotations{
			List: []Annotation{
				{
					Name:        "Deployments",
					Datasource:  g.promDatasource(),
					Enable:      true,
					Hide:        false,
					IconColor:   "green",
					Query:       "increase(" + g.namespace + "_database_connections_opened_total[$__interval])",
					ShowLine:    true,
					TagKeys:     "deployment",
					TitleFormat: "Deployment",
					TextFormat:  "New deployment detected",
				},
			},
		},
		Panels: []Panel{
			g.createConnectionsPanel(1, 0, 0),
			g.createQueryRatePanel(2, 12, 0),
			g.createQueryLatencyPanel(3, 0, 9),
			g.createErrorRatePanel(4, 12, 9),
			g.createTransactionPanel(5, 0, 18),
			g.createDatabaseSizesPanel(6, 12, 18),
		},
		Links: []DashboardLink{
			{
				Icon:        "external link",
				Tags:        []string{"gorp"},
				TargetBlank: false,
				Title:       "GORP Performance Dashboard",
				Type:        "dashboards",
				URL:         "/d/gorp-performance/gorp-performance-details",
			},
		},
	}
}

// GeneratePerformanceDashboard creates detailed performance monitoring dashboard
func (g *DashboardGenerator) GeneratePerformanceDashboard() *GrafanaDashboard {
	return &GrafanaDashboard{
		UID:           "gorp-performance",
		Title:         "GORP Performance Details",
		Tags:          []string{"gorp", "performance", "database"},
		Style:         "dark",
		Timezone:      "browser",
		Editable:      true,
		HideControls:  false,
		GraphTooltip:  1,
		SchemaVersion: 27,
		Version:       1,
		Time: TimeRange{
			From: "now-6h",
			To:   "now",
		},
		Refresh: "10s",
		TimePicker: TimePicker{
			RefreshIntervals: []string{"5s", "10s", "30s", "1m", "5m", "15m", "30m", "1h", "2h", "1d"},
			TimeOptions:      []string{"5m", "15m", "1h", "6h", "12h", "24h", "2d", "7d", "30d"},
		},
		Templating: g.generateTemplating(),
		Panels: []Panel{
			g.createQueryLatencyHistogramPanel(1, 0, 0),
			g.createSlowQueriesPanel(2, 12, 0),
			g.createConnectionPoolPanel(3, 0, 9),
			g.createQueryCountByTypePanel(4, 8, 9),
			g.createCacheHitRatePanel(5, 16, 9),
			g.createDatabaseSpecificMetricsPanel(6, 0, 18),
		},
	}
}

// GenerateSecurityDashboard creates security monitoring dashboard
func (g *DashboardGenerator) GenerateSecurityDashboard() *GrafanaDashboard {
	return &GrafanaDashboard{
		UID:           "gorp-security",
		Title:         "GORP Security Monitoring",
		Tags:          []string{"gorp", "security", "audit"},
		Style:         "dark",
		Timezone:      "browser",
		Editable:      true,
		HideControls:  false,
		GraphTooltip:  1,
		SchemaVersion: 27,
		Version:       1,
		Time: TimeRange{
			From: "now-24h",
			To:   "now",
		},
		Refresh: "30s",
		TimePicker: TimePicker{
			RefreshIntervals: []string{"10s", "30s", "1m", "5m", "15m", "30m", "1h", "2h", "1d"},
			TimeOptions:      []string{"5m", "15m", "1h", "6h", "12h", "24h", "2d", "7d", "30d"},
		},
		Templating: g.generateTemplating(),
		Panels: []Panel{
			g.createSecurityViolationsPanel(1, 0, 0),
			g.createFailedAuthPanel(2, 12, 0),
			g.createAuditEventPanel(3, 0, 9),
			g.createSQLInjectionPanel(4, 12, 9),
			g.createConnectionSecurityPanel(5, 0, 18),
			g.createParameterSanitizationPanel(6, 12, 18),
		},
	}
}

// Helper methods for common panel creation

func (g *DashboardGenerator) promDatasource() *Datasource {
	return &Datasource{
		Type: "prometheus",
		UID:  g.prometheusUID,
	}
}

func (g *DashboardGenerator) generateTemplating() Templating {
	return Templating{
		List: []Template{
			{
				Name:     "instance",
				Type:     "query",
				Label:    "Instance",
				Query:    "label_values(" + g.namespace + "_database_connections_active, instance)",
				Refresh:  1,
				Current:  TemplateOption{Selected: true, Text: "All", Value: "$__all"},
				Hide:     0,
				IncludeAll: true,
				Multi:    true,
				AllValue: ".*",
				Datasource: g.promDatasource(),
				Sort:     1,
			},
			{
				Name:     "database",
				Type:     "query",
				Label:    "Database",
				Query:    "label_values(" + g.namespace + "_database_connections_active{instance=~\"$instance\"}, database)",
				Refresh:  1,
				Current:  TemplateOption{Selected: true, Text: "All", Value: "$__all"},
				Hide:     0,
				IncludeAll: true,
				Multi:    true,
				AllValue: ".*",
				Datasource: g.promDatasource(),
				Sort:     1,
			},
		},
	}
}

func (g *DashboardGenerator) createConnectionsPanel(id, x, y int) Panel {
	return Panel{
		ID:    id,
		Title: "Database Connections",
		Type:  "timeseries",
		GridPos: GridPos{H: 9, W: 12, X: x, Y: y},
		Targets: []Target{
			{
				Expr:         g.namespace + "_database_connections_active{instance=~\"$instance\",database=~\"$database\"}",
				LegendFormat: "Active - {{instance}} {{database}}",
				RefID:        "A",
				Datasource:   g.promDatasource(),
			},
			{
				Expr:         g.namespace + "_database_connections_idle{instance=~\"$instance\",database=~\"$database\"}",
				LegendFormat: "Idle - {{instance}} {{database}}",
				RefID:        "B",
				Datasource:   g.promDatasource(),
			},
			{
				Expr:         g.namespace + "_database_connections_open{instance=~\"$instance\",database=~\"$database\"}",
				LegendFormat: "Total - {{instance}} {{database}}",
				RefID:        "C",
				Datasource:   g.promDatasource(),
			},
		},
		FieldConfig: &FieldConfig{
			Defaults: FieldDefaults{
				Unit: "short",
				Custom: FieldTheme{
					DrawStyle:   "line",
					LineWidth:   2,
					FillOpacity: 10,
				},
			},
		},
		Options: map[string]interface{}{
			"tooltip": map[string]string{"mode": "multi", "sort": "desc"},
			"legend":  map[string]interface{}{"displayMode": "list", "placement": "bottom"},
		},
	}
}

func (g *DashboardGenerator) createQueryRatePanel(id, x, y int) Panel {
	return Panel{
		ID:    id,
		Title: "Query Rate",
		Type:  "timeseries",
		GridPos: GridPos{H: 9, W: 12, X: x, Y: y},
		Targets: []Target{
			{
				Expr:         "rate(" + g.namespace + "_database_queries_total{instance=~\"$instance\",database=~\"$database\"}[5m])",
				LegendFormat: "{{instance}} {{database}} {{query_type}}",
				RefID:        "A",
				Datasource:   g.promDatasource(),
			},
		},
		FieldConfig: &FieldConfig{
			Defaults: FieldDefaults{
				Unit: "qps",
				Custom: FieldTheme{
					DrawStyle:   "line",
					LineWidth:   2,
					FillOpacity: 10,
				},
			},
		},
		Options: map[string]interface{}{
			"tooltip": map[string]string{"mode": "multi", "sort": "desc"},
			"legend":  map[string]interface{}{"displayMode": "list", "placement": "bottom"},
		},
	}
}

func (g *DashboardGenerator) createQueryLatencyPanel(id, x, y int) Panel {
	return Panel{
		ID:    id,
		Title: "Query Latency",
		Type:  "timeseries",
		GridPos: GridPos{H: 9, W: 12, X: x, Y: y},
		Targets: []Target{
			{
				Expr:         "histogram_quantile(0.95, rate(" + g.namespace + "_database_query_duration_seconds_bucket{instance=~\"$instance\",database=~\"$database\"}[5m]))",
				LegendFormat: "95th percentile - {{instance}} {{database}}",
				RefID:        "A",
				Datasource:   g.promDatasource(),
			},
			{
				Expr:         "histogram_quantile(0.50, rate(" + g.namespace + "_database_query_duration_seconds_bucket{instance=~\"$instance\",database=~\"$database\"}[5m]))",
				LegendFormat: "50th percentile - {{instance}} {{database}}",
				RefID:        "B",
				Datasource:   g.promDatasource(),
			},
			{
				Expr:         "rate(" + g.namespace + "_database_query_duration_seconds_sum{instance=~\"$instance\",database=~\"$database\"}[5m]) / rate(" + g.namespace + "_database_query_duration_seconds_count{instance=~\"$instance\",database=~\"$database\"}[5m])",
				LegendFormat: "Average - {{instance}} {{database}}",
				RefID:        "C",
				Datasource:   g.promDatasource(),
			},
		},
		FieldConfig: &FieldConfig{
			Defaults: FieldDefaults{
				Unit: "s",
				Custom: FieldTheme{
					DrawStyle:   "line",
					LineWidth:   2,
					FillOpacity: 10,
				},
			},
		},
		Options: map[string]interface{}{
			"tooltip": map[string]string{"mode": "multi", "sort": "desc"},
			"legend":  map[string]interface{}{"displayMode": "list", "placement": "bottom"},
		},
	}
}

func (g *DashboardGenerator) createErrorRatePanel(id, x, y int) Panel {
	return Panel{
		ID:    id,
		Title: "Error Rate",
		Type:  "timeseries",
		GridPos: GridPos{H: 9, W: 12, X: x, Y: y},
		Targets: []Target{
			{
				Expr:         "rate(" + g.namespace + "_database_errors_total{instance=~\"$instance\",database=~\"$database\"}[5m])",
				LegendFormat: "{{instance}} {{database}} {{error_type}}",
				RefID:        "A",
				Datasource:   g.promDatasource(),
			},
		},
		FieldConfig: &FieldConfig{
			Defaults: FieldDefaults{
				Unit: "short",
				Custom: FieldTheme{
					DrawStyle:   "line",
					LineWidth:   2,
					FillOpacity: 10,
				},
			},
		},
		ThresholdConfig: &ThresholdConfig{
			Mode: "absolute",
			Steps: []Threshold{
				{Color: "green", Value: nil},
				{Color: "yellow", Value: func() *float64 { v := 0.1; return &v }()},
				{Color: "red", Value: func() *float64 { v := 1.0; return &v }()},
			},
		},
		Options: map[string]interface{}{
			"tooltip": map[string]string{"mode": "multi", "sort": "desc"},
			"legend":  map[string]interface{}{"displayMode": "list", "placement": "bottom"},
		},
	}
}

func (g *DashboardGenerator) createTransactionPanel(id, x, y int) Panel {
	return Panel{
		ID:    id,
		Title: "Transactions",
		Type:  "timeseries",
		GridPos: GridPos{H: 9, W: 12, X: x, Y: y},
		Targets: []Target{
			{
				Expr:         "rate(" + g.namespace + "_database_transactions_total{instance=~\"$instance\",database=~\"$database\",status=\"committed\"}[5m])",
				LegendFormat: "Committed - {{instance}} {{database}}",
				RefID:        "A",
				Datasource:   g.promDatasource(),
			},
			{
				Expr:         "rate(" + g.namespace + "_database_transactions_total{instance=~\"$instance\",database=~\"$database\",status=\"rolled_back\"}[5m])",
				LegendFormat: "Rolled Back - {{instance}} {{database}}",
				RefID:        "B",
				Datasource:   g.promDatasource(),
			},
		},
		FieldConfig: &FieldConfig{
			Defaults: FieldDefaults{
				Unit: "short",
				Custom: FieldTheme{
					DrawStyle:   "line",
					LineWidth:   2,
					FillOpacity: 10,
				},
			},
		},
		Options: map[string]interface{}{
			"tooltip": map[string]string{"mode": "multi", "sort": "desc"},
			"legend":  map[string]interface{}{"displayMode": "list", "placement": "bottom"},
		},
	}
}

func (g *DashboardGenerator) createDatabaseSizesPanel(id, x, y int) Panel {
	return Panel{
		ID:    id,
		Title: "Database Sizes",
		Type:  "stat",
		GridPos: GridPos{H: 9, W: 12, X: x, Y: y},
		Targets: []Target{
			{
				Expr:         g.namespace + "_database_size_bytes{instance=~\"$instance\",database=~\"$database\"}",
				LegendFormat: "{{instance}} {{database}}",
				RefID:        "A",
				Datasource:   g.promDatasource(),
			},
		},
		FieldConfig: &FieldConfig{
			Defaults: FieldDefaults{
				Unit: "bytes",
			},
		},
		Options: map[string]interface{}{
			"reduceOptions": map[string]interface{}{
				"values": false,
				"calcs":  []string{"lastNotNull"},
			},
			"orientation": "auto",
			"textMode":    "auto",
			"colorMode":   "value",
			"graphMode":   "area",
		},
	}
}

// Performance dashboard specific panels

func (g *DashboardGenerator) createQueryLatencyHistogramPanel(id, x, y int) Panel {
	return Panel{
		ID:    id,
		Title: "Query Latency Distribution",
		Type:  "heatmap",
		GridPos: GridPos{H: 9, W: 24, X: x, Y: y},
		Targets: []Target{
			{
				Expr:         "increase(" + g.namespace + "_database_query_duration_seconds_bucket{instance=~\"$instance\",database=~\"$database\"}[5m])",
				LegendFormat: "{{le}}",
				RefID:        "A",
				Datasource:   g.promDatasource(),
				Format:       "heatmap",
			},
		},
		FieldConfig: &FieldConfig{
			Defaults: FieldDefaults{
				Unit: "s",
				Custom: FieldTheme{
					DrawStyle:   "line",
					LineWidth:   1,
					FillOpacity: 80,
				},
			},
		},
		Options: map[string]interface{}{
			"calculate": true,
			"yAxis": map[string]interface{}{
				"unit": "s",
			},
		},
	}
}

func (g *DashboardGenerator) createSlowQueriesPanel(id, x, y int) Panel {
	return Panel{
		ID:    id,
		Title: "Slow Queries (>1s)",
		Type:  "stat",
		GridPos: GridPos{H: 9, W: 12, X: x, Y: y},
		Targets: []Target{
			{
				Expr:         "increase(" + g.namespace + "_database_queries_slow_total{instance=~\"$instance\",database=~\"$database\"}[5m])",
				LegendFormat: "{{instance}} {{database}}",
				RefID:        "A",
				Datasource:   g.promDatasource(),
			},
		},
		FieldConfig: &FieldConfig{
			Defaults: FieldDefaults{
				Unit: "short",
			},
		},
		ThresholdConfig: &ThresholdConfig{
			Mode: "absolute",
			Steps: []Threshold{
				{Color: "green", Value: nil},
				{Color: "yellow", Value: func() *float64 { v := 1.0; return &v }()},
				{Color: "red", Value: func() *float64 { v := 10.0; return &v }()},
			},
		},
		Options: map[string]interface{}{
			"reduceOptions": map[string]interface{}{
				"values": false,
				"calcs":  []string{"lastNotNull"},
			},
			"orientation": "auto",
			"textMode":    "auto",
			"colorMode":   "background",
		},
	}
}

func (g *DashboardGenerator) createConnectionPoolPanel(id, x, y int) Panel {
	return Panel{
		ID:    id,
		Title: "Connection Pool Status",
		Type:  "timeseries",
		GridPos: GridPos{H: 9, W: 8, X: x, Y: y},
		Targets: []Target{
			{
				Expr:         g.namespace + "_database_connection_pool_max{instance=~\"$instance\",database=~\"$database\"}",
				LegendFormat: "Max - {{instance}} {{database}}",
				RefID:        "A",
				Datasource:   g.promDatasource(),
			},
			{
				Expr:         g.namespace + "_database_connection_pool_in_use{instance=~\"$instance\",database=~\"$database\"}",
				LegendFormat: "In Use - {{instance}} {{database}}",
				RefID:        "B",
				Datasource:   g.promDatasource(),
			},
			{
				Expr:         g.namespace + "_database_connection_pool_idle{instance=~\"$instance\",database=~\"$database\"}",
				LegendFormat: "Idle - {{instance}} {{database}}",
				RefID:        "C",
				Datasource:   g.promDatasource(),
			},
		},
		FieldConfig: &FieldConfig{
			Defaults: FieldDefaults{
				Unit: "short",
				Custom: FieldTheme{
					DrawStyle:   "line",
					LineWidth:   2,
					FillOpacity: 10,
				},
			},
		},
	}
}

func (g *DashboardGenerator) createQueryCountByTypePanel(id, x, y int) Panel {
	return Panel{
		ID:    id,
		Title: "Query Count by Type",
		Type:  "piechart",
		GridPos: GridPos{H: 9, W: 8, X: x, Y: y},
		Targets: []Target{
			{
				Expr:         "increase(" + g.namespace + "_database_queries_total{instance=~\"$instance\",database=~\"$database\"}[1h])",
				LegendFormat: "{{query_type}}",
				RefID:        "A",
				Datasource:   g.promDatasource(),
			},
		},
		FieldConfig: &FieldConfig{
			Defaults: FieldDefaults{
				Unit: "short",
			},
		},
		Options: map[string]interface{}{
			"reduceOptions": map[string]interface{}{
				"values": false,
				"calcs":  []string{"lastNotNull"},
			},
			"pieType":       "pie",
			"displayLabels": []string{"name", "value"},
		},
	}
}

func (g *DashboardGenerator) createCacheHitRatePanel(id, x, y int) Panel {
	return Panel{
		ID:    id,
		Title: "Cache Hit Rate",
		Type:  "stat",
		GridPos: GridPos{H: 9, W: 8, X: x, Y: y},
		Targets: []Target{
			{
				Expr:         "rate(" + g.namespace + "_database_cache_hits_total{instance=~\"$instance\",database=~\"$database\"}[5m]) / (rate(" + g.namespace + "_database_cache_hits_total{instance=~\"$instance\",database=~\"$database\"}[5m]) + rate(" + g.namespace + "_database_cache_misses_total{instance=~\"$instance\",database=~\"$database\"}[5m]))",
				LegendFormat: "{{instance}} {{database}}",
				RefID:        "A",
				Datasource:   g.promDatasource(),
			},
		},
		FieldConfig: &FieldConfig{
			Defaults: FieldDefaults{
				Unit: "percentunit",
				Min:  func() *float64 { v := 0.0; return &v }(),
				Max:  func() *float64 { v := 1.0; return &v }(),
			},
		},
		ThresholdConfig: &ThresholdConfig{
			Mode: "absolute",
			Steps: []Threshold{
				{Color: "red", Value: nil},
				{Color: "yellow", Value: func() *float64 { v := 0.8; return &v }()},
				{Color: "green", Value: func() *float64 { v := 0.95; return &v }()},
			},
		},
		Options: map[string]interface{}{
			"orientation": "auto",
			"textMode":    "auto",
			"colorMode":   "background",
			"graphMode":   "area",
		},
	}
}

func (g *DashboardGenerator) createDatabaseSpecificMetricsPanel(id, x, y int) Panel {
	return Panel{
		ID:    id,
		Title: "Database-Specific Metrics",
		Type:  "timeseries",
		GridPos: GridPos{H: 9, W: 24, X: x, Y: y},
		Targets: []Target{
			{
				Expr:         g.namespace + "_postgresql_buffer_cache_hit_ratio{instance=~\"$instance\",database=~\"$database\"}",
				LegendFormat: "PostgreSQL Buffer Cache Hit Ratio - {{instance}} {{database}}",
				RefID:        "A",
				Datasource:   g.promDatasource(),
			},
			{
				Expr:         g.namespace + "_mysql_innodb_buffer_pool_reads{instance=~\"$instance\",database=~\"$database\"}",
				LegendFormat: "MySQL InnoDB Buffer Pool Reads - {{instance}} {{database}}",
				RefID:        "B",
				Datasource:   g.promDatasource(),
			},
			{
				Expr:         g.namespace + "_sqlite_page_cache_hits{instance=~\"$instance\",database=~\"$database\"}",
				LegendFormat: "SQLite Page Cache Hits - {{instance}} {{database}}",
				RefID:        "C",
				Datasource:   g.promDatasource(),
			},
		},
		FieldConfig: &FieldConfig{
			Defaults: FieldDefaults{
				Unit: "short",
				Custom: FieldTheme{
					DrawStyle:   "line",
					LineWidth:   2,
					FillOpacity: 10,
				},
			},
		},
	}
}

// Security dashboard specific panels

func (g *DashboardGenerator) createSecurityViolationsPanel(id, x, y int) Panel {
	return Panel{
		ID:    id,
		Title: "Security Violations",
		Type:  "timeseries",
		GridPos: GridPos{H: 9, W: 12, X: x, Y: y},
		Targets: []Target{
			{
				Expr:         "rate(" + g.namespace + "_security_violations_total{instance=~\"$instance\",database=~\"$database\"}[5m])",
				LegendFormat: "{{instance}} {{database}} {{violation_type}}",
				RefID:        "A",
				Datasource:   g.promDatasource(),
			},
		},
		FieldConfig: &FieldConfig{
			Defaults: FieldDefaults{
				Unit: "short",
				Custom: FieldTheme{
					DrawStyle:   "line",
					LineWidth:   2,
					FillOpacity: 10,
				},
			},
		},
		ThresholdConfig: &ThresholdConfig{
			Mode: "absolute",
			Steps: []Threshold{
				{Color: "green", Value: nil},
				{Color: "red", Value: func() *float64 { v := 0.1; return &v }()},
			},
		},
	}
}

func (g *DashboardGenerator) createFailedAuthPanel(id, x, y int) Panel {
	return Panel{
		ID:    id,
		Title: "Failed Authentication Attempts",
		Type:  "stat",
		GridPos: GridPos{H: 9, W: 12, X: x, Y: y},
		Targets: []Target{
			{
				Expr:         "increase(" + g.namespace + "_security_auth_failed_total{instance=~\"$instance\",database=~\"$database\"}[1h])",
				LegendFormat: "{{instance}} {{database}}",
				RefID:        "A",
				Datasource:   g.promDatasource(),
			},
		},
		FieldConfig: &FieldConfig{
			Defaults: FieldDefaults{
				Unit: "short",
			},
		},
		ThresholdConfig: &ThresholdConfig{
			Mode: "absolute",
			Steps: []Threshold{
				{Color: "green", Value: nil},
				{Color: "yellow", Value: func() *float64 { v := 5.0; return &v }()},
				{Color: "red", Value: func() *float64 { v := 20.0; return &v }()},
			},
		},
		Options: map[string]interface{}{
			"colorMode": "background",
		},
	}
}

func (g *DashboardGenerator) createAuditEventPanel(id, x, y int) Panel {
	return Panel{
		ID:    id,
		Title: "Audit Events",
		Type:  "timeseries",
		GridPos: GridPos{H: 9, W: 12, X: x, Y: y},
		Targets: []Target{
			{
				Expr:         "rate(" + g.namespace + "_security_audit_events_total{instance=~\"$instance\",database=~\"$database\"}[5m])",
				LegendFormat: "{{instance}} {{database}} {{event_type}}",
				RefID:        "A",
				Datasource:   g.promDatasource(),
			},
		},
		FieldConfig: &FieldConfig{
			Defaults: FieldDefaults{
				Unit: "short",
				Custom: FieldTheme{
					DrawStyle:   "line",
					LineWidth:   2,
					FillOpacity: 10,
				},
			},
		},
	}
}

func (g *DashboardGenerator) createSQLInjectionPanel(id, x, y int) Panel {
	return Panel{
		ID:    id,
		Title: "SQL Injection Attempts",
		Type:  "stat",
		GridPos: GridPos{H: 9, W: 12, X: x, Y: y},
		Targets: []Target{
			{
				Expr:         "increase(" + g.namespace + "_security_sql_injection_blocked_total{instance=~\"$instance\",database=~\"$database\"}[1h])",
				LegendFormat: "{{instance}} {{database}}",
				RefID:        "A",
				Datasource:   g.promDatasource(),
			},
		},
		FieldConfig: &FieldConfig{
			Defaults: FieldDefaults{
				Unit: "short",
			},
		},
		ThresholdConfig: &ThresholdConfig{
			Mode: "absolute",
			Steps: []Threshold{
				{Color: "green", Value: nil},
				{Color: "red", Value: func() *float64 { v := 1.0; return &v }()},
			},
		},
		Options: map[string]interface{}{
			"colorMode": "background",
		},
	}
}

func (g *DashboardGenerator) createConnectionSecurityPanel(id, x, y int) Panel {
	return Panel{
		ID:    id,
		Title: "Connection Security Events",
		Type:  "timeseries",
		GridPos: GridPos{H: 9, W: 12, X: x, Y: y},
		Targets: []Target{
			{
				Expr:         "rate(" + g.namespace + "_security_connection_events_total{instance=~\"$instance\",database=~\"$database\"}[5m])",
				LegendFormat: "{{instance}} {{database}} {{event_type}}",
				RefID:        "A",
				Datasource:   g.promDatasource(),
			},
		},
		FieldConfig: &FieldConfig{
			Defaults: FieldDefaults{
				Unit: "short",
				Custom: FieldTheme{
					DrawStyle:   "line",
					LineWidth:   2,
					FillOpacity: 10,
				},
			},
		},
	}
}

func (g *DashboardGenerator) createParameterSanitizationPanel(id, x, y int) Panel {
	return Panel{
		ID:    id,
		Title: "Parameter Sanitization Events",
		Type:  "timeseries",
		GridPos: GridPos{H: 9, W: 12, X: x, Y: y},
		Targets: []Target{
			{
				Expr:         "rate(" + g.namespace + "_security_sanitization_total{instance=~\"$instance\",database=~\"$database\"}[5m])",
				LegendFormat: "{{instance}} {{database}} {{sanitization_type}}",
				RefID:        "A",
				Datasource:   g.promDatasource(),
			},
		},
		FieldConfig: &FieldConfig{
			Defaults: FieldDefaults{
				Unit: "short",
				Custom: FieldTheme{
					DrawStyle:   "line",
					LineWidth:   2,
					FillOpacity: 10,
				},
			},
		},
	}
}

// ExportToJSON exports a dashboard to JSON format
func (d *GrafanaDashboard) ExportToJSON() ([]byte, error) {
	return json.MarshalIndent(d, "", "  ")
}

// SaveToFile saves the dashboard JSON to a file
func (d *GrafanaDashboard) SaveToFile(filename string) error {
	data, err := d.ExportToJSON()
	if err != nil {
		return fmt.Errorf("failed to marshal dashboard: %w", err)
	}

	return writeFile(filename, data, 0644)
}

// GenerateAllDashboards creates all standard GORP dashboards
func (g *DashboardGenerator) GenerateAllDashboards() map[string]*GrafanaDashboard {
	return map[string]*GrafanaDashboard{
		"overview":    g.GenerateOverviewDashboard(),
		"performance": g.GeneratePerformanceDashboard(),
		"security":    g.GenerateSecurityDashboard(),
	}
}

// Helper function to write file
func writeFile(filename string, data []byte, perm uint32) error {
	return os.WriteFile(filename, data, os.FileMode(perm))
}