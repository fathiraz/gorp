package instrumentation

import (
	"context"
	"encoding/json"
	"sort"
	"sync"
	"time"
)

// AggregationWindow defines the time window for aggregation
type AggregationWindow struct {
	Duration time.Duration `json:"duration"`
	Size     int           `json:"size"`     // number of windows to keep
	Interval time.Duration `json:"interval"` // how often to aggregate
}

// CommonAggregationWindows provides predefined window configurations
var CommonAggregationWindows = map[string]AggregationWindow{
	"realtime":  {Duration: 1 * time.Second, Size: 60, Interval: 1 * time.Second},    // 1min of 1s windows
	"minute":    {Duration: 1 * time.Minute, Size: 60, Interval: 10 * time.Second},   // 1hr of 1min windows
	"hourly":    {Duration: 1 * time.Hour, Size: 24, Interval: 5 * time.Minute},      // 1day of 1hr windows
	"daily":     {Duration: 24 * time.Hour, Size: 30, Interval: 1 * time.Hour},       // 30days of 1day windows
	"weekly":    {Duration: 7 * 24 * time.Hour, Size: 52, Interval: 4 * time.Hour},   // 1year of 1week windows
	"monthly":   {Duration: 30 * 24 * time.Hour, Size: 12, Interval: 24 * time.Hour}, // 1year of 1month windows
}

// MetricValue represents a single metric value with timestamp
type MetricValue struct {
	Value     float64           `json:"value"`
	Labels    map[string]string `json:"labels"`
	Timestamp time.Time         `json:"timestamp"`
}

// AggregatedMetric represents aggregated metric data for a time window
type AggregatedMetric struct {
	WindowStart time.Time         `json:"window_start"`
	WindowEnd   time.Time         `json:"window_end"`
	Count       int64             `json:"count"`
	Sum         float64           `json:"sum"`
	Min         float64           `json:"min"`
	Max         float64           `json:"max"`
	Mean        float64           `json:"mean"`
	Median      float64           `json:"median"`
	P95         float64           `json:"p95"`
	P99         float64           `json:"p99"`
	Values      []float64         `json:"values,omitempty"` // raw values for percentile calculation
	Labels      map[string]string `json:"labels"`
}

// MetricAggregator aggregates metrics over configurable time windows
type MetricAggregator struct {
	name            string
	window          AggregationWindow
	aggregatedData  map[string]*TimeSeriesData // key is labels hash
	rawData         map[string][]MetricValue   // buffer for raw values
	mu              sync.RWMutex
	ctx             context.Context
	cancel          context.CancelFunc
	ticker          *time.Ticker

	// Callbacks
	onAggregation func(string, *AggregatedMetric)
	onExpiry      func(string, *AggregatedMetric)
}

// TimeSeriesData holds aggregated metrics over multiple time windows
type TimeSeriesData struct {
	MetricName string                       `json:"metric_name"`
	Labels     map[string]string           `json:"labels"`
	Windows    []*AggregatedMetric         `json:"windows"`
	LastUpdate time.Time                   `json:"last_update"`
}

// NewMetricAggregator creates a new metric aggregator
func NewMetricAggregator(name string, window AggregationWindow) *MetricAggregator {
	ctx, cancel := context.WithCancel(context.Background())

	ma := &MetricAggregator{
		name:           name,
		window:         window,
		aggregatedData: make(map[string]*TimeSeriesData),
		rawData:        make(map[string][]MetricValue),
		ctx:            ctx,
		cancel:         cancel,
		ticker:         time.NewTicker(window.Interval),
	}

	// Start aggregation goroutine
	go ma.aggregationLoop()

	return ma
}

// AddValue adds a raw metric value for aggregation
func (ma *MetricAggregator) AddValue(value float64, labels map[string]string) {
	ma.mu.Lock()
	defer ma.mu.Unlock()

	labelsKey := ma.labelsToKey(labels)
	mv := MetricValue{
		Value:     value,
		Labels:    labels,
		Timestamp: time.Now(),
	}

	ma.rawData[labelsKey] = append(ma.rawData[labelsKey], mv)
}

// GetAggregatedData returns aggregated data for given labels
func (ma *MetricAggregator) GetAggregatedData(labels map[string]string) *TimeSeriesData {
	ma.mu.RLock()
	defer ma.mu.RUnlock()

	labelsKey := ma.labelsToKey(labels)
	return ma.aggregatedData[labelsKey]
}

// GetAllAggregatedData returns all aggregated data
func (ma *MetricAggregator) GetAllAggregatedData() map[string]*TimeSeriesData {
	ma.mu.RLock()
	defer ma.mu.RUnlock()

	result := make(map[string]*TimeSeriesData)
	for k, v := range ma.aggregatedData {
		result[k] = v
	}
	return result
}

// SetOnAggregation sets callback for when aggregation occurs
func (ma *MetricAggregator) SetOnAggregation(callback func(string, *AggregatedMetric)) {
	ma.onAggregation = callback
}

// SetOnExpiry sets callback for when old data expires
func (ma *MetricAggregator) SetOnExpiry(callback func(string, *AggregatedMetric)) {
	ma.onExpiry = callback
}

// Close stops the aggregator
func (ma *MetricAggregator) Close() {
	if ma.ticker != nil {
		ma.ticker.Stop()
	}
	ma.cancel()
}

// aggregationLoop runs the periodic aggregation
func (ma *MetricAggregator) aggregationLoop() {
	for {
		select {
		case <-ma.ctx.Done():
			return
		case <-ma.ticker.C:
			ma.performAggregation()
		}
	}
}

// performAggregation performs the actual aggregation
func (ma *MetricAggregator) performAggregation() {
	ma.mu.Lock()
	defer ma.mu.Unlock()

	now := time.Now()
	windowStart := now.Truncate(ma.window.Duration)
	windowEnd := windowStart.Add(ma.window.Duration)

	for labelsKey, rawValues := range ma.rawData {
		if len(rawValues) == 0 {
			continue
		}

		// Filter values for current window
		windowValues := ma.filterValuesForWindow(rawValues, windowStart, windowEnd)
		if len(windowValues) == 0 {
			continue
		}

		// Create aggregated metric
		aggregated := ma.calculateAggregation(windowValues, windowStart, windowEnd)

		// Get or create time series data
		if ma.aggregatedData[labelsKey] == nil {
			ma.aggregatedData[labelsKey] = &TimeSeriesData{
				MetricName: ma.name,
				Labels:     windowValues[0].Labels,
				Windows:    make([]*AggregatedMetric, 0),
			}
		}

		ts := ma.aggregatedData[labelsKey]
		ts.Windows = append(ts.Windows, aggregated)
		ts.LastUpdate = now

		// Maintain window size limit
		if len(ts.Windows) > ma.window.Size {
			expired := ts.Windows[0]
			ts.Windows = ts.Windows[1:]

			// Call expiry callback
			if ma.onExpiry != nil {
				ma.onExpiry(labelsKey, expired)
			}
		}

		// Call aggregation callback
		if ma.onAggregation != nil {
			ma.onAggregation(labelsKey, aggregated)
		}

		// Remove processed raw values
		ma.rawData[labelsKey] = ma.filterValuesAfterWindow(rawValues, windowEnd)
	}
}

// filterValuesForWindow filters metric values for a specific time window
func (ma *MetricAggregator) filterValuesForWindow(values []MetricValue, start, end time.Time) []MetricValue {
	var result []MetricValue
	for _, v := range values {
		if v.Timestamp.After(start) && v.Timestamp.Before(end) {
			result = append(result, v)
		}
	}
	return result
}

// filterValuesAfterWindow filters metric values after a specific time window
func (ma *MetricAggregator) filterValuesAfterWindow(values []MetricValue, windowEnd time.Time) []MetricValue {
	var result []MetricValue
	for _, v := range values {
		if v.Timestamp.After(windowEnd) {
			result = append(result, v)
		}
	}
	return result
}

// calculateAggregation calculates aggregation statistics
func (ma *MetricAggregator) calculateAggregation(values []MetricValue, start, end time.Time) *AggregatedMetric {
	if len(values) == 0 {
		return &AggregatedMetric{
			WindowStart: start,
			WindowEnd:   end,
			Labels:      make(map[string]string),
		}
	}

	// Extract just the numeric values
	numValues := make([]float64, len(values))
	sum := 0.0
	min := values[0].Value
	max := values[0].Value

	for i, v := range values {
		numValues[i] = v.Value
		sum += v.Value
		if v.Value < min {
			min = v.Value
		}
		if v.Value > max {
			max = v.Value
		}
	}

	// Sort for percentile calculations
	sortedValues := make([]float64, len(numValues))
	copy(sortedValues, numValues)
	sort.Float64s(sortedValues)

	mean := sum / float64(len(values))
	median := ma.percentile(sortedValues, 50)
	p95 := ma.percentile(sortedValues, 95)
	p99 := ma.percentile(sortedValues, 99)

	return &AggregatedMetric{
		WindowStart: start,
		WindowEnd:   end,
		Count:       int64(len(values)),
		Sum:         sum,
		Min:         min,
		Max:         max,
		Mean:        mean,
		Median:      median,
		P95:         p95,
		P99:         p99,
		Values:      numValues, // Store raw values for advanced calculations
		Labels:      values[0].Labels,
	}
}

// percentile calculates the nth percentile of a sorted slice
func (ma *MetricAggregator) percentile(sortedValues []float64, percentile float64) float64 {
	if len(sortedValues) == 0 {
		return 0
	}
	if len(sortedValues) == 1 {
		return sortedValues[0]
	}

	index := (percentile / 100.0) * float64(len(sortedValues)-1)
	lower := int(index)
	upper := lower + 1

	if upper >= len(sortedValues) {
		return sortedValues[len(sortedValues)-1]
	}

	weight := index - float64(lower)
	return sortedValues[lower]*(1-weight) + sortedValues[upper]*weight
}

// labelsToKey converts labels map to a string key
func (ma *MetricAggregator) labelsToKey(labels map[string]string) string {
	if len(labels) == 0 {
		return "default"
	}

	// Create deterministic key from labels
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	key := ""
	for i, k := range keys {
		if i > 0 {
			key += ","
		}
		key += k + "=" + labels[k]
	}
	return key
}

// MetricAggregatorManager manages multiple metric aggregators
type MetricAggregatorManager struct {
	aggregators map[string]*MetricAggregator
	mu          sync.RWMutex
}

// NewMetricAggregatorManager creates a new aggregator manager
func NewMetricAggregatorManager() *MetricAggregatorManager {
	return &MetricAggregatorManager{
		aggregators: make(map[string]*MetricAggregator),
	}
}

// CreateAggregator creates a new aggregator with given configuration
func (mam *MetricAggregatorManager) CreateAggregator(name string, window AggregationWindow) *MetricAggregator {
	mam.mu.Lock()
	defer mam.mu.Unlock()

	if existing, exists := mam.aggregators[name]; exists {
		existing.Close()
	}

	aggregator := NewMetricAggregator(name, window)
	mam.aggregators[name] = aggregator
	return aggregator
}

// GetAggregator returns an existing aggregator
func (mam *MetricAggregatorManager) GetAggregator(name string) *MetricAggregator {
	mam.mu.RLock()
	defer mam.mu.RUnlock()
	return mam.aggregators[name]
}

// CreateStandardAggregators creates aggregators for common time windows
func (mam *MetricAggregatorManager) CreateStandardAggregators(metricName string) {
	for windowName, window := range CommonAggregationWindows {
		aggregatorName := metricName + "_" + windowName
		mam.CreateAggregator(aggregatorName, window)
	}
}

// CloseAll closes all aggregators
func (mam *MetricAggregatorManager) CloseAll() {
	mam.mu.Lock()
	defer mam.mu.Unlock()

	for _, aggregator := range mam.aggregators {
		aggregator.Close()
	}
	mam.aggregators = make(map[string]*MetricAggregator)
}

// AggregatingMetricsCollector wraps a MetricsCollector to add aggregation
type AggregatingMetricsCollector struct {
	underlying MetricsCollector
	manager    *MetricAggregatorManager
}

// NewAggregatingMetricsCollector creates a new aggregating metrics collector
func NewAggregatingMetricsCollector(underlying MetricsCollector) *AggregatingMetricsCollector {
	return &AggregatingMetricsCollector{
		underlying: underlying,
		manager:    NewMetricAggregatorManager(),
	}
}

// Implement MetricsCollector interface

func (amc *AggregatingMetricsCollector) IncrementCounter(name string, labels map[string]string) {
	amc.underlying.IncrementCounter(name, labels)
	if aggregator := amc.manager.GetAggregator("counters"); aggregator != nil {
		aggregator.AddValue(1.0, labels)
	}
}

func (amc *AggregatingMetricsCollector) IncrementCounterBy(name string, value float64, labels map[string]string) {
	amc.underlying.IncrementCounterBy(name, value, labels)
	if aggregator := amc.manager.GetAggregator("counters"); aggregator != nil {
		aggregator.AddValue(value, labels)
	}
}

func (amc *AggregatingMetricsCollector) SetGauge(name string, value float64, labels map[string]string) {
	amc.underlying.SetGauge(name, value, labels)
	if aggregator := amc.manager.GetAggregator("gauges"); aggregator != nil {
		aggregator.AddValue(value, labels)
	}
}

func (amc *AggregatingMetricsCollector) IncrementGauge(name string, labels map[string]string) {
	amc.underlying.IncrementGauge(name, labels)
	if aggregator := amc.manager.GetAggregator("gauges"); aggregator != nil {
		aggregator.AddValue(1.0, labels)
	}
}

func (amc *AggregatingMetricsCollector) DecrementGauge(name string, labels map[string]string) {
	amc.underlying.DecrementGauge(name, labels)
	if aggregator := amc.manager.GetAggregator("gauges"); aggregator != nil {
		aggregator.AddValue(-1.0, labels)
	}
}

func (amc *AggregatingMetricsCollector) RecordHistogram(name string, value float64, labels map[string]string) {
	amc.underlying.RecordHistogram(name, value, labels)
	if aggregator := amc.manager.GetAggregator("histograms"); aggregator != nil {
		aggregator.AddValue(value, labels)
	}
}

func (amc *AggregatingMetricsCollector) RecordDuration(name string, duration time.Duration, labels map[string]string) {
	amc.underlying.RecordDuration(name, duration, labels)
	if aggregator := amc.manager.GetAggregator("durations"); aggregator != nil {
		aggregator.AddValue(duration.Seconds(), labels)
	}
}

func (amc *AggregatingMetricsCollector) StartTimer(name string, labels map[string]string) Timer {
	return amc.underlying.StartTimer(name, labels)
}

func (amc *AggregatingMetricsCollector) RecordTimer(name string, labels map[string]string) func() {
	return amc.underlying.RecordTimer(name, labels)
}

func (amc *AggregatingMetricsCollector) RegisterCustomMetric(name, help string, metricType MetricType, labelNames []string) error {
	return amc.underlying.RegisterCustomMetric(name, help, metricType, labelNames)
}

func (amc *AggregatingMetricsCollector) RecordCustomMetric(name string, value float64, labels map[string]string) {
	amc.underlying.RecordCustomMetric(name, value, labels)
	if aggregator := amc.manager.GetAggregator("custom"); aggregator != nil {
		aggregator.AddValue(value, labels)
	}
}

func (amc *AggregatingMetricsCollector) Start(ctx context.Context) error {
	return amc.underlying.Start(ctx)
}

func (amc *AggregatingMetricsCollector) Stop(ctx context.Context) error {
	amc.manager.CloseAll()
	return amc.underlying.Stop(ctx)
}

func (amc *AggregatingMetricsCollector) Flush(ctx context.Context) error {
	return amc.underlying.Flush(ctx)
}

// InitializeStandardAggregators initializes standard aggregators
func (amc *AggregatingMetricsCollector) InitializeStandardAggregators() {
	standardMetrics := []string{
		"query_duration",
		"connections",
		"connection_stats",
		"transaction_duration",
		"errors",
		"custom_counters",
		"custom_gauges",
		"custom_histograms",
		"custom_timers",
	}

	for _, metric := range standardMetrics {
		amc.manager.CreateStandardAggregators(metric)
	}
}

// GetAggregatedData returns aggregated data for a metric
func (amc *AggregatingMetricsCollector) GetAggregatedData(metricName string, labels map[string]string) *TimeSeriesData {
	if aggregator := amc.manager.GetAggregator(metricName); aggregator != nil {
		return aggregator.GetAggregatedData(labels)
	}
	return nil
}

// GetAllAggregatedData returns all aggregated data for a metric
func (amc *AggregatingMetricsCollector) GetAllAggregatedData(metricName string) map[string]*TimeSeriesData {
	if aggregator := amc.manager.GetAggregator(metricName); aggregator != nil {
		return aggregator.GetAllAggregatedData()
	}
	return nil
}

// Close closes all aggregators
func (amc *AggregatingMetricsCollector) Close() {
	amc.manager.CloseAll()
}

// ExportAggregatedData exports aggregated data as JSON
func (amc *AggregatingMetricsCollector) ExportAggregatedData() ([]byte, error) {
	allData := make(map[string]map[string]*TimeSeriesData)

	for name := range CommonAggregationWindows {
		for _, metric := range []string{"query_duration", "connections", "transaction_duration", "errors"} {
			aggregatorName := metric + "_" + name
			if data := amc.GetAllAggregatedData(aggregatorName); data != nil {
				if allData[aggregatorName] == nil {
					allData[aggregatorName] = make(map[string]*TimeSeriesData)
				}
				allData[aggregatorName] = data
			}
		}
	}

	return json.MarshalIndent(allData, "", "  ")
}