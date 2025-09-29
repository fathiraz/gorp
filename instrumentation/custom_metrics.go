package instrumentation

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// BusinessMetric represents a business logic metric
type BusinessMetric struct {
	Name        string            `json:"name"`
	Type        string            `json:"type"` // counter, gauge, histogram, timer
	Description string            `json:"description"`
	Labels      map[string]string `json:"labels"`
	Value       interface{}       `json:"value"`
	Timestamp   time.Time         `json:"timestamp"`
}

// CustomMetricsRegistry manages custom business logic metrics
type CustomMetricsRegistry struct {
	collectors     []MetricsCollector
	namedCounters  map[string]*NamedCounter
	namedGauges    map[string]*NamedGauge
	namedHistograms map[string]*NamedHistogram
	namedTimers    map[string]*NamedTimer
	mu             sync.RWMutex

	// Event hooks for custom metric recording
	beforeRecord []func(BusinessMetric)
	afterRecord  []func(BusinessMetric)
}

// NewCustomMetricsRegistry creates a new custom metrics registry
func NewCustomMetricsRegistry() *CustomMetricsRegistry {
	return &CustomMetricsRegistry{
		collectors:      make([]MetricsCollector, 0),
		namedCounters:   make(map[string]*NamedCounter),
		namedGauges:     make(map[string]*NamedGauge),
		namedHistograms: make(map[string]*NamedHistogram),
		namedTimers:     make(map[string]*NamedTimer),
		beforeRecord:    make([]func(BusinessMetric), 0),
		afterRecord:     make([]func(BusinessMetric), 0),
	}
}

// AddCollector adds a metrics collector to the registry
func (cmr *CustomMetricsRegistry) AddCollector(collector MetricsCollector) {
	cmr.mu.Lock()
	defer cmr.mu.Unlock()
	cmr.collectors = append(cmr.collectors, collector)
}

// RemoveCollector removes a metrics collector from the registry
func (cmr *CustomMetricsRegistry) RemoveCollector(collector MetricsCollector) {
	cmr.mu.Lock()
	defer cmr.mu.Unlock()
	for i, c := range cmr.collectors {
		if c == collector {
			cmr.collectors = append(cmr.collectors[:i], cmr.collectors[i+1:]...)
			break
		}
	}
}

// AddBeforeRecordHook adds a hook that runs before recording metrics
func (cmr *CustomMetricsRegistry) AddBeforeRecordHook(hook func(BusinessMetric)) {
	cmr.mu.Lock()
	defer cmr.mu.Unlock()
	cmr.beforeRecord = append(cmr.beforeRecord, hook)
}

// AddAfterRecordHook adds a hook that runs after recording metrics
func (cmr *CustomMetricsRegistry) AddAfterRecordHook(hook func(BusinessMetric)) {
	cmr.mu.Lock()
	defer cmr.mu.Unlock()
	cmr.afterRecord = append(cmr.afterRecord, hook)
}

// NamedCounter represents a named counter metric
type NamedCounter struct {
	name        string
	description string
	registry    *CustomMetricsRegistry
}

// NewNamedCounter creates a new named counter
func (cmr *CustomMetricsRegistry) NewNamedCounter(name, description string) *NamedCounter {
	cmr.mu.Lock()
	defer cmr.mu.Unlock()

	if existing, exists := cmr.namedCounters[name]; exists {
		return existing
	}

	counter := &NamedCounter{
		name:        name,
		description: description,
		registry:    cmr,
	}
	cmr.namedCounters[name] = counter
	return counter
}

// Inc increments the counter by 1
func (nc *NamedCounter) Inc(labels map[string]string) {
	nc.Add(1, labels)
}

// Add adds a value to the counter
func (nc *NamedCounter) Add(value float64, labels map[string]string) {
	metric := BusinessMetric{
		Name:        nc.name,
		Type:        "counter",
		Description: nc.description,
		Labels:      labels,
		Value:       value,
		Timestamp:   time.Now(),
	}

	nc.registry.recordMetric(metric)
}

// NamedGauge represents a named gauge metric
type NamedGauge struct {
	name        string
	description string
	registry    *CustomMetricsRegistry
}

// NewNamedGauge creates a new named gauge
func (cmr *CustomMetricsRegistry) NewNamedGauge(name, description string) *NamedGauge {
	cmr.mu.Lock()
	defer cmr.mu.Unlock()

	if existing, exists := cmr.namedGauges[name]; exists {
		return existing
	}

	gauge := &NamedGauge{
		name:        name,
		description: description,
		registry:    cmr,
	}
	cmr.namedGauges[name] = gauge
	return gauge
}

// Set sets the gauge value
func (ng *NamedGauge) Set(value float64, labels map[string]string) {
	metric := BusinessMetric{
		Name:        ng.name,
		Type:        "gauge",
		Description: ng.description,
		Labels:      labels,
		Value:       value,
		Timestamp:   time.Now(),
	}

	ng.registry.recordMetric(metric)
}

// Inc increments the gauge by 1
func (ng *NamedGauge) Inc(labels map[string]string) {
	ng.Add(1, labels)
}

// Dec decrements the gauge by 1
func (ng *NamedGauge) Dec(labels map[string]string) {
	ng.Add(-1, labels)
}

// Add adds a value to the gauge
func (ng *NamedGauge) Add(value float64, labels map[string]string) {
	metric := BusinessMetric{
		Name:        ng.name,
		Type:        "gauge",
		Description: ng.description,
		Labels:      labels,
		Value:       value,
		Timestamp:   time.Now(),
	}

	ng.registry.recordMetric(metric)
}

// NamedHistogram represents a named histogram metric
type NamedHistogram struct {
	name        string
	description string
	registry    *CustomMetricsRegistry
}

// NewNamedHistogram creates a new named histogram
func (cmr *CustomMetricsRegistry) NewNamedHistogram(name, description string) *NamedHistogram {
	cmr.mu.Lock()
	defer cmr.mu.Unlock()

	if existing, exists := cmr.namedHistograms[name]; exists {
		return existing
	}

	histogram := &NamedHistogram{
		name:        name,
		description: description,
		registry:    cmr,
	}
	cmr.namedHistograms[name] = histogram
	return histogram
}

// Observe records an observation
func (nh *NamedHistogram) Observe(value float64, labels map[string]string) {
	metric := BusinessMetric{
		Name:        nh.name,
		Type:        "histogram",
		Description: nh.description,
		Labels:      labels,
		Value:       value,
		Timestamp:   time.Now(),
	}

	nh.registry.recordMetric(metric)
}

// NamedTimer represents a named timer metric
type NamedTimer struct {
	name        string
	description string
	registry    *CustomMetricsRegistry
}

// NewNamedTimer creates a new named timer
func (cmr *CustomMetricsRegistry) NewNamedTimer(name, description string) *NamedTimer {
	cmr.mu.Lock()
	defer cmr.mu.Unlock()

	if existing, exists := cmr.namedTimers[name]; exists {
		return existing
	}

	timer := &NamedTimer{
		name:        name,
		description: description,
		registry:    cmr,
	}
	cmr.namedTimers[name] = timer
	return timer
}

// Record records a duration
func (nt *NamedTimer) Record(duration time.Duration, labels map[string]string) {
	metric := BusinessMetric{
		Name:        nt.name,
		Type:        "timer",
		Description: nt.description,
		Labels:      labels,
		Value:       duration,
		Timestamp:   time.Now(),
	}

	nt.registry.recordMetric(metric)
}

// Time measures the duration of a function execution
func (nt *NamedTimer) Time(labels map[string]string, fn func()) {
	start := time.Now()
	fn()
	nt.Record(time.Since(start), labels)
}

// recordMetric records a business metric to all collectors
func (cmr *CustomMetricsRegistry) recordMetric(metric BusinessMetric) {
	cmr.mu.RLock()
	defer cmr.mu.RUnlock()

	// Run before hooks
	for _, hook := range cmr.beforeRecord {
		hook(metric)
	}

	// Record to all collectors
	for _, collector := range cmr.collectors {
		switch metric.Type {
		case "counter":
			collector.IncrementCounter(metric.Name, metric.Labels)
		case "gauge":
			if value, ok := metric.Value.(float64); ok {
				collector.SetGauge(metric.Name, value, metric.Labels)
			}
		case "histogram":
			if value, ok := metric.Value.(float64); ok {
				collector.RecordHistogram(metric.Name, value, metric.Labels)
			}
		case "timer":
			if duration, ok := metric.Value.(time.Duration); ok {
				collector.RecordDuration(metric.Name, duration, metric.Labels)
			}
		}
	}

	// Run after hooks
	for _, hook := range cmr.afterRecord {
		hook(metric)
	}
}

// BusinessMetricsCollector provides convenient methods for recording business metrics
type BusinessMetricsCollector struct {
	registry *CustomMetricsRegistry

	// Common business metrics
	UserRegistrations *NamedCounter
	UserLogins        *NamedCounter
	OrdersCreated     *NamedCounter
	OrdersCompleted   *NamedCounter
	PaymentsProcessed *NamedCounter
	PaymentFailures   *NamedCounter
	APIRequests       *NamedCounter
	APIErrors         *NamedCounter

	ActiveUsers       *NamedGauge
	QueueDepth        *NamedGauge
	CacheHitRatio     *NamedGauge
	SystemLoad        *NamedGauge

	RequestDuration   *NamedHistogram
	OrderValue        *NamedHistogram
	ResponseSize      *NamedHistogram

	ProcessingTime    *NamedTimer
	BackupDuration    *NamedTimer
	MaintenanceWindow *NamedTimer
}

// NewBusinessMetricsCollector creates a new business metrics collector
func NewBusinessMetricsCollector(registry *CustomMetricsRegistry) *BusinessMetricsCollector {
	return &BusinessMetricsCollector{
		registry: registry,

		// Initialize common counters
		UserRegistrations: registry.NewNamedCounter("user_registrations_total", "Total number of user registrations"),
		UserLogins:        registry.NewNamedCounter("user_logins_total", "Total number of user logins"),
		OrdersCreated:     registry.NewNamedCounter("orders_created_total", "Total number of orders created"),
		OrdersCompleted:   registry.NewNamedCounter("orders_completed_total", "Total number of orders completed"),
		PaymentsProcessed: registry.NewNamedCounter("payments_processed_total", "Total number of payments processed"),
		PaymentFailures:   registry.NewNamedCounter("payment_failures_total", "Total number of payment failures"),
		APIRequests:       registry.NewNamedCounter("api_requests_total", "Total number of API requests"),
		APIErrors:         registry.NewNamedCounter("api_errors_total", "Total number of API errors"),

		// Initialize common gauges
		ActiveUsers:   registry.NewNamedGauge("active_users", "Number of currently active users"),
		QueueDepth:    registry.NewNamedGauge("queue_depth", "Current queue depth"),
		CacheHitRatio: registry.NewNamedGauge("cache_hit_ratio", "Cache hit ratio"),
		SystemLoad:    registry.NewNamedGauge("system_load", "Current system load"),

		// Initialize common histograms
		RequestDuration: registry.NewNamedHistogram("request_duration_seconds", "HTTP request duration in seconds"),
		OrderValue:      registry.NewNamedHistogram("order_value", "Order value distribution"),
		ResponseSize:    registry.NewNamedHistogram("response_size_bytes", "HTTP response size in bytes"),

		// Initialize common timers
		ProcessingTime:    registry.NewNamedTimer("processing_time", "Processing time for various operations"),
		BackupDuration:    registry.NewNamedTimer("backup_duration", "Database backup duration"),
		MaintenanceWindow: registry.NewNamedTimer("maintenance_window", "Maintenance window duration"),
	}
}

// RecordUserRegistration records a user registration event
func (bmc *BusinessMetricsCollector) RecordUserRegistration(source string) {
	bmc.UserRegistrations.Inc(map[string]string{"source": source})
}

// RecordUserLogin records a user login event
func (bmc *BusinessMetricsCollector) RecordUserLogin(method string, success bool) {
	labels := map[string]string{"method": method, "success": fmt.Sprintf("%t", success)}
	bmc.UserLogins.Inc(labels)
}

// RecordOrderCreated records an order creation event
func (bmc *BusinessMetricsCollector) RecordOrderCreated(channel string, value float64) {
	bmc.OrdersCreated.Inc(map[string]string{"channel": channel})
	bmc.OrderValue.Observe(value, map[string]string{"channel": channel, "status": "created"})
}

// RecordOrderCompleted records an order completion event
func (bmc *BusinessMetricsCollector) RecordOrderCompleted(channel string, value float64) {
	bmc.OrdersCompleted.Inc(map[string]string{"channel": channel})
	bmc.OrderValue.Observe(value, map[string]string{"channel": channel, "status": "completed"})
}

// RecordPaymentProcessed records a payment processing event
func (bmc *BusinessMetricsCollector) RecordPaymentProcessed(method string, amount float64, success bool) {
	status := "success"
	if !success {
		status = "failure"
		bmc.PaymentFailures.Inc(map[string]string{"method": method})
	}

	labels := map[string]string{"method": method, "status": status}
	bmc.PaymentsProcessed.Inc(labels)

	if success {
		bmc.OrderValue.Observe(amount, map[string]string{"type": "payment", "method": method})
	}
}

// RecordAPIRequest records an API request event
func (bmc *BusinessMetricsCollector) RecordAPIRequest(method, endpoint string, statusCode int, duration time.Duration, responseSize int64) {
	labels := map[string]string{
		"method":      method,
		"endpoint":    endpoint,
		"status_code": fmt.Sprintf("%d", statusCode),
	}

	bmc.APIRequests.Inc(labels)
	bmc.RequestDuration.Observe(duration.Seconds(), labels)
	bmc.ResponseSize.Observe(float64(responseSize), labels)

	if statusCode >= 400 {
		bmc.APIErrors.Inc(labels)
	}
}

// UpdateActiveUsers updates the active users gauge
func (bmc *BusinessMetricsCollector) UpdateActiveUsers(count int, userType string) {
	bmc.ActiveUsers.Set(float64(count), map[string]string{"type": userType})
}

// UpdateQueueDepth updates the queue depth gauge
func (bmc *BusinessMetricsCollector) UpdateQueueDepth(queueName string, depth int) {
	bmc.QueueDepth.Set(float64(depth), map[string]string{"queue": queueName})
}

// UpdateCacheHitRatio updates the cache hit ratio gauge
func (bmc *BusinessMetricsCollector) UpdateCacheHitRatio(cacheName string, ratio float64) {
	bmc.CacheHitRatio.Set(ratio, map[string]string{"cache": cacheName})
}

// RecordProcessingTime records processing time for an operation
func (bmc *BusinessMetricsCollector) RecordProcessingTime(operation string, duration time.Duration) {
	bmc.ProcessingTime.Record(duration, map[string]string{"operation": operation})
}

// TimeOperation measures the duration of an operation
func (bmc *BusinessMetricsCollector) TimeOperation(operation string, fn func()) {
	bmc.ProcessingTime.Time(map[string]string{"operation": operation}, fn)
}

// BusinessMetricsContext provides context-aware business metrics
type BusinessMetricsContext struct {
	ctx       context.Context
	collector *BusinessMetricsCollector
	labels    map[string]string
}

// NewBusinessMetricsContext creates a new business metrics context
func NewBusinessMetricsContext(ctx context.Context, collector *BusinessMetricsCollector, labels map[string]string) *BusinessMetricsContext {
	return &BusinessMetricsContext{
		ctx:       ctx,
		collector: collector,
		labels:    labels,
	}
}

// WithLabel adds a label to the context
func (bmc *BusinessMetricsContext) WithLabel(key, value string) *BusinessMetricsContext {
	newLabels := make(map[string]string)
	for k, v := range bmc.labels {
		newLabels[k] = v
	}
	newLabels[key] = value

	return &BusinessMetricsContext{
		ctx:       bmc.ctx,
		collector: bmc.collector,
		labels:    newLabels,
	}
}

// RecordUserLogin records a user login with context labels
func (bmc *BusinessMetricsContext) RecordUserLogin(method string, success bool) {
	labels := make(map[string]string)
	for k, v := range bmc.labels {
		labels[k] = v
	}
	labels["method"] = method
	labels["success"] = fmt.Sprintf("%t", success)

	bmc.collector.UserLogins.Inc(labels)
}

// Context returns the underlying context
func (bmc *BusinessMetricsContext) Context() context.Context {
	return bmc.ctx
}

// Global default registry
var defaultCustomMetricsRegistry *CustomMetricsRegistry
var defaultBusinessMetricsCollector *BusinessMetricsCollector
var registryOnce sync.Once

// GetDefaultCustomMetricsRegistry returns the global custom metrics registry
func GetDefaultCustomMetricsRegistry() *CustomMetricsRegistry {
	registryOnce.Do(func() {
		defaultCustomMetricsRegistry = NewCustomMetricsRegistry()
		defaultBusinessMetricsCollector = NewBusinessMetricsCollector(defaultCustomMetricsRegistry)
	})
	return defaultCustomMetricsRegistry
}

// GetDefaultBusinessMetricsCollector returns the global business metrics collector
func GetDefaultBusinessMetricsCollector() *BusinessMetricsCollector {
	registryOnce.Do(func() {
		defaultCustomMetricsRegistry = NewCustomMetricsRegistry()
		defaultBusinessMetricsCollector = NewBusinessMetricsCollector(defaultCustomMetricsRegistry)
	})
	return defaultBusinessMetricsCollector
}

// RecordUserRegistration records a user registration using the default collector
func RecordUserRegistration(source string) {
	GetDefaultBusinessMetricsCollector().RecordUserRegistration(source)
}

// RecordUserLogin records a user login using the default collector
func RecordUserLogin(method string, success bool) {
	GetDefaultBusinessMetricsCollector().RecordUserLogin(method, success)
}

// RecordAPIRequest records an API request using the default collector
func RecordAPIRequest(method, endpoint string, statusCode int, duration time.Duration, responseSize int64) {
	GetDefaultBusinessMetricsCollector().RecordAPIRequest(method, endpoint, statusCode, duration, responseSize)
}

// TimeOperation times an operation using the default collector
func TimeOperation(operation string, fn func()) {
	GetDefaultBusinessMetricsCollector().TimeOperation(operation, fn)
}