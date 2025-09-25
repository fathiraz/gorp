package logging

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// LogrusAdapter adapts logrus to GORP Logger interface
type LogrusAdapter struct {
	logger *logrus.Logger
	level  LogLevel
}

// NewLogrusAdapter creates a new logrus adapter
func NewLogrusAdapter(logger *logrus.Logger) *LogrusAdapter {
	if logger == nil {
		logger = logrus.New()
	}

	adapter := &LogrusAdapter{
		logger: logger,
		level:  INFO, // Default level
	}

	// Set initial level based on logrus level
	switch logger.GetLevel() {
	case logrus.DebugLevel:
		adapter.level = DEBUG
	case logrus.InfoLevel:
		adapter.level = INFO
	case logrus.WarnLevel:
		adapter.level = WARN
	case logrus.ErrorLevel:
		adapter.level = ERROR
	case logrus.FatalLevel:
		adapter.level = FATAL
	}

	return adapter
}

func (l *LogrusAdapter) Debug(ctx context.Context, msg string, fields ...Field) {
	l.logWithFields(logrus.DebugLevel, ctx, msg, nil, fields...)
}

func (l *LogrusAdapter) Info(ctx context.Context, msg string, fields ...Field) {
	l.logWithFields(logrus.InfoLevel, ctx, msg, nil, fields...)
}

func (l *LogrusAdapter) Warn(ctx context.Context, msg string, fields ...Field) {
	l.logWithFields(logrus.WarnLevel, ctx, msg, nil, fields...)
}

func (l *LogrusAdapter) Error(ctx context.Context, msg string, err error, fields ...Field) {
	l.logWithFields(logrus.ErrorLevel, ctx, msg, err, fields...)
}

func (l *LogrusAdapter) Fatal(ctx context.Context, msg string, err error, fields ...Field) {
	l.logWithFields(logrus.FatalLevel, ctx, msg, err, fields...)
}

func (l *LogrusAdapter) LogQuery(ctx context.Context, query string, args []interface{}, duration time.Duration, err error) {
	fields := []Field{
		String("query", query),
		Duration("duration", duration),
		Any("args", args),
	}

	if err != nil {
		l.Error(ctx, "Query failed", err, fields...)
	} else {
		l.Debug(ctx, "Query executed", fields...)
	}
}

func (l *LogrusAdapter) LogSlowQuery(ctx context.Context, query string, args []interface{}, duration time.Duration, threshold time.Duration) {
	if duration < threshold {
		return
	}

	fields := []Field{
		String("query", query),
		Duration("duration", duration),
		Duration("threshold", threshold),
		Float64("slowness_ratio", float64(duration)/float64(threshold)),
		Any("args", args),
	}

	l.Warn(ctx, "Slow query detected", fields...)
}

func (l *LogrusAdapter) LogTransaction(ctx context.Context, event TransactionEvent, fields ...Field) {
	allFields := append([]Field{String("event", string(event))}, fields...)
	l.Debug(ctx, "Transaction event", allFields...)
}

func (l *LogrusAdapter) LogConnection(ctx context.Context, event ConnectionEvent, fields ...Field) {
	allFields := append([]Field{String("event", string(event))}, fields...)

	if event == ConnectionError {
		// Extract error from fields
		var err error
		for _, field := range fields {
			if field.Key == "error" {
				if e, ok := field.Value.(error); ok {
					err = e
					break
				}
			}
		}
		l.Error(ctx, "Connection event", err, allFields...)
	} else {
		l.Info(ctx, "Connection event", allFields...)
	}
}

func (l *LogrusAdapter) LogMetrics(ctx context.Context, metrics *PerformanceMetrics) {
	fields := []Field{
		Int64("query_count", metrics.QueryCount),
		Duration("average_latency", metrics.AverageLatency),
		Float64("error_rate", metrics.ErrorRate),
		Int64("slow_query_count", metrics.SlowQueryCount),
		Int("connections_active", metrics.ConnectionsActive),
		Int("connections_idle", metrics.ConnectionsIdle),
		Time("timestamp", metrics.Timestamp),
	}

	l.Info(ctx, "Performance metrics", fields...)
}

func (l *LogrusAdapter) SetLevel(level LogLevel) {
	l.level = level
	switch level {
	case DEBUG:
		l.logger.SetLevel(logrus.DebugLevel)
	case INFO:
		l.logger.SetLevel(logrus.InfoLevel)
	case WARN:
		l.logger.SetLevel(logrus.WarnLevel)
	case ERROR:
		l.logger.SetLevel(logrus.ErrorLevel)
	case FATAL:
		l.logger.SetLevel(logrus.FatalLevel)
	}
}

func (l *LogrusAdapter) GetLevel() LogLevel {
	return l.level
}

func (l *LogrusAdapter) IsEnabled(level LogLevel) bool {
	return level >= l.level
}

func (l *LogrusAdapter) WithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, "request_id", requestID)
}

func (l *LogrusAdapter) WithFields(fields ...Field) Logger {
	logrusFields := make(logrus.Fields)
	for _, field := range fields {
		logrusFields[field.Key] = field.Value
	}

	newLogger := l.logger.WithFields(logrusFields)
	return &LogrusAdapter{
		logger: newLogger.Logger,
		level:  l.level,
	}
}

func (l *LogrusAdapter) logWithFields(level logrus.Level, ctx context.Context, msg string, err error, fields ...Field) {
	if !l.logger.IsLevelEnabled(level) {
		return
	}

	logrusFields := make(logrus.Fields)

	// Add request ID from context
	if requestID := l.getRequestID(ctx); requestID != "" {
		logrusFields["request_id"] = requestID
	}

	// Add fields
	for _, field := range fields {
		logrusFields[field.Key] = field.Value
	}

	// Add error if present
	if err != nil {
		logrusFields["error"] = err.Error()
	}

	entry := l.logger.WithFields(logrusFields)
	entry.Log(level, msg)
}

func (l *LogrusAdapter) getRequestID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}

	if requestID, ok := ctx.Value("request_id").(string); ok {
		return requestID
	}

	return ""
}

// ZapAdapter adapts zap to GORP Logger interface
type ZapAdapter struct {
	logger *zap.Logger
	sugar  *zap.SugaredLogger
	level  LogLevel
}

// NewZapAdapter creates a new zap adapter
func NewZapAdapter(logger *zap.Logger) *ZapAdapter {
	if logger == nil {
		logger = zap.NewNop()
	}

	adapter := &ZapAdapter{
		logger: logger,
		sugar:  logger.Sugar(),
		level:  INFO, // Default level
	}

	// Set initial level based on zap core level
	if logger.Core().Enabled(zapcore.DebugLevel) {
		adapter.level = DEBUG
	} else if logger.Core().Enabled(zapcore.InfoLevel) {
		adapter.level = INFO
	} else if logger.Core().Enabled(zapcore.WarnLevel) {
		adapter.level = WARN
	} else if logger.Core().Enabled(zapcore.ErrorLevel) {
		adapter.level = ERROR
	} else {
		adapter.level = FATAL
	}

	return adapter
}

func (z *ZapAdapter) Debug(ctx context.Context, msg string, fields ...Field) {
	z.logWithFields(zapcore.DebugLevel, ctx, msg, nil, fields...)
}

func (z *ZapAdapter) Info(ctx context.Context, msg string, fields ...Field) {
	z.logWithFields(zapcore.InfoLevel, ctx, msg, nil, fields...)
}

func (z *ZapAdapter) Warn(ctx context.Context, msg string, fields ...Field) {
	z.logWithFields(zapcore.WarnLevel, ctx, msg, nil, fields...)
}

func (z *ZapAdapter) Error(ctx context.Context, msg string, err error, fields ...Field) {
	z.logWithFields(zapcore.ErrorLevel, ctx, msg, err, fields...)
}

func (z *ZapAdapter) Fatal(ctx context.Context, msg string, err error, fields ...Field) {
	z.logWithFields(zapcore.FatalLevel, ctx, msg, err, fields...)
}

func (z *ZapAdapter) LogQuery(ctx context.Context, query string, args []interface{}, duration time.Duration, err error) {
	fields := []Field{
		String("query", query),
		Duration("duration", duration),
		Any("args", args),
	}

	if err != nil {
		z.Error(ctx, "Query failed", err, fields...)
	} else {
		z.Debug(ctx, "Query executed", fields...)
	}
}

func (z *ZapAdapter) LogSlowQuery(ctx context.Context, query string, args []interface{}, duration time.Duration, threshold time.Duration) {
	if duration < threshold {
		return
	}

	fields := []Field{
		String("query", query),
		Duration("duration", duration),
		Duration("threshold", threshold),
		Float64("slowness_ratio", float64(duration)/float64(threshold)),
		Any("args", args),
	}

	z.Warn(ctx, "Slow query detected", fields...)
}

func (z *ZapAdapter) LogTransaction(ctx context.Context, event TransactionEvent, fields ...Field) {
	allFields := append([]Field{String("event", string(event))}, fields...)
	z.Debug(ctx, "Transaction event", allFields...)
}

func (z *ZapAdapter) LogConnection(ctx context.Context, event ConnectionEvent, fields ...Field) {
	allFields := append([]Field{String("event", string(event))}, fields...)

	if event == ConnectionError {
		// Extract error from fields
		var err error
		for _, field := range fields {
			if field.Key == "error" {
				if e, ok := field.Value.(error); ok {
					err = e
					break
				}
			}
		}
		z.Error(ctx, "Connection event", err, allFields...)
	} else {
		z.Info(ctx, "Connection event", allFields...)
	}
}

func (z *ZapAdapter) LogMetrics(ctx context.Context, metrics *PerformanceMetrics) {
	fields := []Field{
		Int64("query_count", metrics.QueryCount),
		Duration("average_latency", metrics.AverageLatency),
		Float64("error_rate", metrics.ErrorRate),
		Int64("slow_query_count", metrics.SlowQueryCount),
		Int("connections_active", metrics.ConnectionsActive),
		Int("connections_idle", metrics.ConnectionsIdle),
		Time("timestamp", metrics.Timestamp),
	}

	z.Info(ctx, "Performance metrics", fields...)
}

func (z *ZapAdapter) SetLevel(level LogLevel) {
	z.level = level
	// Note: Changing zap level at runtime requires rebuilding the logger
	// This is a simplified implementation
}

func (z *ZapAdapter) GetLevel() LogLevel {
	return z.level
}

func (z *ZapAdapter) IsEnabled(level LogLevel) bool {
	zapLevel := z.toZapLevel(level)
	return z.logger.Core().Enabled(zapLevel)
}

func (z *ZapAdapter) WithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, "request_id", requestID)
}

func (z *ZapAdapter) WithFields(fields ...Field) Logger {
	zapFields := make([]zap.Field, 0, len(fields))
	for _, field := range fields {
		zapFields = append(zapFields, z.toZapField(field))
	}

	newLogger := z.logger.With(zapFields...)
	return &ZapAdapter{
		logger: newLogger,
		sugar:  newLogger.Sugar(),
		level:  z.level,
	}
}

func (z *ZapAdapter) logWithFields(level zapcore.Level, ctx context.Context, msg string, err error, fields ...Field) {
	if !z.logger.Core().Enabled(level) {
		return
	}

	zapFields := make([]zap.Field, 0, len(fields)+2)

	// Add request ID from context
	if requestID := z.getRequestID(ctx); requestID != "" {
		zapFields = append(zapFields, zap.String("request_id", requestID))
	}

	// Add error if present
	if err != nil {
		zapFields = append(zapFields, zap.Error(err))
	}

	// Add fields
	for _, field := range fields {
		zapFields = append(zapFields, z.toZapField(field))
	}

	z.logger.Log(level, msg, zapFields...)
}

func (z *ZapAdapter) toZapField(field Field) zap.Field {
	switch v := field.Value.(type) {
	case string:
		return zap.String(field.Key, v)
	case int:
		return zap.Int(field.Key, v)
	case int64:
		return zap.Int64(field.Key, v)
	case float64:
		return zap.Float64(field.Key, v)
	case bool:
		return zap.Bool(field.Key, v)
	case time.Duration:
		return zap.Duration(field.Key, v)
	case time.Time:
		return zap.Time(field.Key, v)
	case error:
		return zap.Error(v)
	default:
		return zap.Any(field.Key, v)
	}
}

func (z *ZapAdapter) toZapLevel(level LogLevel) zapcore.Level {
	switch level {
	case DEBUG:
		return zapcore.DebugLevel
	case INFO:
		return zapcore.InfoLevel
	case WARN:
		return zapcore.WarnLevel
	case ERROR:
		return zapcore.ErrorLevel
	case FATAL:
		return zapcore.FatalLevel
	default:
		return zapcore.InfoLevel
	}
}

func (z *ZapAdapter) getRequestID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}

	if requestID, ok := ctx.Value("request_id").(string); ok {
		return requestID
	}

	return ""
}

// SlogAdapter adapts slog to GORP Logger interface
type SlogAdapter struct {
	logger *slog.Logger
	level  LogLevel
}

// NewSlogAdapter creates a new slog adapter
func NewSlogAdapter(logger *slog.Logger) *SlogAdapter {
	if logger == nil {
		logger = slog.Default()
	}

	adapter := &SlogAdapter{
		logger: logger,
		level:  INFO, // Default level
	}

	// Try to determine level from slog logger (this is approximate)
	if logger.Enabled(context.Background(), slog.LevelDebug) {
		adapter.level = DEBUG
	} else if logger.Enabled(context.Background(), slog.LevelInfo) {
		adapter.level = INFO
	} else if logger.Enabled(context.Background(), slog.LevelWarn) {
		adapter.level = WARN
	} else if logger.Enabled(context.Background(), slog.LevelError) {
		adapter.level = ERROR
	} else {
		adapter.level = FATAL
	}

	return adapter
}

func (s *SlogAdapter) Debug(ctx context.Context, msg string, fields ...Field) {
	s.logWithFields(ctx, slog.LevelDebug, msg, nil, fields...)
}

func (s *SlogAdapter) Info(ctx context.Context, msg string, fields ...Field) {
	s.logWithFields(ctx, slog.LevelInfo, msg, nil, fields...)
}

func (s *SlogAdapter) Warn(ctx context.Context, msg string, fields ...Field) {
	s.logWithFields(ctx, slog.LevelWarn, msg, nil, fields...)
}

func (s *SlogAdapter) Error(ctx context.Context, msg string, err error, fields ...Field) {
	s.logWithFields(ctx, slog.LevelError, msg, err, fields...)
}

func (s *SlogAdapter) Fatal(ctx context.Context, msg string, err error, fields ...Field) {
	s.logWithFields(ctx, slog.LevelError, msg, err, fields...)
	// Note: slog doesn't have a fatal level that exits
}

func (s *SlogAdapter) LogQuery(ctx context.Context, query string, args []interface{}, duration time.Duration, err error) {
	fields := []Field{
		String("query", query),
		Duration("duration", duration),
		Any("args", args),
	}

	if err != nil {
		s.Error(ctx, "Query failed", err, fields...)
	} else {
		s.Debug(ctx, "Query executed", fields...)
	}
}

func (s *SlogAdapter) LogSlowQuery(ctx context.Context, query string, args []interface{}, duration time.Duration, threshold time.Duration) {
	if duration < threshold {
		return
	}

	fields := []Field{
		String("query", query),
		Duration("duration", duration),
		Duration("threshold", threshold),
		Float64("slowness_ratio", float64(duration)/float64(threshold)),
		Any("args", args),
	}

	s.Warn(ctx, "Slow query detected", fields...)
}

func (s *SlogAdapter) LogTransaction(ctx context.Context, event TransactionEvent, fields ...Field) {
	allFields := append([]Field{String("event", string(event))}, fields...)
	s.Debug(ctx, "Transaction event", allFields...)
}

func (s *SlogAdapter) LogConnection(ctx context.Context, event ConnectionEvent, fields ...Field) {
	allFields := append([]Field{String("event", string(event))}, fields...)

	if event == ConnectionError {
		// Extract error from fields
		var err error
		for _, field := range fields {
			if field.Key == "error" {
				if e, ok := field.Value.(error); ok {
					err = e
					break
				}
			}
		}
		s.Error(ctx, "Connection event", err, allFields...)
	} else {
		s.Info(ctx, "Connection event", allFields...)
	}
}

func (s *SlogAdapter) LogMetrics(ctx context.Context, metrics *PerformanceMetrics) {
	fields := []Field{
		Int64("query_count", metrics.QueryCount),
		Duration("average_latency", metrics.AverageLatency),
		Float64("error_rate", metrics.ErrorRate),
		Int64("slow_query_count", metrics.SlowQueryCount),
		Int("connections_active", metrics.ConnectionsActive),
		Int("connections_idle", metrics.ConnectionsIdle),
		Time("timestamp", metrics.Timestamp),
	}

	s.Info(ctx, "Performance metrics", fields...)
}

func (s *SlogAdapter) SetLevel(level LogLevel) {
	s.level = level
	// Note: slog level changes require creating a new handler
}

func (s *SlogAdapter) GetLevel() LogLevel {
	return s.level
}

func (s *SlogAdapter) IsEnabled(level LogLevel) bool {
	slogLevel := s.toSlogLevel(level)
	return s.logger.Enabled(context.Background(), slogLevel)
}

func (s *SlogAdapter) WithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, "request_id", requestID)
}

func (s *SlogAdapter) WithFields(fields ...Field) Logger {
	slogArgs := make([]any, 0, len(fields)*2)
	for _, field := range fields {
		slogArgs = append(slogArgs, field.Key, field.Value)
	}

	newLogger := s.logger.With(slogArgs...)
	return &SlogAdapter{
		logger: newLogger,
		level:  s.level,
	}
}

func (s *SlogAdapter) logWithFields(ctx context.Context, level slog.Level, msg string, err error, fields ...Field) {
	if !s.logger.Enabled(ctx, level) {
		return
	}

	args := make([]any, 0, len(fields)*2+4)

	// Add request ID from context
	if requestID := s.getRequestID(ctx); requestID != "" {
		args = append(args, "request_id", requestID)
	}

	// Add error if present
	if err != nil {
		args = append(args, "error", err.Error())
	}

	// Add fields
	for _, field := range fields {
		args = append(args, field.Key, field.Value)
	}

	s.logger.Log(ctx, level, msg, args...)
}

func (s *SlogAdapter) toSlogLevel(level LogLevel) slog.Level {
	switch level {
	case DEBUG:
		return slog.LevelDebug
	case INFO:
		return slog.LevelInfo
	case WARN:
		return slog.LevelWarn
	case ERROR:
		return slog.LevelError
	case FATAL:
		return slog.LevelError // slog doesn't have fatal
	default:
		return slog.LevelInfo
	}
}

func (s *SlogAdapter) getRequestID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}

	if requestID, ok := ctx.Value("request_id").(string); ok {
		return requestID
	}

	return ""
}

// Convenience functions for creating adapters

// NewLogrusLogger creates a new logrus-based logger with default configuration
func NewLogrusLogger() Logger {
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})
	logger.SetLevel(logrus.InfoLevel)
	return NewLogrusAdapter(logger)
}

// NewZapLogger creates a new zap-based logger with default configuration
func NewZapLogger() Logger {
	config := zap.NewProductionConfig()
	logger, _ := config.Build()
	return NewZapAdapter(logger)
}

// NewSlogLogger creates a new slog-based logger with default configuration
func NewSlogLogger() Logger {
	handler := slog.NewJSONHandler(nil, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	logger := slog.New(handler)
	return NewSlogAdapter(logger)
}

// LoggerFactory provides a way to create different types of loggers
type LoggerFactory struct{}

// NewLoggerFactory creates a new logger factory
func NewLoggerFactory() *LoggerFactory {
	return &LoggerFactory{}
}

// CreateLogger creates a logger of the specified type
func (lf *LoggerFactory) CreateLogger(loggerType string, config interface{}) (Logger, error) {
	switch strings.ToLower(loggerType) {
	case "standard":
		if cfg, ok := config.(*LoggerConfig); ok {
			return NewStandardLogger(cfg), nil
		}
		return NewStandardLogger(nil), nil

	case "logrus":
		if logger, ok := config.(*logrus.Logger); ok {
			return NewLogrusAdapter(logger), nil
		}
		return NewLogrusLogger(), nil

	case "zap":
		if logger, ok := config.(*zap.Logger); ok {
			return NewZapAdapter(logger), nil
		}
		return NewZapLogger(), nil

	case "slog":
		if logger, ok := config.(*slog.Logger); ok {
			return NewSlogAdapter(logger), nil
		}
		return NewSlogLogger(), nil

	case "noop":
		return NewNoOpLogger(), nil

	default:
		return nil, fmt.Errorf("unsupported logger type: %s", loggerType)
	}
}

// GetSupportedLoggers returns a list of supported logger types
func (lf *LoggerFactory) GetSupportedLoggers() []string {
	return []string{"standard", "logrus", "zap", "slog", "noop"}
}