// Copyright 2012 James Cooper. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package gorp_test

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/go-gorp/gorp/v3"
	"github.com/stretchr/testify/suite"
)

type LoggingTestSuite struct {
	suite.Suite
}

func TestLoggingTestSuite(t *testing.T) {
	suite.Run(t, new(LoggingTestSuite))
}

// MockLogger implements the gorp.Logger interface for testing
type MockLogger struct {
	LastMessage string
}

func (m *MockLogger) Printf(format string, v ...interface{}) {
	m.LastMessage = strings.TrimSpace(format)
	if len(v) > 0 {
		m.LastMessage = strings.TrimSpace(strings.TrimRight(m.LastMessage, "%s%v%d"))
	}
}

func (s *LoggingTestSuite) TestTraceOnOff() {
	// Create a new DbMap
	dbmap := &gorp.DbMap{}
	logger := &MockLogger{}

	// Test TraceOn without prefix
	dbmap.TraceOn("", logger)
	s.NotNil(dbmap.GetLogger())
	s.Equal("", dbmap.GetLoggerPrefix())

	// Test TraceOn with prefix
	dbmap.TraceOn("[gorp]", logger)
	s.NotNil(dbmap.GetLogger())
	s.Equal("[gorp] ", dbmap.GetLoggerPrefix())

	// Test TraceOff
	dbmap.TraceOff()
	s.Nil(dbmap.GetLogger())
	s.Equal("", dbmap.GetLoggerPrefix())
}

func (s *LoggingTestSuite) TestSlogLogger() {
	// Create a buffer to capture log output
	var buf bytes.Buffer
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	handler := slog.NewJSONHandler(&buf, opts)
	logger := slog.New(handler)

	// Create SlogLogger with attributes
	attrs := []slog.Attr{
		slog.String("component", "gorp"),
		slog.String("env", "test"),
	}
	slogLogger := gorp.NewSlogLogger(logger, attrs...)

	// Test logging
	testMsg := "SELECT * FROM users"
	slogLogger.Printf(testMsg)

	// Verify log output contains our message and attributes
	logOutput := buf.String()
	s.Contains(logOutput, testMsg)
	s.Contains(logOutput, `"component":"gorp"`)
	s.Contains(logOutput, `"env":"test"`)
	s.Contains(logOutput, `"sql_statement"`)
}

func (s *LoggingTestSuite) TestSlogLoggerDefaultLogger() {
	// Test that NewSlogLogger uses default logger when nil is passed
	slogLogger := gorp.NewSlogLogger(nil)
	s.NotNil(slogLogger)

	// Test logging with default logger
	slogLogger.Printf("test message")
	// Note: We can't easily capture output from default logger,
	// but we can verify it doesn't panic
}

func (s *LoggingTestSuite) TestSlogLoggerAttributes() {
	// Create a buffer to capture log output
	var buf bytes.Buffer
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	handler := slog.NewJSONHandler(&buf, opts)
	logger := slog.New(handler)

	// Test with multiple attributes
	attrs := []slog.Attr{
		slog.String("app", "test-app"),
		slog.Int("version", 1),
		slog.Bool("debug", true),
	}
	slogLogger := gorp.NewSlogLogger(logger, attrs...)

	// Log a message
	slogLogger.Printf("test query")

	// Verify all attributes are present
	logOutput := buf.String()
	s.Contains(logOutput, `"app":"test-app"`)
	s.Contains(logOutput, `"version":1`)
	s.Contains(logOutput, `"debug":true`)
}

func (s *LoggingTestSuite) TestLoggerInterface() {
	// Verify that both GorpLogger and Logger can be used interchangeably
	var logger1 gorp.Logger = &MockLogger{}
	var logger2 gorp.GorpLogger = &MockLogger{}

	s.NotNil(logger1)
	s.NotNil(logger2)

	// Both should work with TraceOn
	dbmap := &gorp.DbMap{}
	dbmap.TraceOn("", logger1)
	s.NotNil(dbmap.GetLogger())
	dbmap.TraceOn("", logger2)
	s.NotNil(dbmap.GetLogger())
}
