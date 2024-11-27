// Copyright 2012 James Cooper. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package gorp

import (
	"fmt"
	"log/slog"
)

// GorpLogger is a deprecated alias of Logger.
type GorpLogger = Logger

// Logger is the type that gorp uses to log SQL statements.
// See DbMap.TraceOn.
type Logger interface {
	Printf(format string, v ...interface{})
}

// SlogLogger implements Logger interface using slog.
type SlogLogger struct {
	logger *slog.Logger
	attrs  []slog.Attr
}

// NewSlogLogger creates a new SlogLogger with optional attributes.
func NewSlogLogger(logger *slog.Logger, attrs ...slog.Attr) *SlogLogger {
	if logger == nil {
		logger = slog.Default()
	}
	return &SlogLogger{
		logger: logger,
		attrs:  attrs,
	}
}

// Printf implements Logger interface using structured logging.
func (l *SlogLogger) Printf(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	attrs := append(l.attrs, slog.String("sql_statement", msg))
	l.logger.LogAttrs(nil, slog.LevelInfo, "sql_trace", attrs...)
}

// TraceOn turns on SQL statement logging for this DbMap. After this is
// called, all SQL statements will be sent to the logger. If prefix is
// a non-empty string, it will be written to the front of all logged
// strings, which can aid in filtering log lines.
//
// Use TraceOn if you want to spy on the SQL statements that gorp
// generates.
//
// Note that the base log.Logger type satisfies Logger, but adapters can
// easily be written for other logging packages (e.g., the golang-sanctioned
// slog framework).
//
// Example using slog:
//
//	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
//	dbmap.TraceOn("[gorp]", gorp.NewSlogLogger(logger))
func (m *DbMap) TraceOn(prefix string, logger Logger) {
	m.logger = logger
	if prefix == "" {
		m.logPrefix = prefix
	} else {
		m.logPrefix = fmt.Sprintf("%s ", prefix)
	}
}

// TraceOff turns off tracing. It is idempotent.
func (m *DbMap) TraceOff() {
	m.logger = nil
	m.logPrefix = ""
}
